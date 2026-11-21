mod parquet;
mod util;

use parquet::Col;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use util::*;

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const GAP_MS: u64 = 1_800_000;
const CHUNK: usize = 4 << 20;

#[repr(C)]
#[derive(Clone, Copy)]
struct Click {
    user: u64,
    ckey: u64,
    ts: u32,
    camp: u32,
}

struct Chunk {
    file: usize,
    start: usize,
    end: usize,
}

/// Rows are written in blocks of BLK per partition.
const BLK: usize = 512;
/// Rows faulted in per kernel call.
const POP: usize = 1 << 16;
const DEAD_W: u64 = u64::MAX;

/// One event: the person and the packed time and purchase, then what dedup needs.
#[repr(C)]
struct Row {
    name: u64,
    w: u64,
    key: u64,
    ingest: u32,
    best: u32,
}

struct EvState {
    rows: P<Row>,
    used: usize,
    cur: Vec<(usize, usize)>,
    lists: Vec<Vec<u32>>,
    dups: Vec<(u32, u32)>,
    inserted: usize,
    crows: P<Click>,
    cused: usize,
    ccur: Vec<(usize, usize)>,
    clists: Vec<Vec<u32>>,
    nparts: usize,
    pshift: u32,
    row_cap: usize,
    click_cap: usize,
}

struct ThreadOut {
    rows: P<Row>,
    lists: Vec<Vec<u32>>,
    left: Vec<u32>,
    dups: Vec<(u32, u32)>,
    crows: P<Click>,
    clists: Vec<Vec<u32>>,
    cleft: Vec<u32>,
}

struct TableCtx<'a> {
    table: P<AtomicU64>,
    mask: usize,
    tshift: u32,
    tag_mask: u32,
    loc_base: u32,
    _p: std::marker::PhantomData<&'a ()>,
}

/// Lock-free union-find over the IDs in links, in an open-addressing table.
/// Word 0 of a slot is the ID; word 1 is the parent slot + 1 while building (0 for a
/// root) and the person's name afterwards. A root always has the smallest ID of its set.
struct LinkTable {
    e: P<[AtomicU64; 2]>,
    mask: usize,
    shift: u32,
}

impl LinkTable {
    #[inline(always)]
    fn w(&self, i: usize, k: usize) -> &AtomicU64 {
        unsafe { &(*self.e.p().add(i))[k] }
    }
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (mix(k) >> self.shift) as usize
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        prefetch(unsafe { self.e.p().add(self.slot(k)) });
    }
    /// The name of the person with this ID.
    #[inline(always)]
    fn get(&self, k: u64) -> u64 {
        let mut i = self.slot(k);
        loop {
            let c = self.w(i, 0).load(Ordering::Relaxed);
            if c == k {
                return self.w(i, 1).load(Ordering::Relaxed);
            }
            if c == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn insert(&self, k: u64) -> usize {
        let mut i = self.slot(k);
        loop {
            let mut c = self.w(i, 0).load(Ordering::Acquire);
            if c == 0 {
                match self.w(i, 0).compare_exchange(0, k, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => return i,
                    Err(x) => c = x,
                }
            }
            if c == k {
                return i;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn find(&self, mut i: usize) -> usize {
        loop {
            let p = self.w(i, 1).load(Ordering::Acquire);
            if p == 0 {
                return i;
            }
            let gp = self.w(p as usize - 1, 1).load(Ordering::Acquire);
            if gp != 0 {
                let _ = self.w(i, 1).compare_exchange(p, gp, Ordering::AcqRel, Ordering::Relaxed);
            }
            i = p as usize - 1;
        }
    }
    fn unite(&self, a: usize, b: usize) {
        loop {
            let (ra, rb) = (self.find(a), self.find(b));
            if ra == rb {
                return;
            }
            let (ka, kb) = (self.w(ra, 0).load(Ordering::Relaxed), self.w(rb, 0).load(Ordering::Relaxed));
            let (keep, child) = if key_lt(ka, kb) { (ra, rb) } else { (rb, ra) };
            if self.w(child, 1).compare_exchange(0, keep as u64 + 1, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return;
            }
        }
    }

    fn build(files: &[Map], chunks: &[Chunk], nthreads: usize) -> Self {
        // Count the lines to size the table: at most two new IDs per line.
        let lines = AtomicUsize::new(files.len());
        let next = AtomicUsize::new(0);
        std::thread::scope(|sc| {
            for _ in 0..nthreads {
                sc.spawn(|| loop {
                    let j = next.fetch_add(1, Ordering::Relaxed);
                    if j >= chunks.len() {
                        break;
                    }
                    let c = &chunks[j];
                    let sl = unsafe { std::slice::from_raw_parts(files[c.file].ptr.add(c.start), c.end - c.start) };
                    lines.fetch_add(sl.iter().filter(|&&b| b == b'\n').count(), Ordering::Relaxed);
                });
            }
        });
        let cap = (lines.load(Ordering::Relaxed) * 4).max(1024).next_power_of_two();
        let t = LinkTable { e: P(alloc_zeroed::<[AtomicU64; 2]>(cap)), mask: cap - 1, shift: 64 - cap.trailing_zeros() };
        let next = AtomicUsize::new(0);
        let tids = AtomicUsize::new(0);
        std::thread::scope(|sc| {
            for _ in 0..nthreads {
                sc.spawn(|| loop {
                    let j = next.fetch_add(1, Ordering::Relaxed);
                    if j >= chunks.len() {
                        break;
                    }
                    if j < nthreads {
                        let k = tids.fetch_add(1, Ordering::Relaxed);
                        if k < nthreads {
                            populate(unsafe { t.e.p().add(cap / nthreads * k) }, cap / nthreads);
                        }
                    }
                    let c = &chunks[j];
                    let m = &files[c.file];
                    let d = m.ptr;
                    const RING: usize = 8;
                    let mut ring = [(0u64, 0u64); RING];
                    let mut n = 0usize;
                    unsafe {
                        scan_lines(d, m.len, c.start, c.end, |ls, commas, nf, nl, qc| {
                            let Some(f) = fields::<2>(d, ls, commas, nf, nl, qc) else { return };
                            if f[0].1 == 0 || f[1].1 == 0 {
                                return;
                            }
                            let a = pack(d.add(f[0].0), f[0].1);
                            let b = pack(d.add(f[1].0), f[1].1);
                            if a == b {
                                return;
                            }
                            t.prefetch(a);
                            t.prefetch(b);
                            let r = n % RING;
                            if n >= RING {
                                let (x, y) = ring[r];
                                t.unite(t.insert(x), t.insert(y));
                            }
                            ring[r] = (a, b);
                            n += 1;
                        });
                    }
                    for &(x, y) in &ring[..n.min(RING)] {
                        t.unite(t.insert(x), t.insert(y));
                    }
                });
            }
        });
        // Point every slot at its root, then replace the pointer by the root's ID.
        const STEP: usize = 1 << 15;
        for pass in 0..2 {
            let next = AtomicUsize::new(0);
            std::thread::scope(|sc| {
                for _ in 0..nthreads {
                    sc.spawn(|| loop {
                        let s = next.fetch_add(STEP, Ordering::Relaxed);
                        if s >= cap {
                            break;
                        }
                        for i in s..(s + STEP).min(cap) {
                            if t.w(i, 0).load(Ordering::Relaxed) == 0 {
                                continue;
                            }
                            if pass == 0 {
                                let r = t.find(i);
                                if r != i {
                                    t.w(i, 1).store(r as u64 + 1, Ordering::Relaxed);
                                }
                            } else {
                                let p = t.w(i, 1).load(Ordering::Relaxed);
                                let own = if p == 0 { i } else { p as usize - 1 };
                                t.w(i, 1).store(t.w(own, 0).load(Ordering::Relaxed), Ordering::Relaxed);
                            }
                        }
                    });
                }
            });
        }
        t
    }
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn manifest_ts(text: &str, name: &str) -> i64 {
    let at = text.find(name).expect("manifest key");
    let rest = &text[at + name.len() + 1..];
    let a = rest.find('"').expect("manifest value") + 1;
    let b = a + rest[a..].find('"').expect("manifest value end");
    let mut buf = rest[a..b].as_bytes().to_vec();
    let len = buf.len();
    buf.resize(len + 32, 0);
    let v = unsafe { parse_ts(buf.as_ptr(), len, &mut DateCache::new()) };
    assert!(v != INVALID, "bad manifest timestamp");
    v
}

struct Ctx {
    ws: i64,
    we: i64,
    products: KMap<u32>,
    cat_unknown: u32,
    campaigns: KMap<u32>,
}

/// Lookup file with two columns: (key, value bytes) per row.
fn read_lookup(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let m = map_file(path);
    let mut out = Vec::new();
    unsafe {
        scan_lines(m.ptr, m.len, 0, m.len, |ls, commas, nf, nl, qc| {
            if let Some(f) = fields::<2>(m.ptr, ls, commas, nf, nl, qc) {
                let a = std::slice::from_raw_parts(m.ptr.add(f[0].0), f[0].1);
                let b = std::slice::from_raw_parts(m.ptr.add(f[1].0), f[1].1);
                out.push((a.to_vec(), b.to_vec()));
            }
        });
    }
    out
}

#[derive(Default)]
struct UserOut {
    ids: Vec<u8>,
    day: Vec<i32>,
    sessions: Vec<i32>,
    events: Vec<i32>,
    revenue: Vec<i64>,
    cat: Vec<u8>,
}

fn main() {
    let t0 = Instant::now();
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nthreads = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from));
    let verbose = std::env::var("VERBOSE").is_ok();
    let lap = |what: &str| {
        if verbose {
            eprintln!("{:>8.1} ms  {}", t0.elapsed().as_secs_f64() * 1e3, what);
        }
    };

    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = manifest_ts(&manifest, "\"window_start\"");
    let we = manifest_ts(&manifest, "\"window_end\"");
    assert!(we - ws < (1i64 << 31), "window too long for this build");
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = (((we - 1).div_euclid(DAY_MS) - day0 + 1).max(1)) as usize;

    // Products and categories. Category ids follow byte order, so a tie goes to the smaller id.
    let prod_rows = read_lookup(&data_dir.join("products.csv"));
    let mut cats: Vec<Vec<u8>> = prod_rows.iter().map(|r| r.1.clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    assert!(cats.len() <= 255, "too many categories for this build");
    let cat_unknown = cats.binary_search(&b"unknown".to_vec()).unwrap() as u32;
    let mut products: KMap<u32> = KMap::with_capacity(prod_rows.len());
    for (id, cat) in &prod_rows {
        if !id.is_empty() {
            products.get_or_insert(pack_slice(id), cats.binary_search(cat).unwrap() as u32);
        }
    }
    let camp_rows = read_lookup(&data_dir.join("campaigns.csv"));
    let mut campaigns: KMap<u32> = KMap::with_capacity(camp_rows.len());
    let mut camp_names: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (id, ch) in &camp_rows {
        if !id.is_empty() {
            let k = pack_slice(id);
            if campaigns.get(k).is_none() {
                campaigns.get_or_insert(k, camp_names.len() as u32);
                camp_names.push((id.clone(), ch.clone()));
            }
        }
    }
    let ncamp = camp_names.len();
    camp_names.push((b"organic".to_vec(), b"organic".to_vec()));
    let ctx = Ctx { ws, we, products, cat_unknown, campaigns };

    // Map inputs and cut them into chunks.
    let ev_files: Vec<Map> = list_csv(&data_dir.join("events")).iter().map(|p| map_file(p)).collect();
    let cl_files: Vec<Map> = list_csv(&data_dir.join("clicks")).iter().map(|p| map_file(p)).collect();
    let ln_files: Vec<Map> = list_csv(&data_dir.join("links")).iter().map(|p| map_file(p)).collect();
    let cut = |files: &[Map], size: usize| -> (Vec<Chunk>, usize) {
        let mut chunks = Vec::new();
        let mut bytes = 0usize;
        for (fi, m) in files.iter().enumerate() {
            bytes += m.len;
            let mut s = 0;
            while s < m.len {
                let e = (s + size).min(m.len);
                chunks.push(Chunk { file: fi, start: s, end: e });
                s = e;
            }
        }
        (chunks, bytes)
    };
    let (ev_chunks, ev_bytes) = cut(&ev_files, CHUNK);
    let (cl_chunks, cl_bytes) = cut(&cl_files, CHUNK);
    let (ln_chunks, _) = cut(&ln_files, 1 << 20);
    // Ask the kernel to start reading everything now, in the order it will be parsed.
    #[cfg(target_os = "linux")]
    if std::env::var("NO_READAHEAD").is_err() {
        let ranges: Vec<(usize, usize)> =
            ln_files.iter().chain(cl_files.iter()).chain(ev_files.iter()).filter(|m| m.len > 0).map(|m| (m.ptr as usize, m.len)).collect();
        std::thread::spawn(move || {
            for (p, len) in ranges {
                unsafe { libc::madvise(p as *mut libc::c_void, len, libc::MADV_WILLNEED) };
            }
        });
    }
    lap("setup");

    // Phase 1: links. Every ID in a link maps to its person's name.
    let links = LinkTable::build(&ln_files, &ln_chunks, nthreads);
    lap("links");

    // Dedup table size: from the sampled line length, never more than the guaranteed
    // bound (a valid event line has at least 28 bytes).
    let rec_bound = ev_bytes / 28 + 16;
    let (mut sample_bytes, mut sample_lines) = (0usize, 0usize);
    for m in &ev_files {
        let n = m.len.min(16 << 10);
        let sl = unsafe { std::slice::from_raw_parts(m.ptr, n) };
        sample_bytes += n;
        sample_lines += sl.iter().filter(|&&b| b == b'\n').count();
    }
    let bits = |n: usize| n.max(1024).next_power_of_two().trailing_zeros();
    let g_bits = bits(rec_bound + rec_bound / 8);
    let est = if sample_bytes > 0 { (ev_bytes as f64 * sample_lines as f64 / sample_bytes as f64 * 1.05) as usize } else { 0 };
    let chunk_lines = CHUNK / 28 + 2;
    let min_bits = bits(nthreads * chunk_lines * 6);
    let mut tbits = g_bits.min(bits((est as f64 / 0.7) as usize).max(min_bits));
    if let Some(f) = std::env::var("FORCE_TBITS").ok().and_then(|v| v.parse::<u32>().ok()) {
        tbits = f.min(g_bits);
    }
    // Two levels of partitioning by person: a small fan-out while parsing, so every
    // output stream stays in cache, and a second split just before sessionizing.
    let est_rows = est.min(rec_bound).max(1);
    let pbits = ((est_rows >> 16).max(1).next_power_of_two().trailing_zeros()).clamp(2, 7);
    let nparts = 1usize << pbits;
    let pshift = 64 - pbits;
    let sbits = (((est_rows >> pbits) / 6000).max(1).next_power_of_two().trailing_zeros()).min(10);
    let nsub = 1usize << sbits;
    if verbose {
        eprintln!("est {est} bound {rec_bound} tbits {tbits} g_bits {g_bits} nparts {nparts}");
    }

    // Rows go straight into their person's partition: blocks of BLK rows in one lazily
    // committed array per thread. A row's location is (thread, index) in 32 bits.
    let row_cap = rec_bound + (nparts + 2) * BLK;
    let tb = nthreads.next_power_of_two().trailing_zeros();
    let lshift = 32 - tb;
    assert!((row_cap as u64) + 1 < (1u64 << lshift), "too many events for this build");
    let lmask = (1u32 << lshift) - 1;

    let tag_mask = std::env::var("FORCE_TAG_MASK").ok().and_then(|v| v.parse::<u32>().ok()).unwrap_or(u32::MAX);

    // Phase 2: parse events and clicks.
    let njobs = ev_chunks.len() + cl_chunks.len();
    let mut outs: Vec<ThreadOut>;
    loop {
        let slots = 1usize << tbits;
        let table = P(alloc_zeroed::<AtomicU64>(slots));
        let limit = if tbits >= g_bits { usize::MAX } else { slots / 5 * 4 };
        let inserted = AtomicUsize::new(0);
        let abort = std::sync::atomic::AtomicBool::new(false);
        let next = AtomicUsize::new(0);
        let rows: Vec<P<Row>> = (0..nthreads).map(|_| P(alloc_zeroed::<Row>(row_cap))).collect();
        outs = Vec::new();
        std::thread::scope(|sc| {
            let mut hs = Vec::new();
            for tid in 0..nthreads {
                let (ctx, ev_chunks, cl_chunks, ev_files, cl_files, rows, next, inserted, abort, links) =
                    (&ctx, &ev_chunks, &cl_chunks, &ev_files, &cl_files, &rows, &next, &inserted, &abort, &links);
                hs.push(sc.spawn(move || {
                    let mut st = EvState {
                        rows: rows[tid],
                        used: 0,
                        cur: vec![(0, 0); nparts + 1],
                        lists: (0..nparts + 1).map(|_| Vec::new()).collect(),
                        dups: Vec::new(),
                        inserted: 0,
                        crows: P(alloc_zeroed::<Click>(cl_bytes / 8 + 16 + (nparts + 1) * BLK)),
                        cused: 0,
                        ccur: vec![(0, 0); nparts],
                        clists: (0..nparts).map(|_| Vec::new()).collect(),
                        nparts,
                        pshift,
                        row_cap,
                        click_cap: cl_bytes / 8 + 16 + (nparts + 1) * BLK,
                    };
                    populate(unsafe { table.p().add(slots / nthreads * tid) }, slots / nthreads);
                    let tctx = TableCtx { table, mask: slots - 1, tshift: 64 - tbits, tag_mask, loc_base: (tid as u32) << lshift, _p: std::marker::PhantomData };
                    loop {
                        let j = next.fetch_add(1, Ordering::Relaxed);
                        if j >= njobs || abort.load(Ordering::Relaxed) {
                            break;
                        }
                        if j < cl_chunks.len() {
                            let c = &cl_chunks[j];
                            unsafe { parse_clicks(ctx, &cl_files[c.file], c, &mut st, links) };
                        } else {
                            let c = &ev_chunks[j - cl_chunks.len()];
                            let before = st.inserted;
                            unsafe { parse_events(ctx, &ev_files[c.file], c, &mut st, &tctx, links) };
                            let n = st.inserted - before;
                            if inserted.fetch_add(n, Ordering::Relaxed) + n > limit {
                                abort.store(true, Ordering::Relaxed);
                            }
                        }
                    }
                    lap("worker");
                    ThreadOut {
                        rows: st.rows,
                        left: st.cur.iter().map(|c| c.1 as u32).collect(),
                        lists: st.lists,
                        dups: st.dups,
                        crows: st.crows,
                        cleft: st.ccur.iter().map(|c| c.1 as u32).collect(),
                        clists: st.clists,
                    }
                }));
            }
            for h in hs {
                outs.push(h.join().unwrap());
            }
        });
        if !abort.load(Ordering::Relaxed) {
            break;
        }
        // The estimate was too low: start over with a bigger table.
        unsafe {
            libc::munmap(table.p() as *mut libc::c_void, slots * 8);
            for t in 0..nthreads {
                libc::munmap(rows[t].p() as *mut libc::c_void, row_cap * 32);
            }
        }
        tbits += 1;
        lap("table too small, retrying");
    }
    lap("parse");

    // Dedup: of the copies of an event, the one with the smallest ingest_ts survives.
    // Each group of copies is handled by one thread, chosen by its first copy.
    let odd: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|sc| {
        for w in 0..nthreads {
            let (outs, odd) = (&outs, &odd);
            sc.spawn(move || unsafe {
                let di = |loc: u32| -> *mut Row { outs[(loc >> lshift) as usize].rows.p().add((loc & lmask) as usize) };
                let mut my_odd: Vec<u32> = Vec::new();
                const AHEAD: usize = 12;
                for o in outs.iter() {
                    let d = &o.dups;
                    for i in 0..d.len() {
                        if i + AHEAD < d.len() {
                            let (pf, pm) = d[i + AHEAD];
                            if pf as usize % nthreads == w {
                                prefetch_w(di(pf));
                                prefetch_w(di(pm));
                            }
                        }
                        let (first, me) = d[i];
                        if first as usize % nthreads != w {
                            continue;
                        }
                        let f = di(first);
                        let m = di(me);
                        if (*m).key != (*f).key {
                            // Only the hash tags matched: not a copy of that row.
                            my_odd.push(me);
                            continue;
                        }
                        let b = if (*f).best == 0 { first } else { (*f).best - 1 };
                        if (*m).ingest < (*di(b)).ingest {
                            (*di(b)).w = DEAD_W;
                            (*f).best = me + 1;
                        } else {
                            (*m).w = DEAD_W;
                        }
                    }
                }
                if !my_odd.is_empty() {
                    odd.lock().unwrap().extend_from_slice(&my_odd);
                }
            });
        }
    });
    // Rows that only shared a tag never entered the table; dedup them among themselves.
    let odd = odd.into_inner().unwrap();
    if !odd.is_empty() {
        let di = |loc: u32| -> *mut Row { unsafe { outs[(loc >> lshift) as usize].rows.p().add((loc & lmask) as usize) } };
        let mut best: std::collections::HashMap<u64, u32> = std::collections::HashMap::new();
        for &me in &odd {
            unsafe {
                match best.entry((*di(me)).key) {
                    std::collections::hash_map::Entry::Vacant(v) => {
                        v.insert(me);
                    }
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        let b = *e.get();
                        if (*di(me)).ingest < (*di(b)).ingest {
                            (*di(b)).w = DEAD_W;
                            e.insert(me);
                        } else {
                            (*di(me)).w = DEAD_W;
                        }
                    }
                }
            }
        }
    }
    lap("dedup");
    let ev_parts = &outs;

    // Phase 3: per partition, sort by person and time, sessionize, attribute.
    std::fs::create_dir_all(out_dir.join("user_daily")).expect("mkdir");
    std::fs::create_dir_all(out_dir.join("campaign_daily")).expect("mkdir");
    let next = AtomicUsize::new(0);
    let wrote = AtomicUsize::new(0);
    let mut agg_total = vec![[0i64; 4]; (ncamp + 1) * ndays];
    std::thread::scope(|sc| {
        let mut hs = Vec::new();
        for tid in 0..nthreads {
            let (cats, out_dir, next, wrote) = (&cats, &out_dir, &next, &wrote);
            hs.push(sc.spawn(move || {
                let mut agg = vec![[0i64; 4]; (ncamp + 1) * ndays];
                let mut out = UserOut::default();
                let mut st = PersonState::new(ncamp, cats.len());
                let mut sc = Scratch::default();
                let env = Env { ws, day0, ndays, ncamp };
                let mut sub_ev: Vec<Vec<[u64; 2]>> = (0..nsub).map(|_| Vec::new()).collect();
                let mut sub_cl: Vec<Vec<Click>> = (0..nsub).map(|_| Vec::new()).collect();
                loop {
                    let p = next.fetch_add(1, Ordering::Relaxed);
                    if p >= nparts {
                        break;
                    }
                    // Second split: by the next bits of the person's hash.
                    for v in sub_ev.iter_mut() {
                        v.clear();
                    }
                    for v in sub_cl.iter_mut() {
                        v.clear();
                    }
                    let sub_of = |name: u64| -> usize { if sbits == 0 { 0 } else { ((mix(name) << pbits) >> (64 - sbits)) as usize } };
                    for t in ev_parts.iter() {
                        let l = &t.lists[p];
                        for (bi, &b) in l.iter().enumerate() {
                            let len = if bi + 1 == l.len() { BLK - t.left[p] as usize } else { BLK };
                            let seg = unsafe { std::slice::from_raw_parts(t.rows.p().add(b as usize) as *const Row, len) };
                            for r in seg {
                                if r.w != DEAD_W {
                                    unsafe { sub_ev.get_unchecked_mut(sub_of(r.name)).push([r.name, r.w]) };
                                }
                            }
                        }
                        let l = &t.clists[p];
                        for (bi, &b) in l.iter().enumerate() {
                            let len = if bi + 1 == l.len() { BLK - t.cleft[p] as usize } else { BLK };
                            let seg = unsafe { std::slice::from_raw_parts(t.crows.p().add(b as usize) as *const Click, len) };
                            for c in seg {
                                unsafe { sub_cl.get_unchecked_mut(sub_of(c.user)).push(*c) };
                            }
                        }
                    }
                    for q in 0..nsub {
                        process_sub(&mut sc, &sub_ev[q], &sub_cl[q], &mut st, &env, &mut agg, &mut out);
                    }
                }
                let n = out.day.len();
                if n > 0 {
                    wrote.fetch_add(1, Ordering::Relaxed);
                    write_user_file(out_dir, tid, &out, cats);
                }
                agg
            }));
        }
        for h in hs {
            let a = h.join().unwrap();
            for (t, x) in agg_total.iter_mut().zip(&a) {
                for k in 0..4 {
                    t[k] += x[k];
                }
            }
        }
    });
    if wrote.load(Ordering::Relaxed) == 0 {
        write_user_file(&out_dir, 0, &UserOut::default(), &cats);
    }
    lap("sessionize");

    // campaign_daily.
    let (mut ids, mut chans) = (Vec::new(), Vec::new());
    let (mut day, mut sess, mut purch, mut rev, mut asst) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in 0..=ncamp {
        for d in 0..ndays {
            let a = agg_total[c * ndays + d];
            if a[0] != 0 || a[3] != 0 {
                let (id, ch) = &camp_names[c];
                ids.extend_from_slice(&(id.len() as u32).to_le_bytes());
                ids.extend_from_slice(id);
                chans.extend_from_slice(&(ch.len() as u32).to_le_bytes());
                chans.extend_from_slice(ch);
                day.push((day0 + d as i64) as i32);
                sess.push(a[0]);
                purch.push(a[1]);
                rev.push(a[2]);
                asst.push(a[3]);
            }
        }
    }
    parquet::write_file(
        &out_dir.join("campaign_daily").join("part-0.parquet"),
        day.len(),
        &[
            Col::Str { name: "campaign_id", data: &ids },
            Col::Str { name: "channel", data: &chans },
            Col::I32 { name: "day", data: &day, date: true },
            Col::I64 { name: "sessions", data: &sess },
            Col::I64 { name: "purchasing_sessions", data: &purch },
            Col::I64 { name: "revenue_cents", data: &rev },
            Col::I64 { name: "assisted_sessions", data: &asst },
        ],
    );
    lap("done");
    if verbose {
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
        eprintln!("maxrss {} (KB on Linux), minflt {}, majflt {}", ru.ru_maxrss, ru.ru_minflt, ru.ru_majflt);
    }
    // Skip destructors and unmapping: the kernel reclaims everything at exit.
    unsafe { libc::_exit(0) };
}

fn write_user_file(out_dir: &Path, tid: usize, out: &UserOut, cats: &[Vec<u8>]) {
    parquet::write_file(
        &out_dir.join("user_daily").join(format!("part-{tid}.parquet")),
        out.day.len(),
        &[
            Col::Str { name: "user_id", data: &out.ids },
            Col::I32 { name: "day", data: &out.day, date: true },
            Col::I32 { name: "sessions", data: &out.sessions, date: false },
            Col::I32 { name: "events", data: &out.events, date: false },
            Col::I64 { name: "revenue_cents", data: &out.revenue },
            Col::DictStr { name: "top_category", dict: cats, idx: &out.cat },
        ],
    );
}

struct Env {
    ws: i64,
    day0: i64,
    ndays: usize,
    ncamp: usize,
}

#[derive(Default)]
struct Scratch {
    lm: LocalMap,
    names: Vec<u64>,
    off: Vec<u32>,
    coff: Vec<u32>,
    ids: Vec<u32>,
    cids: Vec<u32>,
    ev: Vec<u64>,
    cl: Vec<Click>,
}

/// One sub-partition: group rows by person, sort each person by time, sessionize.
#[inline(never)]
fn process_sub(sc: &mut Scratch, rows: &[[u64; 2]], clicks: &[Click], st: &mut PersonState, env: &Env, agg: &mut [[i64; 4]], out: &mut UserOut) {
    let Scratch { lm, names, off, coff, ids, cids, ev, cl } = sc;
    let n = rows.len();
    if n == 0 {
        return;
    }
    // Dense person numbers, then a counting sort by person.
    lm.reset(n / 4);
    names.clear();
    off.clear();
    ids.clear();
    for r in rows.iter() {
        let id = lm.get_or_insert(r[0], names);
        if id as usize == off.len() {
            off.push(0);
        }
        off[id as usize] += 1;
        ids.push(id);
    }
    let np = names.len();
    let mut acc = 0u32;
    for o in off.iter_mut() {
        let c = *o;
        *o = acc;
        acc += c;
    }
    off.push(acc);
    ev.clear();
    ev.reserve(n);
    unsafe { ev.set_len(n) };
    for (r, &id) in rows.iter().zip(ids.iter()) {
        let id = id as usize;
        ev[off[id] as usize] = r[1];
        off[id] += 1;
    }
    // off[id] is now the end of person id, which is the start of id + 1.
    coff.clear();
    coff.resize(np + 1, 0);
    cids.clear();
    for c in clicks.iter() {
        let id = lm.get(c.user);
        cids.push(id);
        if id != u32::MAX {
            coff[id as usize] += 1;
        }
    }
    let mut acc = 0u32;
    for o in coff.iter_mut() {
        let c = *o;
        *o = acc;
        acc += c;
    }
    cl.clear();
    cl.resize(acc as usize, Click { user: 0, ckey: 0, ts: 0, camp: 0 });
    for (c, &id) in clicks.iter().zip(cids.iter()) {
        if id != u32::MAX {
            cl[coff[id as usize] as usize] = *c;
            coff[id as usize] += 1;
        }
    }
    let (mut es, mut cs) = (0usize, 0usize);
    for id in 0..np {
        let (ee, ce) = (off[id] as usize, coff[id] as usize);
        let e = &mut ev[es..ee];
        sort_times(e);
        let c = &mut cl[cs..ce];
        if c.len() > 1 {
            c.sort_unstable_by_key(|c| (c.ts, c.ckey));
        }
        st.person(names[id], e, c, env.ws, env.day0, env.ndays, env.ncamp, agg, out);
        es = ee;
        cs = ce;
    }
}

#[inline(never)]
fn sort_times(e: &mut [u64]) {
    e.sort_unstable();
}

/// Person name to dense number within one partition.
#[derive(Default)]
struct LocalMap {
    keys: Vec<u64>,
    ids: Vec<u32>,
    mask: usize,
}
impl LocalMap {
    fn new() -> Self {
        LocalMap { keys: Vec::new(), ids: Vec::new(), mask: 0 }
    }
    fn reset(&mut self, hint: usize) {
        let cap = hint.max(64).next_power_of_two();
        self.keys.clear();
        self.keys.resize(cap, 0);
        self.ids.clear();
        self.ids.resize(cap, 0);
        self.mask = cap - 1;
    }
    #[inline(always)]
    fn slot(&self, k: u64) -> usize {
        (mix(k) >> 6) as usize & self.mask
    }
    #[inline(always)]
    fn get(&self, k: u64) -> u32 {
        let mut i = self.slot(k);
        loop {
            let c = self.keys[i];
            if c == k {
                return self.ids[i];
            }
            if c == 0 {
                return u32::MAX;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get_or_insert(&mut self, k: u64, names: &mut Vec<u64>) -> u32 {
        let mut i = self.slot(k);
        loop {
            let c = self.keys[i];
            if c == k {
                return self.ids[i];
            }
            if c == 0 {
                break;
            }
            i = (i + 1) & self.mask;
        }
        let id = names.len() as u32;
        names.push(k);
        if names.len() * 2 > self.mask + 1 {
            self.reset((self.mask + 1) * 2);
            for (j, &nk) in names.iter().enumerate() {
                let mut i = self.slot(nk);
                while self.keys[i] != 0 {
                    i = (i + 1) & self.mask;
                }
                self.keys[i] = nk;
                self.ids[i] = j as u32;
            }
        } else {
            self.keys[i] = k;
            self.ids[i] = id;
        }
        id
    }
}

struct PersonState {
    cnt: Vec<u32>,
    since: Vec<(u32, u32)>,
    first: Vec<u32>,
    active: Vec<u32>,
    cat_sum: Vec<i64>,
    cat_touched: Vec<u32>,
    name_buf: Vec<u8>,
    name16: [u8; 16],
}

impl PersonState {
    fn new(ncamp: usize, ncat: usize) -> Self {
        PersonState {
            cnt: vec![0; ncamp + 1],
            since: vec![(0, 0); ncamp + 1],
            first: Vec::new(),
            active: Vec::new(),
            cat_sum: vec![0; ncat],
            cat_touched: Vec::new(),
            name_buf: Vec::new(),
            name16: [0; 16],
        }
    }

    #[inline(always)]
    fn flush(&mut self, day: i64, sessions: i32, events: i32, revenue: i64, out: &mut UserOut) {
        if sessions == 0 {
            return;
        }
        let nl = self.name_buf.len();
        if nl <= 12 {
            // Length and bytes in one fixed 16-byte store.
            out.ids.reserve(16);
            unsafe {
                let dst = out.ids.as_mut_ptr().add(out.ids.len());
                (dst as *mut u32).write_unaligned(nl as u32);
                std::ptr::copy_nonoverlapping(self.name16.as_ptr(), dst.add(4), 12);
                out.ids.set_len(out.ids.len() + 4 + nl);
            }
        } else {
            out.ids.extend_from_slice(&(nl as u32).to_le_bytes());
            out.ids.extend_from_slice(&self.name_buf);
        }
        out.day.push(day as i32);
        out.sessions.push(sessions);
        out.events.push(events);
        out.revenue.push(revenue);
        let mut best = 255u32;
        let mut best_sum = 0i64;
        for &c in &self.cat_touched {
            let s = self.cat_sum[c as usize];
            if s > best_sum || (s == best_sum && c < best) {
                best = c;
                best_sum = s;
            }
            self.cat_sum[c as usize] = 0;
        }
        self.cat_touched.clear();
        out.cat.push(best as u8);
    }

    #[allow(clippy::too_many_arguments)]
    #[inline(never)]
    fn person(
        &mut self,
        name: u64,
        ev: &[u64],
        cl: &[Click],
        ws: i64,
        day0: i64,
        ndays: usize,
        ncamp: usize,
        agg: &mut [[i64; 4]],
        out: &mut UserOut,
    ) {
        self.name_buf.clear();
        unpack(name, &mut self.name_buf);
        if self.name_buf.len() <= 12 {
            self.name16 = [0; 16];
            self.name16[..self.name_buf.len()].copy_from_slice(&self.name_buf);
        }
        let (mut lo, mut hi) = (0usize, 0usize);
        let mut cur_day = i64::MIN;
        let mut cur_di = usize::MAX;
        let mut nsess = 0u32;
        if self.first.len() < ndays + 1 {
            self.first.resize(ndays + 1, 0);
        }
        let (mut d_sessions, mut d_events, mut d_rev) = (0i32, 0i32, 0i64);
        let m = ev.len();
        let mut k = 0;
        while k < m {
            let start = ev[k] >> 32;
            let day = day0 + ((ws - day0 * DAY_MS) as u64 + start) as i64 / DAY_MS;
            if day != cur_day {
                self.flush(cur_day, d_sessions, d_events, d_rev, out);
                cur_day = day;
                d_sessions = 0;
                d_events = 0;
                d_rev = 0;
            }
            let mut prev = start;
            let mut rev = 0i64;
            let mut n = 0i32;
            while k < m {
                let w = ev[k];
                let ts = w >> 32;
                if ts - prev > GAP_MS {
                    break;
                }
                prev = ts;
                n += 1;
                let price = (w & 0xFF_FFFF) as i64;
                if price != 0 {
                    let cat = ((w >> 24) & 0xFF) as usize;
                    rev += price;
                    if self.cat_sum[cat] == 0 {
                        self.cat_touched.push(cat as u32);
                    }
                    self.cat_sum[cat] += price;
                }
                k += 1;
            }
            d_sessions += 1;
            d_events += n;
            d_rev += rev;
            let di = (day - day0) as usize;
            debug_assert!(di < ndays);
            // Clicks are timed from window_start - 7 days.
            let s = start + WEEK_MS as u64;
            if di != cur_di {
                // first[d] is the number of this person's sessions before day d.
                if cur_di == usize::MAX {
                    self.first[di] = 0;
                } else {
                    for d in cur_di + 1..=di {
                        self.first[d] = nsess;
                    }
                }
                cur_di = di;
            }
            while hi < cl.len() && cl[hi].ts as u64 <= s {
                let c = cl[hi].camp as usize;
                if self.cnt[c] == 0 {
                    // The campaign assists every session from this one until its clicks age out.
                    self.since[c] = (nsess, di as u32);
                    self.active.push(c as u32);
                }
                self.cnt[c] += 1;
                hi += 1;
            }
            while lo < hi && (cl[lo].ts as u64) + (WEEK_MS as u64) < s {
                let c = cl[lo].camp as usize;
                self.cnt[c] -= 1;
                if self.cnt[c] == 0 {
                    self.assists(c, nsess, cur_di, ndays, agg);
                }
                lo += 1;
            }
            let camp = if hi > lo {
                let t = cl[hi - 1].ts;
                let mut best = hi - 1;
                let mut g = hi - 1;
                while g > lo && cl[g - 1].ts == t {
                    g -= 1;
                    if key_lt(cl[g].ckey, cl[best].ckey) {
                        best = g;
                    }
                }
                cl[best].camp as usize
            } else {
                ncamp
            };
            let a = &mut agg[camp * ndays + di];
            a[0] += 1;
            a[1] += (rev > 0) as i64;
            a[2] += rev;
            nsess += 1;
        }
        self.flush(cur_day, d_sessions, d_events, d_rev, out);
        for i in 0..self.active.len() {
            let c = self.active[i] as usize;
            if self.cnt[c] > 0 {
                self.assists(c, nsess, cur_di, ndays, agg);
                self.cnt[c] = 0;
            }
        }
        self.active.clear();
    }

    /// Campaign `c` assisted this person's sessions numbered since[c].0 up to `end`
    /// (exclusive): count them on their days.
    #[inline(always)]
    fn assists(&self, c: usize, end: u32, cur_di: usize, ndays: usize, agg: &mut [[i64; 4]]) {
        let (a, sd) = self.since[c];
        for d in sd as usize..=cur_di {
            let lo = a.max(self.first[d]);
            let hi = if d < cur_di { self.first[d + 1] } else { end };
            if hi > lo {
                agg[c * ndays + d][3] += (hi - lo) as i64;
            }
        }
    }
}

unsafe fn parse_clicks(ctx: &Ctx, m: &Map, c: &Chunk, st: &mut EvState, links: &LinkTable) {
    let d = m.ptr;
    let mut dc = DateCache::new();
    let lo = ctx.ws - WEEK_MS;
    const RING: usize = 16;
    let mut ring = [Click { user: 0, ckey: 0, ts: 0, camp: 0 }; RING];
    let mut n = 0usize;
    let mut emit = |c: Click| {
        let name = links.get(c.user);
        let p = (mix(name) >> st.pshift) as usize;
        let cur = st.ccur.get_unchecked_mut(p);
        if cur.1 == 0 {
            if st.cused % POP == 0 {
                populate(st.crows.p().add(st.cused), POP.min(st.click_cap - st.cused));
            }
            st.clists.get_unchecked_mut(p).push(st.cused as u32);
            cur.0 = st.cused;
            cur.1 = BLK;
            st.cused += BLK;
        }
        st.crows.p().add(cur.0).write(Click { user: name, ..c });
        cur.0 += 1;
        cur.1 -= 1;
    };
    scan_lines(d, m.len, c.start, c.end, |ls, commas, nf, nl, qc| {
        let Some(f) = fields::<4>(d, ls, commas, nf, nl, qc) else { return };
        if f[0].1 == 0 || f[1].1 == 0 || f[2].1 == 0 {
            return;
        }
        let ts = parse_ts(d.add(f[3].0), f[3].1, &mut dc);
        if ts < lo || ts >= ctx.we {
            return;
        }
        let Some(camp) = ctx.campaigns.get(pack(d.add(f[2].0), f[2].1)) else { return };
        let user = pack(d.add(f[1].0), f[1].1);
        links.prefetch(user);
        let r = n % RING;
        if n >= RING {
            emit(ring[r]);
        }
        ring[r] = Click { user, ckey: pack(d.add(f[0].0), f[0].1), ts: (ts - lo) as u32, camp };
        n += 1;
    });
    for i in 0..n.min(RING) {
        emit(ring[i]);
    }
}

#[derive(Clone, Copy)]
struct Pending {
    key: u64,
    user: u64,
    w: u64,
    slot: usize,
    tag: u32,
    ingest: u32,
}

/// Resolves the row's person, writes it to its partition and enters it in the dedup table.
#[inline(always)]
unsafe fn commit(st: &mut EvState, t: &TableCtx, links: &LinkTable, pe: Pending) {
    let (name, p) = if pe.w != DEAD_W {
        let name = links.get(pe.user);
        (name, (mix(name) >> st.pshift) as usize)
    } else {
        (0, st.nparts)
    };
    let c = st.cur.get_unchecked_mut(p);
    if c.1 == 0 {
        if st.used % POP == 0 {
            populate(st.rows.p().add(st.used), POP.min(st.row_cap - st.used));
        }
        st.lists.get_unchecked_mut(p).push(st.used as u32);
        c.0 = st.used;
        c.1 = BLK;
        st.used += BLK;
    }
    let idx = c.0;
    c.0 += 1;
    c.1 -= 1;
    st.rows.p().add(idx).write(Row { name, w: pe.w, key: pe.key, ingest: pe.ingest, best: 0 });
    // The next line of this partition's stream, so the store never waits on a miss.
    prefetch_w(st.rows.p().add(idx + 2));
    st.inserted += 1;
    let loc = t.loc_base | idx as u32;
    let tag = pe.tag & t.tag_mask;
    let val = ((tag as u64) << 32) | (loc as u64 + 1);
    let mut i = pe.slot;
    loop {
        let slot = &*t.table.p().add(i);
        let mut cur = slot.load(Ordering::Acquire);
        if cur == 0 {
            match slot.compare_exchange(0, val, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(x) => cur = x,
            }
        }
        if (cur >> 32) as u32 == tag {
            // Very likely a copy of that row. The keys are compared after parsing, where
            // the other row can be fetched ahead of time; a row never waits on it here.
            st.dups.push(((cur as u32) - 1, loc));
            return;
        }
        i = (i + 1) & t.mask;
    }
}

unsafe fn parse_events(ctx: &Ctx, m: &Map, c: &Chunk, st: &mut EvState, t: &TableCtx, links: &LinkTable) {
    let d = m.ptr;
    let mut dc = DateCache::new();
    let mut n = 0usize;
    const RING: usize = 16;
    let mut ring = [Pending { key: 0, user: 0, w: 0, slot: 0, tag: 0, ingest: 0 }; RING];
    let (ws, we) = (ctx.ws, ctx.we);
    scan_lines(d, m.len, c.start, c.end, |ls, commas, nf, nl, qc| {
        let Some(f) = fields::<7>(d, ls, commas, nf, nl, qc) else { return };
        if f[0].1 == 0 {
            return;
        }
        let key = pack(d.add(f[0].0), f[0].1);
        let ing = parse_f1(d.add(f[3].0), f[3].1, &mut dc);
        let h = mix(key);
        let mut pe = Pending { key, user: 0, w: DEAD_W, slot: (h >> t.tshift) as usize, tag: h as u32, ingest: u32::MAX };
        prefetch_w(t.table.p().add(pe.slot));
        if ing != INVALID {
            pe.ingest = (ing / 1000).clamp(0, (u32::MAX - 1) as i64) as u32;
        }
        'v: {
            if ing == INVALID || f[1].1 == 0 || f[5].1 == 0 {
                break 'v;
            }
            let ts = parse_ts(d.add(f[2].0), f[2].1, &mut dc);
            if ts < ws || ts >= we || ing - ts > DAY_MS {
                break 'v;
            }
            let tp = d.add(f[4].0);
            let mut extra = 0u32;
            match f[4].1 {
                4 => {
                    let x = (tp as *const u32).read_unaligned();
                    if x != u32::from_ne_bytes(*b"view") && x != u32::from_ne_bytes(*b"cart") {
                        break 'v;
                    }
                }
                8 => {
                    if (tp as *const u64).read_unaligned() != u64::from_ne_bytes(*b"purchase") {
                        break 'v;
                    }
                    let price = parse_price(d.add(f[6].0), f[6].1);
                    if price == 0 {
                        break 'v;
                    }
                    let cat = ctx.products.get(pack(d.add(f[5].0), f[5].1)).unwrap_or(ctx.cat_unknown);
                    extra = price | (cat << 24);
                }
                _ => break 'v,
            }
            pe.user = pack(d.add(f[1].0), f[1].1);
            links.prefetch(pe.user);
            pe.w = (((ts - ws) as u64) << 32) | extra as u64;
        }
        let r = n % RING;
        if n >= RING {
            commit(st, t, links, ring[r]);
        }
        ring[r] = pe;
        n += 1;
    });
    for i in 0..n.min(RING) {
        commit(st, t, links, ring[i]);
    }
}
