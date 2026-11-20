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
const DEAD: u32 = u32::MAX;
const CHUNK: usize = 4 << 20;

#[repr(C)]
#[derive(Clone, Copy)]
struct Rec {
    key: u64,
    user: u64,
    ingest: u32,
    ts: u32,
    extra: u32,
    _pad: u32,
}

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
    base: usize,
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
    let mut ev_chunks = Vec::new();
    let mut base = 0usize;
    let mut ev_bytes = 0usize;
    for (fi, m) in ev_files.iter().enumerate() {
        ev_bytes += m.len;
        let mut s = 0;
        while s < m.len {
            let e = (s + CHUNK).min(m.len);
            ev_chunks.push(Chunk { file: fi, start: s, end: e, base });
            base += (e - s) / 28 + 2;
            s = e;
        }
    }
    let rec_cap = base;
    assert!(rec_cap < (u32::MAX - 1) as usize, "too many events for this build");
    let mut cl_chunks = Vec::new();
    for (fi, m) in cl_files.iter().enumerate() {
        let mut s = 0;
        while s < m.len {
            let e = (s + CHUNK).min(m.len);
            cl_chunks.push(Chunk { file: fi, start: s, end: e, base: 0 });
            s = e;
        }
    }
    let recs = P(alloc_zeroed::<Rec>(rec_cap));
    // At most ev_bytes / 28 records; the table holds at least 1.4 times that.
    let tbits = (((ev_bytes / 20).max(1024)).next_power_of_two()).trailing_zeros();
    let table = P(alloc_zeroed::<AtomicU64>(1usize << tbits));
    let ev_counts: Vec<AtomicUsize> = ev_chunks.iter().map(|_| AtomicUsize::new(0)).collect();
    lap("setup");

    // Phase 1: parse events and clicks on the workers, links on their own thread.
    let next = AtomicUsize::new(0);
    let njobs = ev_chunks.len() + cl_chunks.len();
    let mut dups: Vec<(u32, u32)> = Vec::new();
    let mut clicks_by_thread: Vec<Vec<Click>> = Vec::new();
    let mut resolve: KMap<u64> = KMap::with_capacity(0);
    std::thread::scope(|sc| {
        let link_handle = sc.spawn(|| build_links(&ln_files));
        let mut hs = Vec::new();
        for _ in 0..nthreads {
            hs.push(sc.spawn(|| {
                let mut dups: Vec<(u32, u32)> = Vec::new();
                let mut clicks: Vec<Click> = Vec::new();
                loop {
                    let j = next.fetch_add(1, Ordering::Relaxed);
                    if j >= njobs {
                        break;
                    }
                    // Largest inputs (events) last so clicks are out of the way early.
                    if j < cl_chunks.len() {
                        let c = &cl_chunks[j];
                        let m = &cl_files[c.file];
                        unsafe { parse_clicks(&ctx, m, c, &mut clicks) };
                    } else {
                        let ci = j - cl_chunks.len();
                        let c = &ev_chunks[ci];
                        let m = &ev_files[c.file];
                        let n = unsafe { parse_events(&ctx, m, c, recs, table, tbits, &mut dups) };
                        ev_counts[ci].store(n, Ordering::Relaxed);
                    }
                }
                (dups, clicks)
            }));
        }
        for h in hs {
            let (d, c) = h.join().unwrap();
            dups.extend_from_slice(&d);
            clicks_by_thread.push(c);
        }
        resolve = link_handle.join().unwrap();
    });
    lap("parse");

    // Dedup: the first copy seen represents the event; it takes the values of the
    // copy with the smallest ingest_ts, and the others die.
    unsafe {
        for &(first, me) in &dups {
            let m = *recs.p().add(me as usize);
            let r = &mut *recs.p().add(first as usize);
            if m.ingest < r.ingest {
                r.ingest = m.ingest;
                r.user = m.user;
                r.ts = m.ts;
                r.extra = m.extra;
            }
            (*recs.p().add(me as usize)).ts = DEAD;
        }
    }
    lap("dedup");

    // Phase 2: resolve each row's person and scatter rows into partitions by person.
    let total_recs: usize = ev_counts.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let pbits = ((total_recs * 16 / (512 << 10)).max(1).next_power_of_two().trailing_zeros()).clamp(4, 12);
    let nparts = 1usize << pbits;
    let pshift = 64 - pbits;
    let next = AtomicUsize::new(0);
    let njobs = ev_chunks.len() + clicks_by_thread.len();
    let mut ev_parts: Vec<Vec<Vec<[u64; 2]>>> = Vec::new();
    let mut cl_parts: Vec<Vec<Vec<Click>>> = Vec::new();
    std::thread::scope(|sc| {
        let mut hs = Vec::new();
        for _ in 0..nthreads {
            hs.push(sc.spawn(|| {
                let cap = total_recs / (nthreads * nparts) * 5 / 4 + 16;
                let mut ev: Vec<Vec<[u64; 2]>> = (0..nparts).map(|_| Vec::with_capacity(cap)).collect();
                let mut cl: Vec<Vec<Click>> = (0..nparts).map(|_| Vec::new()).collect();
                const B: usize = 32;
                loop {
                    let j = next.fetch_add(1, Ordering::Relaxed);
                    if j >= njobs {
                        break;
                    }
                    if j < ev_chunks.len() {
                        let n = ev_counts[j].load(Ordering::Relaxed);
                        let rs = unsafe { std::slice::from_raw_parts(recs.p().add(ev_chunks[j].base), n) };
                        for blk in rs.chunks(B) {
                            for r in blk {
                                if r.ts != DEAD {
                                    resolve.prefetch(r.user);
                                }
                            }
                            for r in blk {
                                if r.ts != DEAD {
                                    let name = resolve.get(r.user).unwrap_or(r.user);
                                    let p = (mix(name) >> pshift) as usize;
                                    ev[p].push([name, ((r.ts as u64) << 32) | r.extra as u64]);
                                }
                            }
                        }
                    } else {
                        let cs = &clicks_by_thread[j - ev_chunks.len()];
                        for blk in cs.chunks(B) {
                            for c in blk {
                                resolve.prefetch(c.user);
                            }
                            for c in blk {
                                let name = resolve.get(c.user).unwrap_or(c.user);
                                let p = (mix(name) >> pshift) as usize;
                                cl[p].push(Click { user: name, ..*c });
                            }
                        }
                    }
                }
                (ev, cl)
            }));
        }
        for h in hs {
            let (e, c) = h.join().unwrap();
            ev_parts.push(e);
            cl_parts.push(c);
        }
    });
    lap("scatter");

    // Phase 3: per partition, sort by person and time, sessionize, attribute.
    std::fs::create_dir_all(out_dir.join("user_daily")).expect("mkdir");
    std::fs::create_dir_all(out_dir.join("campaign_daily")).expect("mkdir");
    let next = AtomicUsize::new(0);
    let wrote = AtomicUsize::new(0);
    let mut agg_total = vec![[0i64; 4]; (ncamp + 1) * ndays];
    std::thread::scope(|sc| {
        let mut hs = Vec::new();
        for tid in 0..nthreads {
            let (ev_parts, cl_parts, cats, out_dir, next, wrote) = (&ev_parts, &cl_parts, &cats, &out_dir, &next, &wrote);
            hs.push(sc.spawn(move || {
                let mut agg = vec![[0i64; 4]; (ncamp + 1) * ndays];
                let mut out = UserOut::default();
                let mut st = PersonState::new(ncamp, cats.len());
                let mut ev: Vec<u128> = Vec::new();
                let mut cl: Vec<Click> = Vec::new();
                loop {
                    let p = next.fetch_add(1, Ordering::Relaxed);
                    if p >= nparts {
                        break;
                    }
                    ev.clear();
                    cl.clear();
                    for t in ev_parts.iter() {
                        ev.extend(t[p].iter().map(|r| ((r[0] as u128) << 64) | r[1] as u128));
                    }
                    for t in cl_parts.iter() {
                        cl.extend_from_slice(&t[p]);
                    }
                    ev.sort_unstable();
                    cl.sort_unstable_by_key(|c| (c.user, c.ts, c.ckey));
                    let (mut i, mut ci) = (0usize, 0usize);
                    while i < ev.len() {
                        let name = (ev[i] >> 64) as u64;
                        let mut j = i + 1;
                        while j < ev.len() && (ev[j] >> 64) as u64 == name {
                            j += 1;
                        }
                        while ci < cl.len() && cl[ci].user < name {
                            ci += 1;
                        }
                        let cs = ci;
                        while ci < cl.len() && cl[ci].user == name {
                            ci += 1;
                        }
                        st.person(name, &ev[i..j], &cl[cs..ci], ws, day0, ndays, ncamp, &mut agg, &mut out);
                        i = j;
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

struct PersonState {
    cnt: Vec<u32>,
    active: Vec<u32>,
    cat_sum: Vec<i64>,
    cat_touched: Vec<u32>,
    name_buf: Vec<u8>,
}

impl PersonState {
    fn new(ncamp: usize, ncat: usize) -> Self {
        PersonState {
            cnt: vec![0; ncamp + 1],
            active: Vec::new(),
            cat_sum: vec![0; ncat],
            cat_touched: Vec::new(),
            name_buf: Vec::new(),
        }
    }

    #[inline(always)]
    fn flush(&mut self, day: i64, sessions: i32, events: i32, revenue: i64, out: &mut UserOut) {
        if sessions == 0 {
            return;
        }
        out.ids.extend_from_slice(&(self.name_buf.len() as u32).to_le_bytes());
        out.ids.extend_from_slice(&self.name_buf);
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
    fn person(
        &mut self,
        name: u64,
        ev: &[u128],
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
        let (mut lo, mut hi) = (0usize, 0usize);
        let mut cur_day = i64::MIN;
        let (mut d_sessions, mut d_events, mut d_rev) = (0i32, 0i32, 0i64);
        let m = ev.len();
        let mut k = 0;
        while k < m {
            let start = (ev[k] as u64) >> 32;
            let day = (ws + start as i64).div_euclid(DAY_MS);
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
                let w = ev[k] as u64;
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
            while hi < cl.len() && cl[hi].ts as u64 <= s {
                let c = cl[hi].camp as usize;
                if self.cnt[c] == 0 {
                    self.active.push(c as u32);
                }
                self.cnt[c] += 1;
                hi += 1;
            }
            while lo < hi && (cl[lo].ts as u64) + (WEEK_MS as u64) < s {
                self.cnt[cl[lo].camp as usize] -= 1;
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
                // Assists: every campaign with a click in the window, once.
                let mut w = 0;
                for r in 0..self.active.len() {
                    let c = self.active[r];
                    if self.cnt[c as usize] > 0 {
                        agg[c as usize * ndays + di][3] += 1;
                        self.active[w] = c;
                        w += 1;
                    }
                }
                self.active.truncate(w);
                cl[best].camp as usize
            } else {
                for &c in &self.active {
                    self.cnt[c as usize] = 0;
                }
                self.active.clear();
                ncamp
            };
            let a = &mut agg[camp * ndays + di];
            a[0] += 1;
            a[1] += (rev > 0) as i64;
            a[2] += rev;
        }
        self.flush(cur_day, d_sessions, d_events, d_rev, out);
        for &c in &self.active {
            self.cnt[c as usize] = 0;
        }
        self.active.clear();
    }
}

unsafe fn parse_clicks(ctx: &Ctx, m: &Map, c: &Chunk, out: &mut Vec<Click>) {
    let d = m.ptr;
    let mut dc = DateCache::new();
    let lo = ctx.ws - WEEK_MS;
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
        out.push(Click {
            user: pack(d.add(f[1].0), f[1].1),
            ckey: pack(d.add(f[0].0), f[0].1),
            ts: (ts - lo) as u32,
            camp,
        });
    });
}

#[derive(Clone, Copy)]
struct Pending {
    slot: usize,
    tag: u32,
    loc: u32,
}

#[inline(always)]
unsafe fn table_insert(table: P<AtomicU64>, mask: usize, recs: P<Rec>, pe: Pending, dups: &mut Vec<(u32, u32)>) {
    let val = ((pe.tag as u64) << 32) | (pe.loc as u64 + 1);
    let key = (*recs.p().add(pe.loc as usize)).key;
    let mut i = pe.slot;
    loop {
        let slot = &*table.p().add(i);
        let mut cur = slot.load(Ordering::Acquire);
        if cur == 0 {
            match slot.compare_exchange(0, val, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return,
                Err(c) => cur = c,
            }
        }
        if (cur >> 32) as u32 == pe.tag {
            let other = (cur as u32) - 1;
            if (*recs.p().add(other as usize)).key == key {
                dups.push((other, pe.loc));
                return;
            }
        }
        i = (i + 1) & mask;
    }
}

unsafe fn parse_events(
    ctx: &Ctx,
    m: &Map,
    c: &Chunk,
    recs: P<Rec>,
    table: P<AtomicU64>,
    tbits: u32,
    dups: &mut Vec<(u32, u32)>,
) -> usize {
    let d = m.ptr;
    let mut dc = DateCache::new();
    let mut n = 0usize;
    let base = c.base;
    let mask = (1usize << tbits) - 1;
    let tshift = 64 - tbits;
    const RING: usize = 16;
    let mut ring = [Pending { slot: 0, tag: 0, loc: 0 }; RING];
    let (ws, we) = (ctx.ws, ctx.we);
    scan_lines(d, m.len, c.start, c.end, |ls, commas, nf, nl, qc| {
        let Some(f) = fields::<7>(d, ls, commas, nf, nl, qc) else { return };
        if f[0].1 == 0 {
            return;
        }
        let key = pack(d.add(f[0].0), f[0].1);
        let ing = parse_f1(d.add(f[3].0), f[3].1, &mut dc);
        let mut rec = Rec { key, user: 0, ingest: u32::MAX, ts: DEAD, extra: 0, _pad: 0 };
        if ing != INVALID {
            rec.ingest = (ing / 1000).clamp(0, (u32::MAX - 1) as i64) as u32;
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
            rec.user = pack(d.add(f[1].0), f[1].1);
            rec.ts = (ts - ws) as u32;
            rec.extra = extra;
        }
        let loc = base + n;
        *recs.p().add(loc) = rec;
        let h = mix(key);
        let pe = Pending { slot: (h >> tshift) as usize, tag: h as u32, loc: loc as u32 };
        prefetch(table.p().add(pe.slot));
        let r = n % RING;
        if n >= RING {
            table_insert(table, mask, recs, ring[r], dups);
        }
        ring[r] = pe;
        n += 1;
    });
    for i in 0..n.min(RING) {
        table_insert(table, mask, recs, ring[i], dups);
    }
    n
}

/// Reads the links and returns, for every ID whose person has another name, that name.
fn build_links(files: &[Map]) -> KMap<u64> {
    let mut pairs: Vec<(u64, u64)> = Vec::new();
    for m in files {
        let d = m.ptr;
        unsafe {
            scan_lines(d, m.len, 0, m.len, |ls, commas, nf, nl, qc| {
                let Some(f) = fields::<2>(d, ls, commas, nf, nl, qc) else { return };
                if f[0].1 == 0 || f[1].1 == 0 {
                    return;
                }
                let a = pack(d.add(f[0].0), f[0].1);
                let b = pack(d.add(f[1].0), f[1].1);
                if a != b {
                    pairs.push((a, b));
                }
            });
        }
    }
    let mut ids: KMap<u32> = KMap::with_capacity(pairs.len() * 2);
    let mut keys: Vec<u64> = Vec::with_capacity(pairs.len() + 16);
    let mut parent: Vec<u32> = Vec::with_capacity(pairs.len() + 16);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    const AHEAD: usize = 12;
    for i in 0..pairs.len() {
        if i + AHEAD < pairs.len() {
            ids.prefetch(pairs[i + AHEAD].0);
            ids.prefetch(pairs[i + AHEAD].1);
        }
        let (a, b) = pairs[i];
        let mut id = |k: u64| -> u32 {
            let n = keys.len() as u32;
            let v = ids.get_or_insert(k, n);
            if v == n {
                keys.push(k);
                parent.push(n);
            }
            v
        };
        let (x, y) = (id(a), id(b));
        let (rx, ry) = (find(&mut parent, x), find(&mut parent, y));
        if rx != ry {
            // The root is always the byte-wise smallest ID of its set.
            if key_lt(keys[rx as usize], keys[ry as usize]) {
                parent[ry as usize] = rx;
            } else {
                parent[rx as usize] = ry;
            }
        }
    }
    let mut resolve: KMap<u64> = KMap::with_capacity(keys.len());
    for i in 0..keys.len() as u32 {
        let r = find(&mut parent, i);
        if r != i {
            resolve.get_or_insert(keys[i as usize], keys[r as usize]);
        }
    }
    resolve
}
