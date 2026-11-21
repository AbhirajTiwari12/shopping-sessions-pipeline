//! Low-level helpers: memory mapping, key packing, hashing, timestamp parsing, line scanning.

use std::collections::HashMap;
use std::sync::Mutex;

// ---------------------------------------------------------------- memory

pub struct Map {
    pub ptr: *const u8,
    pub len: usize,
}
unsafe impl Send for Map {}
unsafe impl Sync for Map {}

const SLACK: usize = 1 << 16;

/// Maps a file read-only with at least 64 KiB of readable zero bytes after its end,
/// so vector loads may run past the last byte.
pub fn map_file(path: &std::path::Path) -> Map {
    use std::os::unix::io::AsRawFd;
    let f = std::fs::File::open(path).expect("open input");
    let len = f.metadata().expect("stat").len() as usize;
    let total = (len + SLACK + 0xFFFF) & !0xFFFF;
    unsafe {
        let base = libc::mmap(
            std::ptr::null_mut(),
            total,
            libc::PROT_READ,
            libc::MAP_PRIVATE | libc::MAP_ANON,
            -1,
            0,
        );
        assert!(base != libc::MAP_FAILED, "mmap anon");
        if len > 0 {
            let p = libc::mmap(
                base,
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE | libc::MAP_FIXED,
                f.as_raw_fd(),
                0,
            );
            assert!(p != libc::MAP_FAILED, "mmap file");
        }
        Map { ptr: base as *const u8, len }
    }
}

/// Zeroed, lazily committed anonymous memory for `n` values of `T`.
pub fn alloc_zeroed<T>(n: usize) -> *mut T {
    alloc_zeroed_opt(n, true)
}
pub fn alloc_zeroed_opt<T>(n: usize, huge: bool) -> *mut T {
    let bytes = (n.max(1) * std::mem::size_of::<T>() + 0x1F_FFFF) & !0x1F_FFFF;
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANON | MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED, "mmap anon rw");
        #[cfg(target_os = "linux")]
        if huge && !no_huge() {
            libc::madvise(p, bytes, libc::MADV_HUGEPAGE);
        }
        let _ = huge;
        p as *mut T
    }
}
/// Fault in a range of anonymous memory in one kernel call instead of page by page.
#[inline]
pub fn populate<T>(p: *mut T, n: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        const MADV_POPULATE_WRITE: i32 = 23;
        let start = (p as usize) & !0xFFF;
        let end = (p as usize + n * std::mem::size_of::<T>() + 0xFFF) & !0xFFF;
        if end > start && !no_populate() {
            libc::madvise(start as *mut libc::c_void, end - start, MADV_POPULATE_WRITE);
        }
    }
    let _ = (p, n);
}
pub fn no_populate() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var("NO_POPULATE").is_ok())
}
/// Whether big anonymous regions ask for transparent huge pages by default.
pub const DEFAULT_HUGE: bool = true;
pub fn no_huge() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| if DEFAULT_HUGE { std::env::var("NO_HUGE").is_ok() } else { std::env::var("FORCE_HUGE").is_err() })
}
#[cfg(target_os = "linux")]
const MAP_NORESERVE: i32 = libc::MAP_NORESERVE;
#[cfg(not(target_os = "linux"))]
const MAP_NORESERVE: i32 = 0;

pub struct P<T>(pub *mut T);
impl<T> Clone for P<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for P<T> {}
impl<T> P<T> {
    #[inline(always)]
    pub fn p(self) -> *mut T {
        self.0
    }
}
unsafe impl<T> Send for P<T> {}
unsafe impl<T> Sync for P<T> {}

#[inline(always)]
pub fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, preserves_flags));
    }
}

/// Prefetch for writing: the line arrives exclusive, ready for a compare-and-swap.
#[inline(always)]
pub fn prefetch_w<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::asm!("prefetchw byte ptr [{0}]", in(reg) p, options(nostack, preserves_flags, readonly));
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pstl1keep, [{0}]", in(reg) p, options(nostack, preserves_flags));
    }
}

// ---------------------------------------------------------------- keys

/// IDs of up to 9 ASCII bytes pack losslessly into 63 bits, in an order-preserving way.
/// Anything else is interned here and gets a key with the top bit set.
static SLOW: Mutex<Option<(HashMap<Vec<u8>, u64>, Vec<Vec<u8>>)>> = Mutex::new(None);

#[cold]
#[inline(never)]
fn pack_slow(s: &[u8]) -> u64 {
    let mut g = SLOW.lock().unwrap();
    let (map, list) = g.get_or_insert_with(|| (HashMap::new(), Vec::new()));
    if let Some(&k) = map.get(s) {
        return k;
    }
    let k = (1u64 << 63) | list.len() as u64;
    list.push(s.to_vec());
    map.insert(s.to_vec(), k);
    k
}

const LEN_MASK: [u64; 9] = [
    0,
    0xFF,
    0xFFFF,
    0xFF_FFFF,
    0xFFFF_FFFF,
    0xFF_FFFF_FFFF,
    0xFFFF_FFFF_FFFF,
    0xFF_FFFF_FFFF_FFFF,
    !0,
];

/// Packs the ID at `p` of `len` bytes. `p` must be readable for 16 bytes. Empty gives 0.
#[inline(always)]
pub unsafe fn pack(p: *const u8, len: usize) -> u64 {
    if len <= 9 {
        let m = LEN_MASK[len.min(8)];
        let a = (p as *const u64).read_unaligned().to_le() & m;
        let b = if len == 9 { *p.add(8) as u64 } else { 0 };
        let t = a | !m;
        let zero = t.wrapping_sub(0x0101_0101_0101_0101) & !t & 0x8080_8080_8080_8080;
        if (a & 0x8080_8080_8080_8080) | zero | (b & 0x80) == 0 && (len < 9 || b != 0) {
            #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
            let x = std::arch::x86_64::_pext_u64(a.swap_bytes(), 0x7F7F_7F7F_7F7F_7F7F);
            #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
            let x = {
                let mut x = a.swap_bytes();
                x = ((x & 0x7F00_7F00_7F00_7F00) >> 1) | (x & 0x007F_007F_007F_007F);
                x = ((x & 0x3FFF_0000_3FFF_0000) >> 2) | (x & 0x0000_3FFF_0000_3FFF);
                x = ((x & 0x0FFF_FFFF_0000_0000) >> 4) | (x & 0x0000_0000_0FFF_FFFF);
                x
            };
            return (x << 7) | b;
        }
    }
    pack_slow(std::slice::from_raw_parts(p, len))
}

pub fn pack_slice(s: &[u8]) -> u64 {
    let mut buf = [0u8; 16];
    if s.len() <= 9 {
        buf[..s.len()].copy_from_slice(s);
        unsafe { pack(buf.as_ptr(), s.len()) }
    } else {
        pack_slow(s)
    }
}

/// Appends the bytes of a key.
pub fn unpack(key: u64, out: &mut Vec<u8>) {
    if key >> 63 != 0 {
        let g = SLOW.lock().unwrap();
        out.extend_from_slice(&g.as_ref().unwrap().1[(key & !(1 << 63)) as usize]);
        return;
    }
    let b9 = (key & 0x7F) as u8;
    let mut x = key >> 7;
    x = ((x & 0x00FF_FFFF_F000_0000) << 4) | (x & 0x0FFF_FFFF);
    x = ((x & 0x0FFF_C000_0FFF_C000) << 2) | (x & 0x0000_3FFF_0000_3FFF);
    x = ((x & 0x3F80_3F80_3F80_3F80) << 1) | (x & 0x007F_007F_007F_007F);
    let bytes = x.to_be_bytes();
    let n = (x.trailing_zeros() / 8) as usize;
    let len = if x == 0 { 0 } else { 8 - n };
    out.extend_from_slice(&bytes[..len]);
    if b9 != 0 {
        out.push(b9);
    }
}

/// Byte-wise `a < b` on keys.
#[inline(always)]
pub fn key_lt(a: u64, b: u64) -> bool {
    if (a | b) >> 63 == 0 {
        a < b
    } else {
        key_lt_slow(a, b)
    }
}
#[cold]
fn key_lt_slow(a: u64, b: u64) -> bool {
    let (mut x, mut y) = (Vec::new(), Vec::new());
    unpack(a, &mut x);
    unpack(b, &mut y);
    x < y
}

/// One multiplication: the top bits are well mixed, which is all a slot or a
/// partition number needs.
#[inline(always)]
pub fn h1(k: u64) -> u64 {
    k.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

#[inline(always)]
pub fn mix(k: u64) -> u64 {
    let mut h = k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^= h >> 32;
    h = h.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    h ^= h >> 32;
    h
}

/// Open-addressing map from non-zero u64 keys; key and value share a cache line.
pub struct KMap<V: Copy + Default> {
    pub e: Vec<(u64, V)>,
    pub shift: u32,
    pub mask: usize,
    pub len: usize,
}
impl<V: Copy + Default> KMap<V> {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n.max(8) * 2).next_power_of_two();
        KMap { e: vec![(0, V::default()); cap], shift: 64 - cap.trailing_zeros(), mask: cap - 1, len: 0 }
    }
    #[inline(always)]
    pub fn slot(&self, k: u64) -> usize {
        (mix(k) >> self.shift) as usize
    }
    #[inline(always)]
    pub fn prefetch(&self, k: u64) {
        prefetch(unsafe { self.e.as_ptr().add(self.slot(k)) });
    }
    #[inline(always)]
    pub fn get(&self, k: u64) -> Option<V> {
        let mut i = self.slot(k);
        loop {
            let c = unsafe { *self.e.get_unchecked(i) };
            if c.0 == k {
                return Some(c.1);
            }
            if c.0 == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Returns the value for `k`, inserting `v` if absent.
    #[inline(always)]
    pub fn get_or_insert(&mut self, k: u64, v: V) -> V {
        debug_assert!(self.len * 2 <= self.mask + 1);
        let mut i = self.slot(k);
        loop {
            let c = unsafe { *self.e.get_unchecked(i) };
            if c.0 == k {
                return c.1;
            }
            if c.0 == 0 {
                self.e[i] = (k, v);
                self.len += 1;
                return v;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// A fixed-capacity, lazily committed buffer: pushing never reallocates.
pub struct Arena<T> {
    pub ptr: *mut T,
    pub len: usize,
    pub cap: usize,
}
unsafe impl<T> Send for Arena<T> {}
unsafe impl<T> Sync for Arena<T> {}
impl<T> Arena<T> {
    pub fn new(cap: usize, huge: bool) -> Self {
        Arena { ptr: alloc_zeroed_opt::<T>(cap, huge), len: 0, cap }
    }
    #[inline(always)]
    pub fn push(&mut self, v: T) {
        debug_assert!(self.len < self.cap);
        unsafe { self.ptr.add(self.len).write(v) };
        self.len += 1;
    }
    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

// ---------------------------------------------------------------- timestamps

pub const INVALID: i64 = i64::MIN;

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

pub fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, doy - (153 * mp + 2) / 5 + 1)
}

/// Remembers the day number of the dates seen, keyed by their 10 bytes.
pub struct DateCache {
    a: [u64; 64],
    b: [u16; 64],
    days: [i64; 64],
}
impl DateCache {
    pub fn new() -> Self {
        DateCache { a: [0; 64], b: [0; 64], days: [0; 64] }
    }
}

#[inline(always)]
fn dig(c: u8) -> Option<i64> {
    let d = c.wrapping_sub(b'0');
    if d < 10 {
        Some(d as i64)
    } else {
        None
    }
}

#[cold]
#[inline(never)]
unsafe fn date_slow(p: *const u8) -> Option<i64> {
    let s = std::slice::from_raw_parts(p, 10);
    if s[4] != b'-' || s[7] != b'-' {
        return None;
    }
    let y = dig(s[0])? * 1000 + dig(s[1])? * 100 + dig(s[2])? * 10 + dig(s[3])?;
    let m = dig(s[5])? * 10 + dig(s[6])?;
    let d = dig(s[8])? * 10 + dig(s[9])?;
    if !(1..=12).contains(&m) || d < 1 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let dim = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => 28 + i64::from(leap),
    };
    if d > dim {
        return None;
    }
    Some(days_from_civil(y, m, d))
}

/// `YYYY-MM-DDTHH:MM:SS` (19 bytes) to seconds since the epoch.
#[inline(always)]
unsafe fn parse_dt19(p: *const u8, dc: &mut DateCache) -> Option<i64> {
    let a = (p as *const u64).read_unaligned();
    let b = (p.add(8) as *const u16).read_unaligned();
    let i = ((b as usize) ^ (b as usize >> 6)) & 63;
    let days = if dc.a[i] == a && dc.b[i] == b {
        dc.days[i]
    } else {
        let d = date_slow(p)?;
        dc.a[i] = a;
        dc.b[i] = b;
        dc.days[i] = d;
        d
    };
    if *p.add(10) != b'T' {
        return None;
    }
    let x = (p.add(11) as *const u64).read_unaligned().to_le();
    if x & 0xF0F0_FFF0_F0FF_F0F0 != 0x3030_3A30_303A_3030
        || x.wrapping_add(0x0606_0006_0600_0606) & 0xF0F0_00F0_F000_F0F0 != 0x3030_0030_3000_3030
    {
        return None;
    }
    let d = x & 0x0F0F_000F_0F00_0F0F;
    // Tens times ten plus ones for all three pairs at once: bytes 0, 3 and 6.
    let y = d.wrapping_mul(10) + (d >> 8);
    let h = y & 0xFF;
    let m = (y >> 24) & 0xFF;
    let s = (y >> 48) & 0xFF;
    if h > 23 || m > 59 || s > 59 {
        return None;
    }
    Some(days * 86_400 + (h * 3600 + m * 60 + s) as i64)
}

/// Form F1 only, to seconds; `INVALID` otherwise.
#[inline(always)]
pub unsafe fn parse_f1(p: *const u8, len: usize, dc: &mut DateCache) -> i64 {
    if len == 20 && *p.add(19) == b'Z' {
        if let Some(s) = parse_dt19(p, dc) {
            return s;
        }
    }
    INVALID
}

/// Any of the three forms to milliseconds; `INVALID` for a bad timestamp.
#[inline(always)]
pub unsafe fn parse_ts(p: *const u8, len: usize, dc: &mut DateCache) -> i64 {
    if len == 20 && *p.add(19) == b'Z' {
        return match parse_dt19(p, dc) {
            Some(s) => s * 1000,
            None => INVALID,
        };
    }
    if len == 25 && (*p.add(19) == b'+' || *p.add(19) == b'-') {
        return parse_f2(p, dc);
    }
    parse_f3(p, len)
}

#[inline(never)]
unsafe fn parse_f2(p: *const u8, dc: &mut DateCache) -> i64 {
    let Some(s) = parse_dt19(p, dc) else { return INVALID };
    let t = std::slice::from_raw_parts(p.add(19), 6);
    if t[3] != b':' {
        return INVALID;
    }
    let (Some(h1), Some(h0), Some(m1), Some(m0)) = (dig(t[1]), dig(t[2]), dig(t[4]), dig(t[5]))
    else {
        return INVALID;
    };
    let (h, m) = (h1 * 10 + h0, m1 * 10 + m0);
    if h > 23 || m > 59 {
        return INVALID;
    }
    let off = (h * 3600 + m * 60) * if t[0] == b'-' { -1 } else { 1 };
    (s - off) * 1000
}

#[inline(always)]
fn parse8(v: u64) -> u64 {
    // Eight digit values, most significant in the lowest byte.
    let v = (v.wrapping_mul(2561)) >> 8;
    let v = ((v & 0x00FF_00FF_00FF_00FF).wrapping_mul(6_553_601)) >> 16;
    ((v & 0x0000_FFFF_0000_FFFF).wrapping_mul(42_949_672_960_001)) >> 32
}

#[inline(always)]
unsafe fn parse_f3(p: *const u8, len: usize) -> i64 {
    if len.wrapping_sub(1) < 16 {
        // Up to 16 digits: right-align them in 16 bytes, padded with zeros on the left.
        const ZEROS: u128 = 0x3030_3030_3030_3030_3030_3030_3030_3030;
        let x = u128::from_le((p as *const u128).read_unaligned());
        let sh = 8 * (16 - len as u32);
        let x = (x << sh) | (ZEROS & ((1u128 << sh) - 1));
        let t = x ^ ZEROS;
        if (t | t.wrapping_add(0x0606_0606_0606_0606_0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0_F0F0_F0F0_F0F0_F0F0 != 0 {
            return INVALID;
        }
        return (parse8(t as u64) * 100_000_000 + parse8((t >> 64) as u64)) as i64;
    }
    parse_f3_slow(p, len)
}

#[inline(never)]
unsafe fn parse_f3_slow(p: *const u8, len: usize) -> i64 {
    if len == 0 {
        return INVALID;
    }
    let mut v: i64 = 0;
    let mut i = 0;
    // Leading zeros are fine; more than 18 significant digits is outside any window.
    while i < len && *p.add(i) == b'0' {
        i += 1;
    }
    if len - i > 18 {
        return INVALID;
    }
    while i < len {
        let d = (*p.add(i)).wrapping_sub(b'0');
        if d > 9 {
            return INVALID;
        }
        v = v * 10 + d as i64;
        i += 1;
    }
    v
}

/// Digits only, value 1 to 10,000,000 (leading zeros allowed); 0 if bad.
#[inline(always)]
pub unsafe fn parse_price(p: *const u8, len: usize) -> u32 {
    if len == 0 {
        return 0;
    }
    let mut i = 0;
    if len > 8 {
        while i < len && *p.add(i) == b'0' {
            i += 1;
        }
        if len - i > 8 {
            return 0;
        }
    }
    let mut v: u32 = 0;
    while i < len {
        let d = (*p.add(i)).wrapping_sub(b'0');
        if d > 9 {
            return 0;
        }
        v = v * 10 + d as u32;
        i += 1;
    }
    if v > 10_000_000 {
        0
    } else {
        v
    }
}

// ---------------------------------------------------------------- scanning

/// Bit masks of the commas, quotes and newlines in the 64 bytes at `p`.
#[inline(always)]
pub unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
    {
        use std::arch::x86_64::*;
        let v = _mm512_loadu_si512(p as *const _);
        return (
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8)),
        );
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512bw")))]
    {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p as *const _);
        let b = _mm256_loadu_si256(p.add(32) as *const _);
        let f = |c: u8| -> u64 {
            let k = _mm256_set1_epi8(c as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, k)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, k)) as u32 as u64;
            lo | (hi << 32)
        };
        return (f(b','), f(b'"'), f(b'\n'));
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    {
        use std::arch::x86_64::*;
        let v = [
            _mm_loadu_si128(p as *const _),
            _mm_loadu_si128(p.add(16) as *const _),
            _mm_loadu_si128(p.add(32) as *const _),
            _mm_loadu_si128(p.add(48) as *const _),
        ];
        let f = |c: u8| -> u64 {
            let k = _mm_set1_epi8(c as i8);
            let mut r = 0u64;
            for i in 0..4 {
                r |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v[i], k)) as u32 as u64) << (16 * i);
            }
            r
        };
        return (f(b','), f(b'"'), f(b'\n'));
    }
    #[cfg(target_arch = "aarch64")]
    {
        use std::arch::aarch64::*;
        let v = [vld1q_u8(p), vld1q_u8(p.add(16)), vld1q_u8(p.add(32)), vld1q_u8(p.add(48))];
        const BM: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bm = vld1q_u8(BM.as_ptr());
        let f = |c: u8| -> u64 {
            let k = vdupq_n_u8(c);
            let t0 = vandq_u8(vceqq_u8(v[0], k), bm);
            let t1 = vandq_u8(vceqq_u8(v[1], k), bm);
            let t2 = vandq_u8(vceqq_u8(v[2], k), bm);
            let t3 = vandq_u8(vceqq_u8(v[3], k), bm);
            let s0 = vpaddq_u8(t0, t1);
            let s1 = vpaddq_u8(t2, t3);
            let s = vpaddq_u8(s0, s1);
            let s = vpaddq_u8(s, s);
            vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
        };
        return (f(b','), f(b'"'), f(b'\n'));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let s = std::slice::from_raw_parts(p, 64);
        let f = |c: u8| -> u64 {
            let mut r = 0u64;
            for i in 0..64 {
                r |= ((s[i] == c) as u64) << i;
            }
            r
        };
        return (f(b','), f(b'"'), f(b'\n'));
    }
}

/// Calls `f(line_start, comma_positions, comma_count, line_end, quote_count)` for every
/// line that starts in `[start, end)` of the file, the header excluded.
/// The file must be mapped with zero slack after `flen`.
#[inline(always)]
pub unsafe fn scan_lines<F: FnMut(usize, &[usize; 8], usize, usize, u32)>(
    data: *const u8,
    flen: usize,
    start: usize,
    end: usize,
    mut f: F,
) {
    if flen == 0 || start >= flen {
        return;
    }
    // First line of ours: after the header for the first chunk, else after the first
    // newline at or after start - 1.
    let mut b = if start == 0 { 0 } else { start - 1 };
    let mut line_start;
    loop {
        if b >= flen {
            return;
        }
        let (_, _, n) = masks64(data.add(b));
        if n != 0 {
            line_start = b + n.trailing_zeros() as usize + 1;
            break;
        }
        b += 64;
    }
    if line_start >= end || line_start >= flen {
        return;
    }
    let mut commas = [0usize; 8];
    let mut nf = 0usize;
    let mut qcarry = 0u32;
    b = line_start;
    // The end of the file acts as a final newline, so the handler has one call site.
    while b <= flen {
        let (c, q, mut n) = masks64(data.add(b));
        if flen - b < 64 {
            n |= 1u64 << (flen - b);
        }
        let mut m = c | n;
        let mut qrem = q;
        while m != 0 {
            let i = m.trailing_zeros();
            let bit = 1u64 << i;
            m &= m - 1;
            let p = b + i as usize;
            if n & bit == 0 {
                commas[nf & 7] = p;
                nf += 1;
            } else {
                let below = bit - 1;
                let qc = qcarry + (qrem & below).count_ones();
                qrem &= !below;
                qcarry = 0;
                f(line_start, &commas, nf, p, qc);
                line_start = p + 1;
                nf = 0;
                if line_start >= end {
                    return;
                }
            }
        }
        qcarry += qrem.count_ones();
        b += 64;
    }
}

/// The `N` fields of a line as (start, length), quotes stripped; `None` if malformed.
#[inline(always)]
pub unsafe fn fields<const N: usize>(
    d: *const u8,
    ls: usize,
    commas: &[usize; 8],
    nf: usize,
    nl: usize,
    qc: u32,
) -> Option<[(usize, usize); N]> {
    if nf != N - 1 {
        return None;
    }
    let mut out = [(0usize, 0usize); N];
    let mut s = ls;
    for i in 0..N {
        let e = if i == N - 1 { nl } else { commas[i] };
        out[i] = (s, e - s);
        s = e + 1;
    }
    // Quotes are syntax only around a whole field; any other quote makes the line malformed.
    let mut w = 0u32;
    for i in 0..N {
        let (s, l) = out[i];
        let q = ((l >= 2) & (*d.add(s) == b'"') & (*d.add(s + l).sub(1) == b'"')) as usize;
        out[i] = (s + q, l - 2 * q);
        w += q as u32;
    }
    if qc != 2 * w {
        return None;
    }
    Some(out)
}

// ---------------------------------------------------------------- batched scanning

/// Bytes scanned per batch: separator positions for about this much input are
/// collected first, then the lines are decoded from them.
pub const BATCH: usize = 8 << 10;

/// Reusable buffers for `scan_batches`.
pub struct ScanBuf {
    idx: Vec<u32>,
    nlp: Vec<u32>,
    qcum: Vec<u32>,
}
impl ScanBuf {
    pub fn new() -> Self {
        ScanBuf { idx: vec![0; BATCH * 2 + 256], nlp: vec![0; BATCH + 256], qcum: vec![0; BATCH + 256] }
    }
}

pub trait LineSink {
    /// A line with exactly the expected number of commas. `seps` points at the offsets
    /// (relative to `base`) of its commas and its newline; the entry before them is the
    /// separator that precedes the line. The line spans `ls..nl` and holds `qc` quotes.
    unsafe fn line(&mut self, ls: usize, seps: *const u32, base: usize, nl: usize, qc: u32);
    /// Called after the lines of a batch.
    unsafe fn flush(&mut self);
}

/// Like `scan_lines`, for lines of `K` fields, in two stages per batch: first the
/// positions of all commas and newlines are written out, then each line is handed to
/// the sink. Lines with another number of commas are skipped (they are malformed).
#[inline(always)]
pub unsafe fn scan_batches<S: LineSink, const K: usize>(data: *const u8, flen: usize, start: usize, end: usize, buf: &mut ScanBuf, sink: &mut S) {
    if flen == 0 || start >= flen {
        return;
    }
    let mut b = if start == 0 { 0 } else { start - 1 };
    let line_start;
    loop {
        if b >= flen {
            return;
        }
        let (_, _, n) = masks64(data.add(b));
        if n != 0 {
            line_start = b + n.trailing_zeros() as usize + 1;
            break;
        }
        b += 64;
    }
    if line_start >= end || line_start >= flen {
        return;
    }
    let base = line_start;
    b = line_start;
    let mut ls = line_start;
    // Slot 0 of the index holds the separator before the first line of the batch
    // (the previous newline); the separators proper start at slot 1.
    *buf.idx.as_mut_ptr() = u32::MAX;
    let (mut ni, mut nn) = (0usize, 0usize);
    let (mut qrun, mut qprev) = (0u32, 0u32);
    let mut done_scan = false;
    let mut last_nl_end = 0usize;
    loop {
        // Stage 1: separator positions for the next batch of input.
        let target = b + BATCH;
        while b < target && !done_scan {
            if ni + 80 > buf.idx.len() {
                let n = buf.idx.len() * 2;
                buf.idx.resize(n, 0);
            }
            if nn + 66 > buf.nlp.len() {
                let n = buf.nlp.len() * 2;
                buf.nlp.resize(n, 0);
                buf.qcum.resize(n, 0);
            }
            let (c, q, mut n) = masks64(data.add(b));
            // The end of the file acts as a final newline.
            if flen - b < 64 {
                n |= 1u64 << (flen - b);
            }
            let off = (b - base) as u32;
            let mut m = c | n;
            let cnt = m.count_ones() as usize;
            let out = buf.idx.as_mut_ptr().add(1 + ni);
            // Eight unconditional scalar stores (kept out of the vectorizer's hands:
            // packing them into a vector costs more than the stores).
            for k in 0..8 {
                out.add(k).write_volatile(off + m.trailing_zeros());
                m &= m.wrapping_sub(1);
            }
            if cnt > 8 {
                let mut k = 8;
                while m != 0 {
                    *out.add(k) = off + m.trailing_zeros();
                    m &= m - 1;
                    k += 1;
                }
            }
            ni += cnt;
            // Up to two newlines without a data-dependent branch (a third needs lines
            // under 32 bytes); the loop only runs for the rest.
            let ncnt = n.count_ones() as usize;
            let mut nm = n;
            for k in 0..2 {
                let i = nm.trailing_zeros();
                *buf.nlp.as_mut_ptr().add(nn + k) = off + i;
                *buf.qcum.as_mut_ptr().add(nn + k) = qrun + (q & (1u64 << (i & 63)).wrapping_sub(1)).count_ones();
                nm &= nm.wrapping_sub(1);
            }
            if ncnt > 2 {
                let mut k = 2;
                while nm != 0 {
                    let i = nm.trailing_zeros();
                    *buf.nlp.as_mut_ptr().add(nn + k) = off + i;
                    *buf.qcum.as_mut_ptr().add(nn + k) = qrun + (q & ((1u64 << i) - 1)).count_ones();
                    nm &= nm - 1;
                    k += 1;
                }
            }
            nn += ncnt;
            // Highest newline of the block, if any.
            let top = 63 - (n | 1).leading_zeros() as usize;
            last_nl_end = if n != 0 { b + top + 1 } else { last_nl_end };
            qrun += q.count_ones();
            b += 64;
            // Lines that start at or after `end` belong to the next chunk.
            done_scan = last_nl_end >= end || b > flen;
        }
        // No stale entry may pass for a newline.
        for k in 0..8 {
            *buf.idx.as_mut_ptr().add(1 + ni + k) = u32::MAX;
        }
        // Stage 2: the lines.
        let idx = buf.idx.as_ptr().add(1);
        let mut ip = 0usize;
        let mut finished = false;
        for j in 0..nn {
            if ls >= end {
                finished = true;
                break;
            }
            let nlo = *buf.nlp.as_ptr().add(j);
            let nl = base + nlo as usize;
            let qc = *buf.qcum.as_ptr().add(j) - qprev;
            qprev = *buf.qcum.as_ptr().add(j);
            if *idx.add(ip + K - 1) == nlo {
                sink.line(ls, idx.add(ip), base, nl, qc);
                ip += K;
            } else {
                while *idx.add(ip) != nlo {
                    ip += 1;
                }
                ip += 1;
            }
            ls = nl + 1;
        }
        sink.flush();
        if finished || done_scan {
            break;
        }
        // Separators of the line still open move to the front.
        let rem = ni - ip;
        std::ptr::copy(buf.idx.as_ptr().add(1 + ip), buf.idx.as_mut_ptr().add(1), rem);
        *buf.idx.as_mut_ptr() = ((ls - base) as u32).wrapping_sub(1);
        ni = rem;
        nn = 0;
    }
}

/// Whether the field `s..e` is wrapped in quotes.
#[macro_export]
macro_rules! wrapped {
    ($d:expr, $s:expr, $e:expr) => {{
        let (s, e): (usize, usize) = ($s, $e);
        ((e - s >= 2) & (*$d.add(s) == b'"') & (*$d.add(e).sub(1) == b'"')) as u32
    }};
}

/// One field of a line whose quotes are known to be well formed (every quote wraps a
/// whole field), so a leading quote means the field is wrapped. Gives (start, length).
#[macro_export]
macro_rules! field {
    ($d:expr, $s:expr, $e:expr) => {{
        let (s, e): (usize, usize) = ($s, $e);
        let q = (*$d.add(s) == b'"') as usize;
        (s + q, e - s - 2 * q)
    }};
}

/// Splits a line of `K` fields. `seps[-1]` is the separator before the line and
/// `seps[0..K]` those ending its fields, as offsets from `d`. Fills the start (offset
/// from `d`) and length of every field, quotes stripped, and returns how many fields
/// are wrapped in quotes: the line is well formed when its quotes are exactly twice that.
/// Whether `split_fields` uses a vector gather. Gathers are slow on some CPUs (and
/// mitigations make them slower on others), so the scalar walk is the default.
pub const USE_GATHER: bool = false;

#[inline(always)]
pub unsafe fn split_fields<const K: usize>(d: *const u8, seps: *const u32, starts: &mut [u32; 8], lens: &mut [u32; 8]) -> u32 {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if USE_GATHER {
        use std::arch::x86_64::*;
        const LANES: [i32; 16] = [-1, -1, -1, -1, -1, -1, -1, -1, 0, 0, 0, 0, 0, 0, 0, 0];
        let lanes = _mm256_loadu_si256(LANES.as_ptr().add(8 - K) as *const __m256i);
        let p = _mm256_loadu_si256(seps.sub(1) as *const __m256i);
        let e = _mm256_loadu_si256(seps as *const __m256i);
        let one = _mm256_set1_epi32(1);
        let fi = _mm256_add_epi32(p, one);
        let la = _mm256_sub_epi32(e, one);
        // First and last byte of every field at once.
        let zero = _mm256_setzero_si256();
        let first = _mm256_mask_i32gather_epi32::<1>(zero, d as *const i32, fi, lanes);
        let last = _mm256_mask_i32gather_epi32::<1>(zero, d as *const i32, la, lanes);
        let ff = _mm256_set1_epi32(0xFF);
        let qt = _mm256_set1_epi32(0x22);
        let fq = _mm256_cmpeq_epi32(_mm256_and_si256(first, ff), qt);
        let lq = _mm256_cmpeq_epi32(_mm256_and_si256(last, ff), qt);
        let len = _mm256_sub_epi32(e, fi);
        // Wrapped: starts and ends with a quote and is not a lone quote.
        let w = _mm256_and_si256(_mm256_andnot_si256(_mm256_cmpeq_epi32(len, one), _mm256_and_si256(fq, lq)), lanes);
        _mm256_storeu_si256(starts.as_mut_ptr() as *mut __m256i, _mm256_sub_epi32(fi, w));
        _mm256_storeu_si256(lens.as_mut_ptr() as *mut __m256i, _mm256_add_epi32(len, _mm256_add_epi32(w, w)));
        return (_mm256_movemask_ps(_mm256_castsi256_ps(w)) as u32).count_ones();
    }
    let mut wq = 0u32;
    let mut p = *seps.sub(1);
    for k in 0..K {
        let e = *seps.add(k);
        let s = p.wrapping_add(1);
        let len = e.wrapping_sub(s);
        // An empty field's "first byte" is its separator, never a quote; a lone quote
        // is not a wrapper.
        let q = ((*d.offset(s as i32 as isize) == b'"') & (*d.offset(e as i32 as isize - 1) == b'"') & (len != 1)) as u32;
        *starts.get_unchecked_mut(k) = s + q;
        *lens.get_unchecked_mut(k) = len - 2 * q;
        wq += q;
        p = e;
    }
    wq
}
