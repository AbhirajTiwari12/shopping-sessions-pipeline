//! A minimal Parquet writer: one row group, one page per column, no compression.

use std::io::Write;

pub enum Col<'a> {
    /// Plain BYTE_ARRAY data already encoded as (u32 length, bytes) per value.
    Str { name: &'a str, data: &'a [u8] },
    I32 { name: &'a str, data: &'a [i32], date: bool },
    I64 { name: &'a str, data: &'a [i64] },
    /// Nullable dictionary-encoded string; index >= dict.len() is null.
    DictStr { name: &'a str, dict: &'a [Vec<u8>], idx: &'a [u8] },
}

struct Thrift {
    b: Vec<u8>,
    last: Vec<i16>,
    cur: i16,
}
impl Thrift {
    fn new() -> Self {
        Thrift { b: Vec::new(), last: Vec::new(), cur: 0 }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let d = id - self.cur;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        self.cur = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zz(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zz(v);
    }
    fn str(&mut self, id: i16, s: &[u8]) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn list(&mut self, id: i16, ty: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | ty);
        } else {
            self.b.push(0xF0 | ty);
            self.varint(n as u64);
        }
    }
    fn begin(&mut self, id: i16) {
        self.field(id, 12);
        self.push_struct();
    }
    fn push_struct(&mut self) {
        self.last.push(self.cur);
        self.cur = 0;
    }
    fn end(&mut self) {
        self.b.push(0);
        self.cur = self.last.pop().unwrap();
    }
}

fn page_header(kind: i32, size: usize, nvals: usize, enc: i32) -> Vec<u8> {
    let mut t = Thrift::new();
    t.i32(1, kind);
    t.i32(2, size as i32);
    t.i32(3, size as i32);
    if kind == 0 {
        t.begin(5);
        t.i32(1, nvals as i32);
        t.i32(2, enc);
        t.i32(3, 3);
        t.i32(4, 3);
        t.end();
    } else {
        t.begin(7);
        t.i32(1, nvals as i32);
        t.i32(2, 0);
        t.end();
    }
    t.b.push(0);
    t.b
}

struct Meta {
    ty: i32,
    encodings: Vec<i32>,
    start: usize,
    dict_off: Option<usize>,
    data_off: usize,
    size: usize,
}

pub fn write_file(path: &std::path::Path, nrows: usize, cols: &[Col]) {
    let mut f = std::fs::File::create(path).expect("create parquet");
    let mut pos = 0usize;
    let mut put = |f: &mut std::fs::File, b: &[u8], pos: &mut usize| {
        f.write_all(b).expect("write parquet");
        *pos += b.len();
    };
    put(&mut f, b"PAR1", &mut pos);
    let mut metas = Vec::new();
    if nrows > 0 {
        for c in cols {
            let start = pos;
            match c {
                Col::Str { data, .. } => {
                    put(&mut f, &page_header(0, data.len(), nrows, 0), &mut pos);
                    put(&mut f, data, &mut pos);
                    metas.push(Meta { ty: 6, encodings: vec![0, 3], start, dict_off: None, data_off: start, size: pos - start });
                }
                Col::I32 { data, .. } => {
                    let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) };
                    put(&mut f, &page_header(0, bytes.len(), nrows, 0), &mut pos);
                    put(&mut f, bytes, &mut pos);
                    metas.push(Meta { ty: 1, encodings: vec![0, 3], start, dict_off: None, data_off: start, size: pos - start });
                }
                Col::I64 { data, .. } => {
                    let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
                    put(&mut f, &page_header(0, bytes.len(), nrows, 0), &mut pos);
                    put(&mut f, bytes, &mut pos);
                    metas.push(Meta { ty: 2, encodings: vec![0, 3], start, dict_off: None, data_off: start, size: pos - start });
                }
                Col::DictStr { dict, idx, .. } => {
                    let mut d = Vec::new();
                    for s in dict.iter() {
                        d.extend_from_slice(&(s.len() as u32).to_le_bytes());
                        d.extend_from_slice(s);
                    }
                    put(&mut f, &page_header(2, d.len(), dict.len(), 0), &mut pos);
                    put(&mut f, &d, &mut pos);
                    let data_off = pos;
                    // Definition levels: one bit-packed run, bit width 1.
                    let nd = dict.len();
                    let groups = (nrows + 7) / 8;
                    let mut body = Vec::with_capacity(nrows / 4 + 32);
                    let mut defs = Thrift::new();
                    defs.varint(((groups as u64) << 1) | 1);
                    let mut bits = vec![0u8; groups];
                    let mut nn = 0usize;
                    for (i, &x) in idx.iter().enumerate() {
                        if (x as usize) < nd {
                            bits[i >> 3] |= 1 << (i & 7);
                            nn += 1;
                        }
                    }
                    defs.b.extend_from_slice(&bits);
                    body.extend_from_slice(&(defs.b.len() as u32).to_le_bytes());
                    body.extend_from_slice(&defs.b);
                    // Dictionary indexes: one bit-packed run.
                    let bw = (usize::BITS - nd.max(2).saturating_sub(1).leading_zeros()) as usize;
                    body.push(bw as u8);
                    let g = (nn + 7) / 8;
                    let mut t = Thrift::new();
                    t.varint(((g as u64) << 1) | 1);
                    body.extend_from_slice(&t.b);
                    let mut packed = vec![0u8; g * bw + 8];
                    let mut bit = 0usize;
                    for &x in idx.iter() {
                        if (x as usize) < nd {
                            let v = (x as u32) << (bit & 7);
                            let at = bit >> 3;
                            packed[at] |= v as u8;
                            packed[at + 1] |= (v >> 8) as u8;
                            bit += bw;
                        }
                    }
                    body.extend_from_slice(&packed[..g * bw]);
                    put(&mut f, &page_header(0, body.len(), nrows, 8), &mut pos);
                    put(&mut f, &body, &mut pos);
                    metas.push(Meta { ty: 6, encodings: vec![0, 3, 8], start, dict_off: Some(start), data_off, size: pos - start });
                }
            }
        }
    }
    // Footer.
    let mut t = Thrift::new();
    t.i32(1, 1);
    t.list(2, 12, cols.len() + 1);
    t.push_struct();
    t.str(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end();
    for c in cols {
        t.push_struct();
        let (ty, rep, name, conv, logical): (i32, i32, &str, Option<i32>, Option<i16>) = match c {
            Col::Str { name, .. } => (6, 0, name, Some(0), Some(1)),
            Col::I32 { name, date, .. } => {
                if *date {
                    (1, 0, name, Some(6), Some(6))
                } else {
                    (1, 0, name, None, None)
                }
            }
            Col::I64 { name, .. } => (2, 0, name, None, None),
            Col::DictStr { name, .. } => (6, 1, name, Some(0), Some(1)),
        };
        t.i32(1, ty);
        t.i32(3, rep);
        t.str(4, name.as_bytes());
        if let Some(cv) = conv {
            t.i32(6, cv);
        }
        if let Some(l) = logical {
            t.begin(10);
            t.begin(l);
            t.end();
            t.end();
        }
        t.end();
    }
    t.i64(3, nrows as i64);
    if nrows > 0 {
        t.list(4, 12, 1);
        t.push_struct();
        t.list(1, 12, cols.len());
        let mut total = 0usize;
        for (c, m) in cols.iter().zip(&metas) {
            let name = match c {
                Col::Str { name, .. } | Col::I32 { name, .. } | Col::I64 { name, .. } | Col::DictStr { name, .. } => name,
            };
            t.push_struct();
            t.i64(2, m.start as i64);
            t.begin(3);
            t.i32(1, m.ty);
            t.list(2, 5, m.encodings.len());
            for &e in &m.encodings {
                t.zz(e as i64);
            }
            t.list(3, 8, 1);
            t.varint(name.len() as u64);
            t.b.extend_from_slice(name.as_bytes());
            t.i32(4, 0);
            t.i64(5, nrows as i64);
            t.i64(6, m.size as i64);
            t.i64(7, m.size as i64);
            t.i64(9, m.data_off as i64);
            if let Some(d) = m.dict_off {
                t.i64(11, d as i64);
            }
            t.end();
            t.end();
            total += m.size;
        }
        t.i64(2, total as i64);
        t.i64(3, nrows as i64);
        t.end();
    } else {
        t.list(4, 12, 0);
    }
    t.str(6, b"shopping-sessions");
    t.b.push(0);
    let n = t.b.len() as u32;
    t.b.extend_from_slice(&n.to_le_bytes());
    t.b.extend_from_slice(b"PAR1");
    put(&mut f, &t.b, &mut pos);
}
