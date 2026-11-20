# Shopping sessions pipeline

A single static Rust binary that cleans click-stream CSVs, links user IDs, sessionizes,
attributes sessions to ad clicks and writes two daily Parquet tables.

- Reads `$DATA_DIR` (default `/data`): `events/*.csv`, `clicks/*.csv`, `links/*.csv`,
  `products.csv`, `campaigns.csv`, `manifest.json`.
- Writes `$OUTPUT_DIR` (default `/output`): `user_daily/*.parquet`, `campaign_daily/*.parquet`.

## How it works

1. Input files are memory-mapped and cut into chunks parsed on all cores with a SIMD line scanner.
2. Events are de-duplicated through a lock-free hash table keyed by `event_id`.
3. Links are joined with union-find; every person is named by their smallest ID.
4. Rows are partitioned by person, sorted by time, sessionized and attributed.
5. Results are written with a small built-in Parquet writer (no compression).

## Build and test

```sh
make build        # docker buildx build --platform linux/amd64
make fixtures     # runs the image on every fixture and diffs the result
make run-dev      # runs the image on ./dev with the scorer's container flags
make check-dev    # compares ./out with ./dev-answer
```

Without Docker: `cargo build --release`, then
`python3 scripts/run_fixtures.py fixtures -- target/release/shopping-sessions`.
