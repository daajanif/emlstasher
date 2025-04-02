# emlstasher

This is **a high-performance** `.eml` file parser and indexer written in Rust, designed to extract email content and attachments, and index them into local Elasticsearch.

---

## Features

- Recursively scans directories for `.eml` files
- Parses full email metadata, headers, body (text & HTML)
- Extracts and saves attachments
- Indexes into Elasticsearch with retry logic
- Logs all skipped, failed, and warning events to timestamped files
- Parallelized with `rayon` and async batched indexing with `tokio`

---

## Requirements

- Rust (Edition 2021)
- Elasticsearch 7.x+
- Recommended: Docker or local ES instance

---

## Dependencies

Key crates used:
- [`mail-parser`](https://crates.io/crates/mail-parser)
- [`elasticsearch`](https://crates.io/crates/elasticsearch)
- [`indicatif`](https://crates.io/crates/indicatif)
- [`rayon`](https://crates.io/crates/rayon)
- [`walkdir`](https://crates.io/crates/walkdir)
- [`chrono`](https://crates.io/crates/chrono)
- [`futures`](https://crates.io/crates/futures)
- [`parking_lot`](https://crates.io/crates/parking_lot) 

---

## Usage (Dev)

```bash
cargo run --release -- <email_directory> [--batch N] [--attachments <dir>]
```

---

## Building the App (Production)

You can build `emlstasher` for your platform using Cargo:

### Build

```bash
cargo build --release
# Output: ./target/release/emlstasher in Linux & Mac  
# Output: .\target\release\emlstasher.exe in Windows
```

### Run Example

```bash
# Linux/macOS
./emlstasher ./emails --batch 500 --attachments ./attachments
```

```bash
# Windows
emlstasher.exe "E:\eml\Example" --batch 1000 --attachments "E:\attachments"
```

## Command-Line Arguments

| Argument             | Description                                                                 | Required | Default         |
|----------------------|-----------------------------------------------------------------------------|----------|------------------|
| `<email_directory>`  | Path to the directory containing `.eml` files to process                    | ✅ Yes   | –                |
| `--batch <N>`        | Number of `.eml` files to process per batch                                 | ➖ No    | `50`             |
| `--attachments <dir>`| Directory to save extracted attachments                                     | ➖ No    | `attachments`    |



