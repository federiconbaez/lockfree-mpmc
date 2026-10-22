# Benchmarks

**Machine:** shared box, `Intel(R) Xeon(R) Processor`, 8 vCPU, Linux `Linux 6.12.94+ x86_64`.
**Date:** 2026-10-22 04:34 America (America/Buenos_Aires).
**Command:** `cargo bench --bench throughput` (criterion, 20 samples, 3 s measurement, 200 000 msgs, capacity 1024).
**Method:** non-blocking push/pop with spin on full/empty for `lockfree-mpmc` and `crossbeam-channel::try_recv`; `std::sync::mpsc` uses blocking `recv` (MPSC only, shown on 1p1c).

| Config | lockfree-mpmc | crossbeam-channel (bounded 1024) | std::sync::mpsc (sync_channel 1024) |
|--------|--------------:|---------------------------------:|------------------------------------:|
| 1p1c   | **30.8 M/s** (6.50 ms) | 5.38 M/s (37.2 ms) | **52.5 M/s** (3.81 ms) |
| 2p2c   | **4.85 M/s** (41.2 ms) | 4.13 M/s (48.5 ms) | n/a (MPSC only) |
| 4p4c   | **4.41 M/s** (45.3 ms) | 3.70 M/s (54.0 ms) | n/a |

Throughput is criterion's median (`thrpt` middle value). Times are wall-clock for 200 k messages.

## Reading the numbers

- On **1p1c**, `std::sync::mpsc` wins. That channel is MPSC-only and parks the consumer; our queue (and the crossbeam path in this bench) spin. A fair parking wrapper around this queue is future work.
- Against **crossbeam-channel** with the same non-blocking spin pattern, this queue is faster on every config measured here (≈5.7× on 1p1c, ≈1.2× on 2p2c / 4p4c).
- Absolute MPMC numbers drop under contention: four producers fighting one CAS on `head` is the classic Vyukov bottleneck. Padding head/tail helps false sharing; it does not remove the CAS hot spot.
- These figures are from one shared VM. Re-run on your hardware before citing them.

## Correctness (not throughput)

| Check | Result |
|-------|--------|
| `cargo test` (unit + proptest) | pass |
| `cargo +nightly miri test --lib` (strict provenance; stress test ignored) | pass |
| `RUSTFLAGS='--cfg loom' cargo test --test loom_mpmc --release` | pass (3 models) |
| `cargo clippy --all-targets -- -D warnings` | pass |
