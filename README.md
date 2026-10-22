# lockfree-mpmc

> **ES:** Cola MPMC acotada y lock-free (algoritmo de Vyukov), con head/tail en líneas de caché separadas. Verificada con loom y Miri. Compilar/probar: `cargo test`.
> **EN:** Bounded lock-free MPMC queue (Vyukov algorithm), head/tail cache-padded. Checked with loom and Miri. Build/test: `cargo test`.
>
> **Estado / Status:** cifras en BENCH.md medidas en la box compartida el 2026-10-22; **no auditado** por un tercero / **not independently audited**. Licencia / License: Apache-2.0.

Bounded multi-producer / multi-consumer queue based on
[Dmitry Vyukov's algorithm](https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue).
Capacity is a power of two. `push` returns `Err(T)` when full; `pop` returns `None` when empty. No allocations after construction.

## Why this exists

Most Rust MPMC options are either unbounded, mutex-based, or pull in a large dependency. This crate is a small, auditable implementation of the classic sequence-number MPMC, with the checks that matter for `unsafe` concurrency code: loom for interleavings, Miri for stacked borrows / provenance, and a side-by-side criterion bench against `crossbeam-channel` and `std::sync::mpsc`.

## Usage

```rust
use lockfree_mpmc::MpmcQueue;
use std::sync::Arc;
use std::thread;

let q = Arc::new(MpmcQueue::new(1024));
let qp = Arc::clone(&q);
let producer = thread::spawn(move || {
    for i in 0..10_000u32 {
        while qp.push(i).is_err() { std::hint::spin_loop(); }
    }
});
let mut sum = 0u64;
for _ in 0..10_000 {
    loop {
        if let Some(v) = q.pop() { sum += v as u64; break; }
        std::hint::spin_loop();
    }
}
producer.join().unwrap();
assert_eq!(sum, (0..10_000u64).sum());
```

## Checks

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo +nightly miri test --lib          # MIRIFLAGS=-Zmiri-strict-provenance
RUSTFLAGS='--cfg loom' cargo test --test loom_mpmc --release
cargo bench --bench throughput
```

## Honest performance

Measured on the shared box (Intel Xeon, 8 vCPU, Linux 6.12), 200 k msgs, capacity 1024:

| Config | this crate | crossbeam-channel | std::sync::mpsc |
|--------|-----------:|------------------:|----------------:|
| 1p1c   | 30.8 M/s   | 5.38 M/s          | **52.5 M/s**    |
| 2p2c   | **4.85 M/s** | 4.13 M/s        | n/a             |
| 4p4c   | **4.41 M/s** | 3.70 M/s        | n/a             |

`std::sync::mpsc` wins on 1p1c (it parks; this bench spins). Against crossbeam with the same spin pattern, this queue is ahead on every config measured. Full methodology in [BENCH.md](./BENCH.md). Numbers will differ on your machine.

## License

Apache-2.0
