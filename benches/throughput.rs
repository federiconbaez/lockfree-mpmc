//! Throughput comparison: this queue vs crossbeam-channel vs std::sync::mpsc.
//!
//! Run with: `cargo bench --bench throughput`
//! Results are written under `target/criterion/` and summarised in BENCH.md.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lockfree_mpmc::MpmcQueue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

const MSGS: u64 = 200_000;

fn run_mpmc(producers: usize, consumers: usize) {
    let q = Arc::new(MpmcQueue::new(1024));
    let per = MSGS / producers as u64;
    let remaining = Arc::new(AtomicU64::new(per * producers as u64));

    let ps: Vec<_> = (0..producers)
        .map(|_| {
            let q = Arc::clone(&q);
            thread::spawn(move || {
                for i in 0..per {
                    while q.push(i).is_err() {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();

    let cs: Vec<_> = (0..consumers)
        .map(|_| {
            let q = Arc::clone(&q);
            let remaining = Arc::clone(&remaining);
            thread::spawn(move || loop {
                if remaining.load(Ordering::Relaxed) == 0 {
                    break;
                }
                if q.pop().is_some() {
                    remaining.fetch_sub(1, Ordering::Relaxed);
                } else {
                    std::hint::spin_loop();
                }
            })
        })
        .collect();

    for p in ps {
        p.join().unwrap();
    }
    for c in cs {
        c.join().unwrap();
    }
    black_box(MSGS);
}

fn run_crossbeam(producers: usize, consumers: usize) {
    let (tx, rx) = crossbeam_channel::bounded::<u64>(1024);
    let per = MSGS / producers as u64;
    let remaining = Arc::new(AtomicU64::new(per * producers as u64));

    let ps: Vec<_> = (0..producers)
        .map(|_| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..per {
                    tx.send(i).unwrap();
                }
            })
        })
        .collect();
    drop(tx);

    let rx = Arc::new(rx);
    let cs: Vec<_> = (0..consumers)
        .map(|_| {
            let rx = Arc::clone(&rx);
            let remaining = Arc::clone(&remaining);
            thread::spawn(move || loop {
                if remaining.load(Ordering::Relaxed) == 0 {
                    break;
                }
                match rx.try_recv() {
                    Ok(_) => {
                        remaining.fetch_sub(1, Ordering::Relaxed);
                    }
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        std::hint::spin_loop();
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                }
            })
        })
        .collect();

    for p in ps {
        p.join().unwrap();
    }
    for c in cs {
        c.join().unwrap();
    }
    black_box(MSGS);
}

fn run_std_mpsc(producers: usize) {
    // std::sync::mpsc is multi-producer / single-consumer only.
    let (tx, rx) = mpsc::sync_channel::<u64>(1024);
    let per = MSGS / producers as u64;

    let ps: Vec<_> = (0..producers)
        .map(|_| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..per {
                    tx.send(i).unwrap();
                }
            })
        })
        .collect();
    drop(tx);

    let total = per * producers as u64;
    let c = thread::spawn(move || {
        let mut n = 0u64;
        while n < total {
            if rx.recv().is_ok() {
                n += 1;
            }
        }
    });

    for p in ps {
        p.join().unwrap();
    }
    c.join().unwrap();
    black_box(MSGS);
}

fn bench_group(c: &mut Criterion) {
    let mut group = c.benchmark_group("mpmc_throughput");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(20);
    group.throughput(Throughput::Elements(MSGS));

    for &(p, cons) in &[(1usize, 1usize), (2, 2), (4, 4)] {
        let label = format!("{p}p{cons}c");
        group.bench_with_input(
            BenchmarkId::new("lockfree_mpmc", &label),
            &(p, cons),
            |b, &(p, cons)| b.iter(|| run_mpmc(p, cons)),
        );
        group.bench_with_input(
            BenchmarkId::new("crossbeam_channel", &label),
            &(p, cons),
            |b, &(p, cons)| b.iter(|| run_crossbeam(p, cons)),
        );
        if cons == 1 {
            group.bench_with_input(BenchmarkId::new("std_mpsc", &label), &p, |b, &p| {
                b.iter(|| run_std_mpsc(p))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_group);
criterion_main!(benches);
