//! Property-based tests: any sequence of pushes followed by the same number
//! of pops must return exactly those values (order may differ under MPMC, so
//! we sort before comparing). Under a single producer the order is FIFO.

use lockfree_mpmc::MpmcQueue;
use proptest::prelude::*;
use proptest::test_runner::Config as ProptestConfig;
use std::sync::Arc;
use std::thread;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn spsc_is_fifo(values in prop::collection::vec(any::<u32>(), 0..200)) {
        let cap = values.len().next_power_of_two().max(2);
        let q = MpmcQueue::new(cap);
        for &v in &values {
            q.push(v).unwrap();
        }
        let mut out = Vec::with_capacity(values.len());
        while let Some(v) = q.pop() {
            out.push(v);
        }
        prop_assert_eq!(out, values);
    }

    #[test]
    fn mpmc_preserves_multiset(
        n_per_thread in 1usize..64,
        producers in 1usize..4,
        consumers in 1usize..4,
    ) {
        let total = n_per_thread * producers;
        let cap = total.next_power_of_two().max(2);
        let q = Arc::new(MpmcQueue::new(cap));

        let handles: Vec<_> = (0..producers)
            .map(|p| {
                let q = Arc::clone(&q);
                thread::spawn(move || {
                    let base = (p * n_per_thread) as u32;
                    for i in 0..n_per_thread as u32 {
                        while q.push(base + i).is_err() {
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let per = total / consumers;
        let rem = total % consumers;
        let mut joins = Vec::new();
        for c in 0..consumers {
            let q = Arc::clone(&q);
            let want = per + if c < rem { 1 } else { 0 };
            joins.push(thread::spawn(move || {
                let mut got = Vec::with_capacity(want);
                while got.len() < want {
                    if let Some(v) = q.pop() {
                        got.push(v);
                    } else {
                        thread::yield_now();
                    }
                }
                got
            }));
        }
        let mut all = Vec::with_capacity(total);
        for j in joins {
            all.extend(j.join().unwrap());
        }
        all.sort_unstable();
        let expected: Vec<u32> = (0..total as u32).collect();
        prop_assert_eq!(all, expected);
    }
}
