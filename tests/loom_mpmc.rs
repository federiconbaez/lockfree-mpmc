//! Model-checked interleavings with `loom`.
//!
//! Run with:
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test --test loom_mpmc --release
//! ```
//! Under a normal build the tests are skipped so `cargo test` stays fast.

#![cfg(loom)]

use lockfree_mpmc::MpmcQueue;
use loom::sync::Arc;
use loom::thread;

/// Bounded spin: loom explores every yield, so unbounded loops explode the
/// state space. A handful of yields is enough for capacity-2 hand-offs.
fn pop_with_yields(q: &MpmcQueue<u32>, max_yields: usize) -> Option<u32> {
    for _ in 0..max_yields {
        if let Some(v) = q.pop() {
            return Some(v);
        }
        thread::yield_now();
    }
    q.pop()
}

#[test]
fn spsc_preserves_order() {
    loom::model(|| {
        let q = Arc::new(MpmcQueue::new(2));
        let qp = Arc::clone(&q);
        let p = thread::spawn(move || {
            qp.push(10).unwrap();
            qp.push(20).unwrap();
        });
        let qc = Arc::clone(&q);
        let c = thread::spawn(move || {
            let a = pop_with_yields(&qc, 4).expect("first");
            let b = pop_with_yields(&qc, 4).expect("second");
            vec![a, b]
        });
        p.join().unwrap();
        assert_eq!(c.join().unwrap(), vec![10, 20]);
    });
}

#[test]
fn two_producers_one_consumer() {
    // 2P/1C keeps the state space small while still exercising the MPMC CAS
    // on the producer side. Capacity 2 so the second push may observe "full".
    loom::model(|| {
        let q = Arc::new(MpmcQueue::new(2));

        let q1 = Arc::clone(&q);
        let p1 = thread::spawn(move || {
            // Retry a few times in case the other producer filled the slot.
            for _ in 0..4 {
                if q1.push(1).is_ok() {
                    return;
                }
                thread::yield_now();
            }
            q1.push(1).expect("p1");
        });
        let q2 = Arc::clone(&q);
        let p2 = thread::spawn(move || {
            for _ in 0..4 {
                if q2.push(2).is_ok() {
                    return;
                }
                thread::yield_now();
            }
            q2.push(2).expect("p2");
        });

        p1.join().unwrap();
        p2.join().unwrap();

        // After both producers finish, both values must be present.
        let mut got = [q.pop().expect("a"), q.pop().expect("b")];
        got.sort_unstable();
        assert_eq!(got, [1, 2]);
        assert!(q.pop().is_none());
    });
}

#[test]
fn concurrent_push_pop() {
    // One producer, one consumer racing on a capacity-2 queue.
    loom::model(|| {
        let q = Arc::new(MpmcQueue::new(2));
        let qp = Arc::clone(&q);
        let p = thread::spawn(move || {
            qp.push(7).unwrap();
        });
        let qc = Arc::clone(&q);
        let c = thread::spawn(move || pop_with_yields(&qc, 4));
        p.join().unwrap();
        assert_eq!(c.join().unwrap(), Some(7));
    });
}
