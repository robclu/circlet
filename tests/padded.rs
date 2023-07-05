use circlet::{channel, consumer, producer, PaddedRingbuffer};
use core_affinity::CoreId;
use num_cpus::get_physical;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

#[test]
fn padded_ringbuffer_simple_operations() {
    let buffer = Arc::new(PaddedRingbuffer::<u32>::new(11));
    assert!(buffer.size() == 0 && buffer.empty());

    let (mut producer, mut consumer) = channel(buffer.clone());

    for i in 0..10 {
        producer.push(i as u32);
    }
    assert!(producer.size() == 10 && !producer.empty());
    assert!(consumer.size() == 10 && !consumer.empty());
    assert!(buffer.size() == 10 && !buffer.empty());

    let mut v: u32 = u32::MAX;
    consumer.pop(&mut v);
    assert!(v == 0);
    assert!(producer.size() == 9 && !producer.empty());
    assert!(consumer.size() == 9 && !consumer.empty());

    consumer.pop(&mut v);
    assert!(v == 1);
    assert!(producer.size() == 8 && !producer.empty());
    assert!(consumer.size() == 8 && !consumer.empty());
    assert!(buffer.size() == 8 && !buffer.empty());
}

#[test]
fn padded_try_push_and_pop() {
    let buffer = Arc::new(PaddedRingbuffer::<i32>::new(1));
    let (mut producer, mut consumer) = channel(buffer.clone());

    let mut t: i32 = 0;
    assert!(producer.try_push(1).is_ok());
    assert!(producer.size() == 1 && !producer.empty());
    assert!(consumer.size() == 1 && !consumer.empty());
    assert!(buffer.size() == 1 && !buffer.empty());

    assert!(producer.try_push(2).is_err());
    assert!(producer.size() == 1 && !producer.empty());
    assert!(consumer.size() == 1 && !consumer.empty());
    assert!(buffer.size() == 1 && !buffer.empty());

    assert!(consumer.try_pop(&mut t).is_ok() && t == 1);
    assert!(producer.size() == 0 && producer.empty());
    assert!(consumer.size() == 0 && consumer.empty());
    assert!(buffer.size() == 0 && buffer.empty());

    assert!(consumer.try_pop(&mut t).is_err() && t == 1);
    assert!(producer.size() == 0 && producer.empty());
    assert!(consumer.size() == 0 && consumer.empty());
    assert!(buffer.size() == 0 && buffer.empty());
}

#[test]
fn padded_stress_test() {
    const ELEMENTS: usize = 10_000_000;

    let threads_total = get_physical().max(2);
    let threads_half = (threads_total / 2).max(1);
    let buffer = Arc::new(PaddedRingbuffer::<usize>::new(threads_half as u32));
    let flag = Arc::new(AtomicBool::new(false));
    let sum = Arc::new(AtomicUsize::new(0));

    let mut threads = vec![];
    for i in 0..threads_half {
        let mut producer = producer(buffer.clone());
        let flag = flag.clone();
        threads.push(std::thread::spawn(move || {
            core_affinity::set_for_current(CoreId { id: i });

            while !flag.load(Ordering::Relaxed) {
                std::hint::spin_loop();
            }

            for v in (i..ELEMENTS).step_by(threads_half) {
                producer.push(v);
            }
        }));
    }

    for i in threads_half..threads_total {
        let mut consumer = consumer(buffer.clone());
        let flag = flag.clone();
        let sum = sum.clone();
        threads.push(std::thread::spawn(move || {
            core_affinity::set_for_current(CoreId { id: i });

            while !flag.load(Ordering::Relaxed) {
                std::hint::spin_loop();
            }

            let mut local_sum: usize = 0;
            let mut v: usize = 0;
            let start = i - threads_half;
            for _ in (start..ELEMENTS).step_by(threads_half) {
                consumer.pop(&mut v);
                local_sum += v;
            }
            sum.fetch_add(local_sum, Ordering::SeqCst);
        }));
    }

    flag.store(true, Ordering::Relaxed);
    let start = std::time::Instant::now();

    for t in threads {
        t.join().unwrap();
    }
    let end = std::time::Instant::now();
    let time = (end - start).as_micros();
    println!(
        "Time: {}us {}M/sec on {} threads",
        time,
        ELEMENTS * 1_000_000 / time as usize,
        threads_total
    );

    assert!(sum.load(Ordering::Relaxed) == ELEMENTS * (ELEMENTS - 1) / 2);
}
