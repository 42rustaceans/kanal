#![cfg(not(loom))]
#![cfg(feature = "async")]

use std::{
    collections::VecDeque,
    future::Future,
    pin::{pin, Pin},
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

use futures::stream::{FusedStream, Stream};

// keeps the thread completing detached signals busy between detaching and
// completing them
struct SlowWaker(Duration);
impl Wake for SlowWaker {
    fn wake(self: Arc<Self>) {
        thread::sleep(self.0)
    }
}

fn poll_once<F: Future>(f: Pin<&mut F>, w: &Waker) -> Poll<F::Output> {
    f.poll(&mut Context::from_waker(w))
}

fn poll_until_ready<F: Future>(mut fut: Pin<&mut F>, w: &Waker) -> F::Output {
    loop {
        if let Poll::Ready(v) = poll_once(fut.as_mut(), w) {
            return v;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

// larger than a pointer, so it is transferred through the waiter's stack slot
type Payload = [u64; 4];

#[test]
fn send_timeout_race_with_detached_signal_reports_success() {
    let (s, r) = kanal::bounded::<Payload>(0);
    let s_a = s.clone();
    let a = thread::spawn(move || {
        let w = Arc::new(SlowWaker(Duration::from_millis(300))).into();
        let mut fut = pin!(s_a.as_async().send([1; 4]));
        assert!(poll_once(fut.as_mut(), &w).is_pending());
        poll_until_ready(fut, &w).unwrap();
    });
    thread::sleep(Duration::from_millis(50));
    let b = thread::spawn(move || {
        s.send_timeout([2; 4], Duration::from_millis(100))
            .map_err(|e| e.is_closed())
    });
    thread::sleep(Duration::from_millis(50));

    let mut got = Vec::new();
    assert_eq!(r.drain_into(&mut got).unwrap(), 2);
    a.join().unwrap();
    assert_eq!(b.join().unwrap(), Ok(()));
    assert_eq!(got, vec![[1; 4], [2; 4]]);
}

#[test]
fn recv_timeout_race_with_detached_signal_receives_value() {
    let (s, r) = kanal::bounded::<Payload>(0);
    let r_a = r.clone();
    let a = thread::spawn(move || {
        let w = Arc::new(SlowWaker(Duration::from_millis(300))).into();
        let mut fut = pin!(r_a.as_async().recv());
        assert!(poll_once(fut.as_mut(), &w).is_pending());
        poll_until_ready(fut, &w).unwrap()
    });
    thread::sleep(Duration::from_millis(50));
    let b = thread::spawn(move || {
        r.recv_timeout(Duration::from_millis(100))
            .map_err(|e| e.is_closed())
    });
    thread::sleep(Duration::from_millis(50));

    let mut batch = VecDeque::from(vec![[1; 4], [2; 4]]);
    s.send_many(&mut batch).unwrap();
    assert_eq!(a.join().unwrap(), [1; 4]);
    assert_eq!(b.join().unwrap(), Ok([2; 4]));
}

#[test]
fn dropped_receive_future_hands_value_to_waiting_receiver() {
    for cap in [0, 4] {
        let (s, r) = kanal::bounded::<u64>(cap);
        let mut fut = Box::pin(r.as_async().recv());
        assert!(poll_once(fut.as_mut(), Waker::noop()).is_pending());
        let r_b = r.clone();
        let b = thread::spawn(move || r_b.recv_timeout(Duration::from_secs(2)));
        thread::sleep(Duration::from_millis(100));

        s.send(42).unwrap();
        drop(fut);
        assert_eq!(b.join().unwrap().ok(), Some(42), "capacity {cap}");
        assert_eq!(r.len(), 0);
    }
}

#[test]
fn stream_is_not_terminated_while_item_pending() {
    for cap in [0, 1] {
        let (s, r) = kanal::bounded_async::<u64>(cap);
        let mut stream = r.stream();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut stream).poll_next(&mut cx).is_pending());
        s.try_send(7).unwrap();
        drop(s);
        assert!(!stream.is_terminated(), "capacity {cap}");
        assert_eq!(
            Pin::new(&mut stream).poll_next(&mut cx),
            Poll::Ready(Some(7))
        );
        assert_eq!(Pin::new(&mut stream).poll_next(&mut cx), Poll::Ready(None));
        assert!(stream.is_terminated());
    }
}

struct PanicWaker;
impl Wake for PanicWaker {
    fn wake(self: Arc<Self>) {
        panic!("waker panics")
    }
}

#[test]
fn panicking_waker_does_not_strand_rest_of_batch() {
    let (s, r) = kanal::bounded::<u64>(0);
    let w = Arc::new(PanicWaker).into();
    let mut fut = Box::pin(s.as_async().send(1));
    assert!(poll_once(fut.as_mut(), &w).is_pending());
    let (tx, rx) = std::sync::mpsc::channel();
    let s2 = s.clone();
    thread::spawn(move || tx.send(s2.send(2).is_ok()));
    thread::sleep(Duration::from_millis(50));

    let mut got = Vec::new();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        r.drain_into(&mut got)
    }));
    assert!(res.is_err());
    assert_eq!(rx.recv_timeout(Duration::from_secs(2)), Ok(true));
    assert_eq!(got, [1, 2]);
}

#[test]
fn drain_future_keeps_delivered_value_once_after_waker_panic() {
    let (s, r) = kanal::bounded_async::<String>(0);
    let mut got = Vec::new();
    let mut d = Box::pin(r.drain_into_blocking(&mut got));
    assert!(poll_once(d.as_mut(), Waker::noop()).is_pending());
    s.as_sync().try_send("first".to_string()).unwrap();
    let w = Arc::new(PanicWaker).into();
    let mut f = Box::pin(s.send("second".to_string()));
    assert!(poll_once(f.as_mut(), &w).is_pending());
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        poll_once(d.as_mut(), Waker::noop())
    }));
    assert!(res.is_err());
    drop(d);
    drop(f);
    assert_eq!(got, ["first", "second"]);
}
