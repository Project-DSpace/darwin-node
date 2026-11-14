//! Inbound requests waiting for a handler slot, served by source in turn.
//!
//! The node answers requests on the chunk topic with at most 64 handlers at
//! once, and requests on the listings topic with at most the listing
//! service's own 16. Before ds.23 every message that arrived became a task at
//! once and then waited for a slot inside it, so the tasks waiting, each
//! holding its message, had no bound, and the slots went first come, first
//! served: one source sending faster than the node answered could hold all of
//! them and grow the wait of everyone else, and the node's memory with it.
//!
//! Now a message waits as plain data in a [`RequestQueue`] and a handler task
//! is started only once a slot is held ([`serve_in_turn`]). The queue keeps
//! one first-in first-out list per source and serves the sources that have
//! something waiting in turn, one message each. No source is capped: a source
//! alone on a node (the gateway's usual case) still gets every slot. A source
//! is an address, not a peer id ([`Turn`]), because peer ids cost nothing to
//! make. The queue's total is bounded by [`MAX_QUEUED_REQUESTS`]; when it is
//! full the oldest message of the source with the most waiting is dropped
//! unanswered, so a source with one message waiting keeps it while another
//! has more.

use crate::logging::warn;
use parking_lot::Mutex;
use saorsa_core::identity::PeerId;
use saorsa_core::MultiAddr;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// The most inbound requests one [`RequestQueue`] holds waiting for a
/// handler slot, from all sources together.
///
/// The value, 1,000, is saorsa-core's `DEFAULT_EVENT_CHANNEL_CAPACITY` (in
/// `vendor/saorsa-core/src/lib.rs`, private to that crate): the number of
/// events the node's router can already fall behind by before it skips them
/// (`next_routing_event`). So a queue never holds more waiting messages than
/// the event stream in front of it already can, and that stream's lag rule
/// stays the outer bound. `tests/fork_pins.rs` checks that the two still
/// agree. The node has two queues of this bound, chunk and listings, so in
/// the worst case 2,000 messages of the 5 MiB wire maximum, about 10 GiB,
/// wait for both together, beside the event buffer itself; before ds.23
/// the waiting requests had no bound at all.
///
/// A message that arrives when the queue is full is kept, and the oldest
/// message of the source with the most waiting is dropped instead (that
/// source's own, if it then has the most). Of the choices that keep the
/// bound, this loses least for honest sources: a source with fewer waiting
/// never loses a message to one with more, and the message dropped is the one
/// most likely to be past its sender's timeout already. A dropped request
/// gets no answer, as requests the router skips after falling behind get
/// none; its sender times out and may ask again.
pub const MAX_QUEUED_REQUESTS: usize = 1_000;

/// Whose turn a waiting request counts in: the address that delivered it
/// (an IPv4 address, or an IPv6 /64), or the sender's peer id when no
/// transport address came with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Turn {
    /// The source address, as [`address_bucket`] groups it.
    Address(IpAddr),
    /// The sender's peer id, for a message with no transport address.
    Peer(PeerId),
}

impl Turn {
    /// The turn of a message `source` sent, delivered from `transport_source`.
    #[must_use]
    pub fn of(source: &PeerId, transport_source: Option<&MultiAddr>) -> Self {
        transport_source
            .and_then(MultiAddr::ip)
            .map_or(Self::Peer(*source), |ip| Self::Address(address_bucket(ip)))
    }
}

/// The address group a source counts in: the IPv4 address (an IPv4-mapped
/// IPv6 address counts as its IPv4 form), or the /64 of an IPv6 address. The
/// browser listener counts its sources by the same rule.
#[cfg(feature = "webrtc-direct")]
fn address_bucket(ip: IpAddr) -> IpAddr {
    saorsa_transport::webrtc::source_ip_bucket(ip)
}

/// The address group a source counts in, as the browser listener's
/// `source_ip_bucket` (built only with the `webrtc-direct` feature) groups it.
#[cfg(not(feature = "webrtc-direct"))]
fn address_bucket(ip: IpAddr) -> IpAddr {
    address_bucket_rule(ip)
}

/// The same rule as saorsa-transport's `source_ip_bucket`, for builds without
/// the `webrtc-direct` feature; a test checks the two agree.
#[cfg(any(test, not(feature = "webrtc-direct")))]
fn address_bucket_rule(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V4(ip) => IpAddr::V4(ip),
        IpAddr::V6(ip) => {
            let s = ip.segments();
            IpAddr::V6(std::net::Ipv6Addr::new(s[0], s[1], s[2], s[3], 0, 0, 0, 0))
        }
    }
}

/// One inbound request waiting for a handler slot.
#[derive(Debug)]
pub struct QueuedRequest {
    /// The authenticated sender.
    pub source: PeerId,
    /// The message as it arrived.
    pub data: Vec<u8>,
    /// When the router took it, so a handler's queue wait includes this.
    pub received_at: Instant,
}

#[derive(Debug, Default)]
struct Waiting {
    /// Each source's waiting requests, oldest first. Never holds an empty
    /// list.
    by_turn: HashMap<Turn, VecDeque<QueuedRequest>>,
    /// The sources with something waiting, each once, in the order they are
    /// served.
    ring: VecDeque<Turn>,
    /// All requests waiting.
    len: usize,
    /// Requests dropped because the queue was full, since it was made.
    dropped: u64,
}

/// Inbound requests of one topic waiting for a handler slot, served by
/// source in turn. [`Self::push`] never waits; one consumer waits with
/// [`Self::wait_for_request`] and takes them with [`Self::try_next`].
#[derive(Debug)]
pub struct RequestQueue {
    /// The topic, for the log.
    topic: &'static str,
    bound: usize,
    waiting: Mutex<Waiting>,
    ready: Notify,
}

impl RequestQueue {
    /// An empty queue for `topic` that holds at most `bound` requests
    /// ([`MAX_QUEUED_REQUESTS`] in a running node).
    #[must_use]
    pub fn new(topic: &'static str, bound: usize) -> Self {
        Self {
            topic,
            bound,
            waiting: Mutex::new(Waiting::default()),
            ready: Notify::new(),
        }
    }

    /// Add `request` to its source's list. When that makes more than the
    /// bound waiting, the oldest request of the source with the most waiting
    /// is dropped (ties go to the source whose oldest request is oldest) and
    /// returned. Logs a warning on the first drop and then each time the count
    /// of drops doubles.
    pub fn push(&self, turn: Turn, request: QueuedRequest) -> Option<QueuedRequest> {
        let (dropped, total) = {
            let mut waiting = self.waiting.lock();
            let waiting = &mut *waiting;
            let list = waiting.by_turn.entry(turn).or_default();
            if list.is_empty() {
                waiting.ring.push_back(turn);
            }
            list.push_back(request);
            waiting.len += 1;
            if waiting.len <= self.bound {
                (None, waiting.dropped)
            } else {
                let fullest = waiting
                    .by_turn
                    .iter()
                    .max_by(|(_, a), (_, b)| {
                        a.len().cmp(&b.len()).then_with(|| {
                            // The older front first: reversed, as max_by
                            // keeps the greatest.
                            let (a, b) = (a.front(), b.front());
                            b.map(|r| r.received_at).cmp(&a.map(|r| r.received_at))
                        })
                    })
                    .map(|(turn, _)| *turn);
                let dropped = fullest.and_then(|fullest| {
                    let list = waiting.by_turn.get_mut(&fullest)?;
                    let dropped = list.pop_front();
                    if list.is_empty() {
                        waiting.by_turn.remove(&fullest);
                        waiting.ring.retain(|turn| *turn != fullest);
                    }
                    dropped
                });
                if dropped.is_some() {
                    waiting.len -= 1;
                    waiting.dropped += 1;
                }
                (dropped, waiting.dropped)
            }
        };
        self.ready.notify_one();
        if dropped.is_some() && total.is_power_of_two() {
            warn!(
                "{} requests waiting for a handler reached the queue's bound of {}; \
                 {total} dropped unanswered so far, each the oldest of the source with \
                 the most waiting",
                self.topic, self.bound
            );
        }
        dropped
    }

    /// The oldest request of the next source in turn, which then moves to
    /// the back of the turns; `None` when nothing waits.
    pub fn try_next(&self) -> Option<QueuedRequest> {
        let mut waiting = self.waiting.lock();
        let waiting = &mut *waiting;
        let turn = waiting.ring.pop_front()?;
        let list = waiting.by_turn.get_mut(&turn)?;
        let request = list.pop_front();
        if list.is_empty() {
            waiting.by_turn.remove(&turn);
        } else {
            waiting.ring.push_back(turn);
        }
        if request.is_some() {
            waiting.len -= 1;
        }
        request
    }

    /// Wait until at least one request is waiting.
    pub async fn wait_for_request(&self) {
        while self.is_empty() {
            // A push between the check and here leaves a stored
            // notification, so this returns at once.
            self.ready.notified().await;
        }
    }

    /// How many requests are waiting.
    #[must_use]
    pub fn len(&self) -> usize {
        self.waiting.lock().len
    }

    /// Whether nothing is waiting.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many requests have been dropped because the queue was full.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.waiting.lock().dropped
    }
}

/// Serve `queue`'s requests until `stopping` is cancelled: once a request is
/// waiting, take a slot from `slots`, then the next request in turn, then
/// start `handle`'s work for it on `handlers`, holding the slot until that
/// work ends. So no more handler tasks exist than `slots` has permits, no
/// slot is held while nothing waits, and the source served is the one whose
/// turn it is when a slot frees. Requests still waiting when the node stops
/// are dropped without starting.
pub async fn serve_in_turn<F, Fut>(
    queue: Arc<RequestQueue>,
    slots: Arc<Semaphore>,
    stopping: CancellationToken,
    handlers: TaskTracker,
    mut handle: F,
) where
    F: FnMut(QueuedRequest) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    loop {
        tokio::select! {
            biased;
            () = stopping.cancelled() => return,
            () = queue.wait_for_request() => {}
        }
        let slot = tokio::select! {
            biased;
            () = stopping.cancelled() => return,
            slot = Arc::clone(&slots).acquire_owned() => match slot {
                Ok(slot) => slot,
                Err(_) => return,
            },
        };
        // Chosen now, when the slot is held, so the turn is the current one.
        // Only this loop takes requests, so one is still waiting.
        let Some(request) = queue.try_next() else {
            continue;
        };
        let work = handle(request);
        let stopping = stopping.clone();
        handlers.spawn(async move {
            let _slot = slot;
            // A handler must not start work once shutdown has begun.
            if stopping.is_cancelled() {
                return;
            }
            work.await;
        });
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn peer(n: u8) -> PeerId {
        PeerId::from_bytes([n; 32])
    }

    fn request(source: u8, tag: u8) -> QueuedRequest {
        QueuedRequest {
            source: peer(source),
            data: vec![tag],
            received_at: Instant::now(),
        }
    }

    fn turn(n: u8) -> Turn {
        Turn::Peer(peer(n))
    }

    /// One busy source, the gateway's case, still gets all 64 slots when
    /// nobody else is waiting.
    #[tokio::test]
    async fn a_source_alone_gets_every_slot() {
        let queue = Arc::new(RequestQueue::new("chunk", MAX_QUEUED_REQUESTS));
        for tag in 0..100 {
            queue.push(turn(1), request(1, tag));
        }
        let slots = Arc::new(Semaphore::new(64));
        let handlers = TaskTracker::new();
        let stopping = CancellationToken::new();
        let release = Arc::new(Notify::new());
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let serving = tokio::spawn(serve_in_turn(
            Arc::clone(&queue),
            Arc::clone(&slots),
            stopping.clone(),
            handlers.clone(),
            {
                let (release, started) = (Arc::clone(&release), Arc::clone(&started));
                move |_request| {
                    let (release, started) = (Arc::clone(&release), Arc::clone(&started));
                    async move {
                        started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        release.notified().await;
                    }
                }
            },
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while started.load(std::sync::atomic::Ordering::SeqCst) < 64 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("one source alone fills every slot");
        assert_eq!(slots.available_permits(), 0);
        assert_eq!(handlers.len(), 64);
        assert_eq!(queue.len(), 36);
        stopping.cancel();
        serving.await.unwrap();
        handlers.close();
        release.notify_waiters();
    }

    /// With source A holding 999 waiting messages and source B one, B's
    /// message starts within one turn of each source waiting ahead of it,
    /// never behind A's backlog: here on the next slot to free when B was
    /// already waiting, and on the second when B arrived just after A's turn
    /// came round (a source joins the turns at the back, so a stream of new
    /// sources can't keep one already waiting from its turn).
    #[test]
    fn waiting_sources_are_served_in_turn() {
        // Both waiting: A's turn, then B's, though A has 998 more.
        let queue = RequestQueue::new("chunk", MAX_QUEUED_REQUESTS);
        for _ in 0..999 {
            queue.push(turn(1), request(1, 0));
        }
        queue.push(turn(2), request(2, 7));
        assert_eq!(queue.len(), 1_000);
        assert_eq!(queue.try_next().unwrap().source, peer(1));
        let next = queue.try_next().unwrap();
        assert_eq!((next.source, next.data), (peer(2), vec![7]));
        assert_eq!(queue.try_next().unwrap().source, peer(1));

        // B arrives after A's turn came round: one more of A's, then B.
        let queue = RequestQueue::new("chunk", MAX_QUEUED_REQUESTS);
        for _ in 0..999 {
            queue.push(turn(1), request(1, 0));
        }
        assert_eq!(queue.try_next().unwrap().source, peer(1));
        queue.push(turn(2), request(2, 7));
        assert_eq!(queue.try_next().unwrap().source, peer(1));
        let next = queue.try_next().unwrap();
        assert_eq!((next.source, next.data), (peer(2), vec![7]));
        assert_eq!(queue.len(), 997);
    }

    /// Each source's requests are answered in the order they came.
    #[test]
    fn a_sources_own_messages_keep_their_order() {
        let queue = RequestQueue::new("chunk", MAX_QUEUED_REQUESTS);
        for tag in 0..5 {
            queue.push(turn(1), request(1, tag));
            queue.push(turn(2), request(2, 10 + tag));
        }
        queue.push(turn(3), request(3, 20));
        let mut seen: HashMap<PeerId, Vec<u8>> = HashMap::new();
        let mut order = Vec::new();
        while let Some(next) = queue.try_next() {
            order.push(next.source);
            seen.entry(next.source).or_default().extend(next.data);
        }
        assert_eq!(seen[&peer(1)], vec![0, 1, 2, 3, 4]);
        assert_eq!(seen[&peer(2)], vec![10, 11, 12, 13, 14]);
        assert_eq!(seen[&peer(3)], vec![20]);
        // In turn: 1, 2, 3, then 1 and 2 alternately.
        assert_eq!(&order[..3], &[peer(1), peer(2), peer(3)]);
        assert_eq!(&order[3..5], &[peer(1), peer(2)]);
        assert!(queue.is_empty());
    }

    /// The queue never holds more than its bound, however many arrive.
    #[test]
    fn the_queue_never_holds_more_than_its_bound() {
        let queue = RequestQueue::new("chunk", MAX_QUEUED_REQUESTS);
        for n in 0..3 * MAX_QUEUED_REQUESTS {
            let source = u8::try_from(n % 7).unwrap();
            queue.push(turn(source), request(source, 0));
            assert!(queue.len() <= MAX_QUEUED_REQUESTS);
        }
        assert_eq!(queue.len(), MAX_QUEUED_REQUESTS);
        assert_eq!(queue.dropped(), 2 * MAX_QUEUED_REQUESTS as u64);
    }

    /// A full queue drops the oldest message of the source with the most
    /// waiting, and a source with one message waiting keeps it.
    #[test]
    fn a_full_queue_drops_from_the_source_with_the_most_waiting() {
        let queue = RequestQueue::new("chunk", 4);
        queue.push(turn(2), request(2, 20));
        for tag in 0..3 {
            queue.push(turn(1), request(1, tag));
        }
        assert_eq!(queue.dropped(), 0);
        // Full; one more from a third source: A's oldest goes.
        let dropped = queue.push(turn(3), request(3, 30)).expect("one dropped");
        assert_eq!((dropped.source, dropped.data), (peer(1), vec![0]));
        assert_eq!(queue.len(), 4);
        // The flooding source's own new message pushes out its own oldest.
        let dropped = queue.push(turn(1), request(1, 3)).expect("one dropped");
        assert_eq!((dropped.source, dropped.data), (peer(1), vec![1]));
        let mut left = Vec::new();
        while let Some(next) = queue.try_next() {
            left.push((next.source, next.data[0]));
        }
        // B and C kept their one message each.
        assert!(left.contains(&(peer(2), 20)));
        assert!(left.contains(&(peer(3), 30)));
        assert_eq!(queue.dropped(), 2);

        // All sources with one each: the oldest message is the one dropped.
        let queue = RequestQueue::new("chunk", 2);
        queue.push(turn(1), request(1, 1));
        std::thread::sleep(Duration::from_millis(2));
        queue.push(turn(2), request(2, 2));
        std::thread::sleep(Duration::from_millis(2));
        let dropped = queue.push(turn(3), request(3, 3)).expect("one dropped");
        assert_eq!(dropped.source, peer(1));
    }

    /// Two peer ids from one IPv4 address, or one IPv6 /64, share a turn; a
    /// message with no transport address counts by its peer id.
    #[test]
    fn sources_are_counted_by_address() {
        let address = |s: &str| s.parse::<MultiAddr>().unwrap();
        let v4a = address("/ip4/203.0.113.7/udp/9000/quic");
        let v4b = address("/ip4/203.0.113.7/udp/9555/quic");
        let v4other = address("/ip4/203.0.113.8/udp/9000/quic");
        assert_eq!(
            Turn::of(&peer(1), Some(&v4a)),
            Turn::of(&peer(2), Some(&v4b))
        );
        assert_ne!(
            Turn::of(&peer(1), Some(&v4a)),
            Turn::of(&peer(1), Some(&v4other))
        );
        let v6a = address("/ip6/2001:db8:1:2:aaaa::1/udp/9000/quic");
        let v6b = address("/ip6/2001:db8:1:2:bbbb::9/udp/9001/quic");
        let v6other = address("/ip6/2001:db8:1:3::1/udp/9000/quic");
        assert_eq!(
            Turn::of(&peer(1), Some(&v6a)),
            Turn::of(&peer(2), Some(&v6b))
        );
        assert_ne!(
            Turn::of(&peer(1), Some(&v6a)),
            Turn::of(&peer(2), Some(&v6other))
        );
        let mapped = address("/ip6/::ffff:203.0.113.7/udp/9000/quic");
        assert_eq!(
            Turn::of(&peer(3), Some(&mapped)),
            Turn::of(&peer(1), Some(&v4a))
        );
        assert_eq!(Turn::of(&peer(4), None), Turn::Peer(peer(4)));
        assert_ne!(Turn::of(&peer(4), None), Turn::of(&peer(5), None));

        // Sharing a turn: the two peers on one address are served as one
        // source, alternately with a third.
        let queue = RequestQueue::new("chunk", MAX_QUEUED_REQUESTS);
        queue.push(Turn::of(&peer(1), Some(&v4a)), request(1, 1));
        queue.push(Turn::of(&peer(2), Some(&v4b)), request(2, 2));
        queue.push(Turn::of(&peer(3), Some(&v4other)), request(3, 3));
        let order: Vec<PeerId> = std::iter::from_fn(|| queue.try_next())
            .map(|r| r.source)
            .collect();
        assert_eq!(order, vec![peer(1), peer(3), peer(2)]);
    }

    /// The copy of the address rule agrees with saorsa-transport's
    /// `source_ip_bucket`, which the browser listener uses.
    #[cfg(feature = "webrtc-direct")]
    #[test]
    fn the_address_rule_is_the_browser_listeners() {
        for ip in [
            "203.0.113.7",
            "0.0.0.0",
            "::ffff:203.0.113.7",
            "2001:db8:1:2:aaaa:bbbb:cccc:dddd",
            "::1",
            "fe80::1",
            "2001:db8::",
        ] {
            let ip: IpAddr = ip.parse().unwrap();
            assert_eq!(address_bucket_rule(ip), address_bucket(ip), "{ip}");
        }
    }
}
