//! Rate limits on unpaid storage-protocol requests, per peer and per IP.
//!
//! This fork's addition: upstream limits only new connections per IP and the
//! browser listener. Quote requests and PUTs that end unpaid cost a node a
//! signature or chain RPC calls each, and nothing paid for them, so one source
//! could keep a node busy for free.
//!
//! The buckets are the browser listener's (`RequestRateBucket` in
//! `src/web_rtc.rs`): constant-space, holding one second of their rate. The
//! key maps are bounded, and a new key evicts the least recently seen one when
//! a map is full; a bucket left alone for a second is full again, so evicting
//! it forgives nothing. IPs are keyed by [`source_ip_bucket`]: an IPv4 address,
//! or an IPv6 /64.

use crate::config::RequestRateLimitConfig;
use parking_lot::Mutex;
use saorsa_transport::webrtc::source_ip_bucket;
use std::collections::HashMap;
use std::hash::Hash;
use std::net::IpAddr;
use std::time::Instant;

/// One token, in the bucket's sub-token units (nanosecond resolution).
const TOKEN: u128 = 1_000_000_000;

/// Most peers, and separately most IPs, each limiter tracks at once.
///
/// saorsa-core's default connection limit (10,000) times the browser
/// listener's `TRACKED_SOURCE_MULTIPLIER` (4): enough for every connection a
/// node can hold, with room for churn. An entry is under 200 bytes, so a full
/// map stays under 8 MB. Derived from those upstream values, not chosen by
/// anyone: awaits Dan's confirmation.
const MAX_TRACKED_KEYS: usize = 40_000;

/// Fixed-capacity token bucket with a one-second burst allowance, as in the
/// browser listener.
struct RateBucket {
    rate_per_second: u128,
    token_units: u128,
    last_refill: Instant,
}

impl RateBucket {
    fn new(rate_per_second: usize, now: Instant) -> Self {
        let rate_per_second = rate_per_second as u128;
        Self {
            rate_per_second,
            token_units: rate_per_second.saturating_mul(TOKEN),
            last_refill: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.last_refill = now;
        let capacity = self.rate_per_second.saturating_mul(TOKEN);
        let refill = elapsed.as_nanos().saturating_mul(self.rate_per_second);
        self.token_units = self.token_units.saturating_add(refill).min(capacity);
    }

    fn has_token(&mut self, now: Instant) -> bool {
        self.refill(now);
        self.token_units >= TOKEN
    }

    /// Spend a token, or as much of one as is left.
    fn spend(&mut self, now: Instant) {
        self.refill(now);
        self.token_units = self.token_units.saturating_sub(TOKEN);
    }
}

struct Entry {
    bucket: RateBucket,
    last_seen: Instant,
}

/// Buckets by key, at one rate, in a bounded map.
struct KeyedBuckets<K> {
    rate_per_second: usize,
    entries: HashMap<K, Entry>,
}

impl<K: Eq + Hash + Clone> KeyedBuckets<K> {
    fn new(rate_per_second: usize) -> Self {
        Self {
            rate_per_second,
            entries: HashMap::new(),
        }
    }

    /// Whether `key` has a token now. An unknown key has a full bucket, and
    /// is not added.
    fn has_token(&mut self, key: &K, now: Instant) -> bool {
        match self.entries.get_mut(key) {
            Some(entry) => {
                entry.last_seen = now;
                entry.bucket.has_token(now)
            }
            None => self.rate_per_second > 0,
        }
    }

    fn spend(&mut self, key: &K, now: Instant) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.last_seen = now;
            entry.bucket.spend(now);
            return;
        }
        if self.entries.len() >= MAX_TRACKED_KEYS {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_seen)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                self.entries.remove(&oldest);
            }
        }
        let mut bucket = RateBucket::new(self.rate_per_second, now);
        bucket.spend(now);
        self.entries.insert(
            key.clone(),
            Entry {
                bucket,
                last_seen: now,
            },
        );
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// A per-peer and a per-IP limit on one kind of request. Each method holds
/// the lock throughout, so checking and spending are one step.
struct PeerAndIpLimit {
    buckets: Mutex<(KeyedBuckets<String>, KeyedBuckets<IpAddr>)>,
}

#[allow(clippy::significant_drop_tightening)]
impl PeerAndIpLimit {
    fn new(per_peer: usize, per_ip: usize) -> Self {
        Self {
            buckets: Mutex::new((KeyedBuckets::new(per_peer), KeyedBuckets::new(per_ip))),
        }
    }

    /// Whether both the peer's and the IP's bucket have a token.
    fn has_token(&self, peer: &str, ip: Option<IpAddr>, now: Instant) -> bool {
        let mut guard = self.buckets.lock();
        let (peers, ips) = &mut *guard;
        let peer_ok = peers.has_token(&peer.to_owned(), now);
        let ip_ok = ip.is_none_or(|ip| ips.has_token(&source_ip_bucket(ip), now));
        peer_ok && ip_ok
    }

    fn spend(&self, peer: &str, ip: Option<IpAddr>, now: Instant) {
        let mut guard = self.buckets.lock();
        let (peers, ips) = &mut *guard;
        peers.spend(&peer.to_owned(), now);
        if let Some(ip) = ip {
            ips.spend(&source_ip_bucket(ip), now);
        }
    }

    /// Spend a token from both buckets if both have one.
    fn try_take(&self, peer: &str, ip: Option<IpAddr>, now: Instant) -> bool {
        let mut guard = self.buckets.lock();
        let (peers, ips) = &mut *guard;
        let peer = peer.to_owned();
        let ip = ip.map(source_ip_bucket);
        let allowed = peers.has_token(&peer, now) && ip.is_none_or(|ip| ips.has_token(&ip, now));
        if allowed {
            peers.spend(&peer, now);
            if let Some(ip) = ip {
                ips.spend(&ip, now);
            }
        }
        allowed
    }
}

/// The node's limits on unpaid requests over the native storage protocol.
pub struct RequestLimiter {
    quotes: PeerAndIpLimit,
    unpaid_puts: PeerAndIpLimit,
}

impl RequestLimiter {
    pub fn new(config: &RequestRateLimitConfig) -> Self {
        Self {
            quotes: PeerAndIpLimit::new(
                config.quote_requests_per_second_per_peer,
                config.quote_requests_per_second_per_ip,
            ),
            unpaid_puts: PeerAndIpLimit::new(
                config.unpaid_puts_per_second_per_peer,
                config.unpaid_puts_per_second_per_ip,
            ),
        }
    }

    /// Admit a quote request, spending a token, or refuse it.
    pub fn allow_quote(&self, peer: &str, ip: Option<IpAddr>) -> bool {
        self.quotes.try_take(peer, ip, Instant::now())
    }

    /// Whether a PUT may proceed: refused only while the peer's or the IP's
    /// unpaid budget is spent. Spends nothing, so paid PUTs cost no budget.
    pub fn allow_put(&self, peer: &str, ip: Option<IpAddr>) -> bool {
        self.unpaid_puts.has_token(peer, ip, Instant::now())
    }

    /// Charge a PUT that ended unpaid to its peer and IP.
    pub fn charge_unpaid_put(&self, peer: &str, ip: Option<IpAddr>) {
        self.unpaid_puts.spend(peer, ip, Instant::now());
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::time::Duration;

    fn limits(rate: usize) -> RequestRateLimitConfig {
        RequestRateLimitConfig {
            quote_requests_per_second_per_peer: rate,
            quote_requests_per_second_per_ip: rate,
            unpaid_puts_per_second_per_peer: rate,
            unpaid_puts_per_second_per_ip: rate,
        }
    }

    #[test]
    fn bucket_allows_one_second_of_burst_then_refills() {
        let start = Instant::now();
        let mut bucket = RateBucket::new(4, start);
        for _ in 0..4 {
            assert!(bucket.has_token(start));
            bucket.spend(start);
        }
        assert!(!bucket.has_token(start));
        assert!(bucket.has_token(start + Duration::from_millis(250)));
    }

    #[test]
    fn quote_flood_from_one_peer_is_limited_and_others_are_not() {
        let limiter = RequestLimiter::new(&limits(8));
        let flooder_ip = Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)));
        let allowed = (0..100)
            .filter(|_| limiter.allow_quote("flooder", flooder_ip))
            .count();
        assert!(allowed <= 9, "a one-second burst at most, got {allowed}");
        let other_ip = Some(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 2)));
        assert!(limiter.allow_quote("other", other_ip));
    }

    #[test]
    fn many_peers_behind_one_ip_share_its_limit() {
        let limiter = RequestLimiter::new(&RequestRateLimitConfig {
            quote_requests_per_second_per_peer: 1_000,
            quote_requests_per_second_per_ip: 8,
            ..limits(8)
        });
        let ip = Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)));
        let allowed = (0..100)
            .filter(|i| limiter.allow_quote(&format!("sybil-{i}"), ip))
            .count();
        assert!(allowed <= 9, "got {allowed}");
    }

    #[test]
    fn ipv6_addresses_in_one_64_share_a_bucket() {
        let limiter = RequestLimiter::new(&limits(4));
        let allowed = (0u16..100)
            .filter(|i| {
                let ip = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 1, 2, 0, 0, 0, *i));
                limiter.allow_quote(&format!("peer-{i}"), Some(ip))
            })
            .count();
        assert!(allowed <= 5, "got {allowed}");
        let elsewhere = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 1, 3, 0, 0, 0, 1));
        assert!(limiter.allow_quote("peer-elsewhere", Some(elsewhere)));
    }

    #[test]
    fn unpaid_puts_are_refused_only_once_charged() {
        let limiter = RequestLimiter::new(&limits(4));
        let ip = Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)));
        // Asking spends nothing: any number of (paid) PUTs pass.
        for _ in 0..100 {
            assert!(limiter.allow_put("payer", ip));
        }
        for _ in 0..4 {
            limiter.charge_unpaid_put("payer", ip);
        }
        assert!(!limiter.allow_put("payer", ip));
        assert!(limiter.allow_put("someone-else", None));
    }

    #[test]
    fn a_zero_rate_refuses_everything() {
        let limiter = RequestLimiter::new(&limits(0));
        assert!(!limiter.allow_quote("peer", None));
        assert!(!limiter.allow_put("peer", None));
    }

    #[test]
    fn key_maps_stay_bounded() {
        let mut buckets = KeyedBuckets::<u32>::new(1);
        let now = Instant::now();
        for key in 0..u32::try_from(MAX_TRACKED_KEYS).unwrap() + 10 {
            buckets.spend(&key, now + Duration::from_nanos(u64::from(key)));
        }
        assert_eq!(buckets.len(), MAX_TRACKED_KEYS);
        // The oldest keys went first.
        assert!(!buckets.entries.contains_key(&0));
        assert!(buckets
            .entries
            .contains_key(&(u32::try_from(MAX_TRACKED_KEYS).unwrap() + 9)));
    }
}
