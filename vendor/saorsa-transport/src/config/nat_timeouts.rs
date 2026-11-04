// Copyright 2024 Saorsa Labs Ltd.
//
// This software is licensed under the MIT license <LICENSE-MIT or
// https://opensource.org/licenses/MIT> or the Apache License, Version 2.0
// <LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0>, at your
// option. This file may not be copied, modified, or distributed except
// according to those terms.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//! Configurable timeouts for NAT traversal operations

use crate::Duration;
use serde::{Deserialize, Serialize};

/// Configuration for NAT traversal timeouts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatTraversalTimeouts {
    /// Timeout for hole punching coordination
    pub coordination_timeout: Duration,

    /// Overall timeout for establishing a connection through NAT
    pub connection_establishment_timeout: Duration,

    /// Timeout for individual probe attempts
    pub probe_timeout: Duration,

    /// Interval between retry attempts
    pub retry_interval: Duration,

    /// Timeout for bootstrap node queries
    pub bootstrap_query_timeout: Duration,

    /// Time to wait for path migration to complete
    pub migration_timeout: Duration,

    /// Time to wait for session state transitions
    pub session_timeout: Duration,
}

/// QUIC keep-alive interval for connections this node dials.
///
/// The dialling side sets the cadence for the connection. A keep-alive is
/// ack-eliciting, so the peer answers it, both idle timers reset, and each side
/// puts a packet on the wire once per interval — which is also what refreshes a
/// NAT mapping for that five-tuple. The accepting side carries only
/// [`ACCEPT_KEEP_ALIVE_BACKSTOP`], which this cadence normally holds off.
///
/// 10 s rather than the 5 s + 2 s pair it replaces. Because any traffic resets
/// both timers, the old arrangement ran at whichever interval was shorter, so
/// the effective cadence was the accepting side's 2 s and the interval is the
/// only real lever on idle egress. Measured on a 25-node testnet, moving to
/// 10 s takes idle egress from 29.8 to 6.1 B/s per connection endpoint.
///
/// The constraint on going further is NAT mapping lifetime, not the QUIC idle
/// timeout: RFC 4787 requires two minutes but 20-30 s middleboxes are reported
/// in the field, and if a mapping lapses the keep-alive is dropped before it
/// can provoke the ACK that would have refreshed it. 10 s is deliberately below
/// RFC 8085's 15 s floor for general-Internet keep-alives, for that reason.
pub(crate) const DIAL_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);

/// Longest the accepting side will stay silent on an otherwise idle connection.
///
/// The accepting side does not drive the cadence — the dialling peer's 10 s
/// keep-alive does, and every packet received re-arms this timer, so on a
/// healthy connection it has no room to fire. It exists to bound how long a
/// connection can be silent when the peer stops being heard.
///
/// Without it the whole connection depends on one side. A QUIC connection
/// closes silently after `QUIC_MAX_IDLE_TIMEOUT_MS` with nothing received, and
/// with only the dialling side probing, a network stall is survivable for far
/// less than that: the connection is already part-way into its silence when the
/// stall begins, and afterwards recovery waits on the dialling side's probe
/// timer, which backs off exponentially. Measured against a recurring outage on
/// a harness, at the outage lengths sampled, the survivable stall was ~28 s with
/// both sides on a timer and ~16 s with only the dialling side; restoring a
/// timer here took it back to ~28 s across that range.
///
/// The value is bounded from both directions and 25 s sits between them.
///
/// Too low and it fires in ordinary operation and starts charging every idle
/// connection. The real gap between received packets on a healthy connection is
/// the dialling cadence plus a round trip, and more when a ping is lost and
/// waits on a probe timer, so the floor is well above 10 s. Restoring the
/// outage margin does not need a small value: 5 s was measured alongside 25 s
/// and behaved identically, because what restores the margin is this side
/// having a timer at all rather than how often it runs.
///
/// Too high and it costs twice over.
///
/// It crowds [`QUIC_MAX_IDLE_TIMEOUT_MS`]. `handle_timeout` services timers in
/// `Timer::VALUES` order and `Idle` is ordered before `KeepAlive`, so if the
/// connection driver is not polled for the gap between the two deadlines, the
/// idle timeout is processed first and the connection closes without the
/// backstop ever being sent. 5 s is the nominal difference between the two
/// constants and the most that gap can be; the real separation can be smaller,
/// because sending re-arms the keep-alive timer while the idle timer is only
/// re-armed by the first ack-eliciting send after a receive. That is narrower
/// slack than the arrangement this replaces and worth knowing, because driver
/// stalls are a load phenomenon and load is what this change is about.
///
/// It also holds a dead peer for longer. The backstop PING is ack-eliciting, so
/// the first one sent after the peer goes quiet re-arms the idle timer: a peer
/// that disappears is retained for roughly the backstop plus the idle timeout,
/// about 55 s, against 30 s with no timer on this side and about 32 s under the
/// 2 s arrangement that preceded it. Subsequent probes do not extend it
/// further, since only the first ack-eliciting send after a receive resets the
/// idle timer. This is inherent to having a backstop at all and a lower value
/// would shorten it.
///
/// 25 s therefore favours not reintroducing the idle-egress cost, which is the
/// whole point of the arrangement it is protecting, and pays for that on the
/// other two. If the fleet shows either — connections closing without the
/// backstop under load, or dead-peer retention pressing on connection capacity
/// — a lower value inside the measured range is the lighter response than
/// removing the backstop.
pub(crate) const ACCEPT_KEEP_ALIVE_BACKSTOP: Duration = Duration::from_secs(25);

/// `max_idle_timeout` applied to every QUIC connection this crate creates.
///
/// Named here so the two endpoint configuration sites cannot drift apart, and
/// so the keep-alive above has something to be read against.
///
/// Crate-private on purpose: these are internal wiring values, not API. The
/// integration test asserts the literals it expects rather than importing
/// these, so a wrong constant fails the test instead of moving it.
pub(crate) const QUIC_MAX_IDLE_TIMEOUT_MS: u32 = 30_000;

impl Default for NatTraversalTimeouts {
    fn default() -> Self {
        Self {
            coordination_timeout: Duration::from_secs(10),
            connection_establishment_timeout: Duration::from_secs(30),
            probe_timeout: Duration::from_secs(5),
            retry_interval: Duration::from_secs(1),
            bootstrap_query_timeout: Duration::from_secs(5),
            migration_timeout: Duration::from_secs(60),
            session_timeout: Duration::from_secs(5),
        }
    }
}

impl NatTraversalTimeouts {
    /// Create timeouts optimized for fast local networks
    pub fn fast() -> Self {
        Self {
            coordination_timeout: Duration::from_secs(5),
            connection_establishment_timeout: Duration::from_secs(15),
            probe_timeout: Duration::from_secs(2),
            retry_interval: Duration::from_millis(500),
            bootstrap_query_timeout: Duration::from_secs(2),
            migration_timeout: Duration::from_secs(30),
            session_timeout: Duration::from_secs(2),
        }
    }

    /// Create timeouts optimized for slow or unreliable networks
    pub fn conservative() -> Self {
        Self {
            coordination_timeout: Duration::from_secs(20),
            connection_establishment_timeout: Duration::from_secs(60),
            probe_timeout: Duration::from_secs(10),
            retry_interval: Duration::from_secs(2),
            bootstrap_query_timeout: Duration::from_secs(10),
            migration_timeout: Duration::from_secs(120),
            session_timeout: Duration::from_secs(10),
        }
    }
}

/// Configuration for discovery operation timeouts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryTimeouts {
    /// Total timeout for the entire discovery process
    pub total_timeout: Duration,

    /// Timeout for scanning local network interfaces
    pub local_scan_timeout: Duration,

    /// Time to cache network interface information
    pub interface_cache_ttl: Duration,

    /// Time to cache server reflexive addresses
    pub server_reflexive_cache_ttl: Duration,

    /// Interval between health checks for bootstrap nodes
    pub health_check_interval: Duration,
}

impl Default for DiscoveryTimeouts {
    fn default() -> Self {
        Self {
            total_timeout: Duration::from_secs(30),
            local_scan_timeout: Duration::from_secs(2),
            interface_cache_ttl: Duration::from_secs(60),
            server_reflexive_cache_ttl: Duration::from_secs(300),
            health_check_interval: Duration::from_secs(30),
        }
    }
}

/// Configuration for relay-related timeouts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayTimeouts {
    /// Timeout for relay request operations
    pub request_timeout: Duration,

    /// Interval between retry attempts
    pub retry_interval: Duration,

    /// Time window for rate limiting
    pub rate_limit_window: Duration,
}

impl Default for RelayTimeouts {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(30),
            retry_interval: Duration::from_millis(500),
            rate_limit_window: Duration::from_secs(60),
        }
    }
}

/// Master timeout configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimeoutConfig {
    /// NAT traversal timeouts
    pub nat_traversal: NatTraversalTimeouts,

    /// Discovery timeouts
    pub discovery: DiscoveryTimeouts,

    /// Relay timeouts
    pub relay: RelayTimeouts,
}

impl TimeoutConfig {
    /// Create a configuration optimized for fast networks
    pub fn fast() -> Self {
        Self {
            nat_traversal: NatTraversalTimeouts::fast(),
            discovery: DiscoveryTimeouts::default(),
            relay: RelayTimeouts::default(),
        }
    }

    /// Create a configuration optimized for slow networks
    pub fn conservative() -> Self {
        Self {
            nat_traversal: NatTraversalTimeouts::conservative(),
            discovery: DiscoveryTimeouts::default(),
            relay: RelayTimeouts::default(),
        }
    }
}
