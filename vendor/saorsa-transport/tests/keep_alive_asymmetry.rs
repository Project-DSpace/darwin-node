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

//! The dialling side drives the cadence, and the accepting side's backstop
//! stays silent while it does.
//!
//! Two in-process endpoints are connected and left idle for longer than the
//! 30 s `max_idle_timeout`. Everything is read from the dialling side's
//! `ConnectionStats`, which sees both halves of the question: `frame_tx.ping`
//! is what this node sent and `frame_rx.ping` is what the peer sent, so the
//! accepting side's silence is observed on the wire rather than inferred.
//!
//! The accepting side does carry a keep-alive, but only as a backstop on how
//! long it may stay silent, and every packet it receives re-arms that timer.
//! The dialling peer pings well inside the interval, so on this path the
//! backstop stays silent — which is what `frame_rx.ping` being zero here
//! demonstrates. That is the cost argument for the backstop, so this assertion
//! is load-bearing: if it starts counting here, the mechanism relied on to keep
//! the backstop cheap is not working.
//!
//! It is one connection on a lossless, sub-millisecond-RTT path, so it
//! establishes the mechanism and not the fleet result. A real connection sees
//! the cadence plus a round trip between received packets, and more when a ping
//! is lost, so the backstop can legitimately fire on a degraded-but-live path.
//! Per-node idle egress after rollout is what shows the steady-state cost.
//!
//! `frame_tx.ping` is not a keep-alive counter in general — it also counts
//! DPLPMTUD probes, PTO probes and path validation. None of those should fire
//! here after the settle: the MTU search finishes in a few loopback round trips
//! and does not re-probe for 600 s, PTO needs loss, and path validation needs a
//! migration. That is an argument rather than a proof, so the test checks the
//! *spacing* of the PINGs too, which a stray probe would not match.
//!
//! This is loopback: no NAT, no loss, one connection. It shows the mechanism
//! works on an ideal path. It says nothing about NAT mapping expiry, which is
//! what actually bounds how long the interval can be.

#![allow(clippy::expect_used)]

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use saorsa_transport::config::{ClientConfig, ServerConfig, TransportConfig};
use saorsa_transport::crypto::raw_public_keys::{RawPublicKeyConfigBuilder, key_utils};
use saorsa_transport::crypto::rustls::QuicClientConfig;
use saorsa_transport::high_level::Endpoint;
use saorsa_transport::{NatConfig, P2pConfig, P2pEndpoint, PqcConfig, VarInt};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

/// The cadence and idle timeout the transport is expected to use. Written as
/// literals rather than imported from the crate, so that changing the constant
/// fails this test instead of silently moving what it asserts.
const EXPECTED_KEEP_ALIVE: Duration = Duration::from_secs(10);
const EXPECTED_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const EXPECTED_ACCEPT_BACKSTOP: Duration = Duration::from_secs(25);

const _: () = assert!(
    EXPECTED_ACCEPT_BACKSTOP.as_millis() > EXPECTED_KEEP_ALIVE.as_millis(),
    "the backstop must sit clear of the dialling cadence or it fires in \
     ordinary operation and the saving goes with it"
);
const _: () = assert!(
    EXPECTED_ACCEPT_BACKSTOP.as_millis() < EXPECTED_IDLE_TIMEOUT.as_millis(),
    "a backstop at or past the idle timeout cannot bound anything"
);

/// Long enough for the handshake, PQC key exchange, address discovery and the
/// MTU search to finish, so none of them lands inside the window.
const SETTLE: Duration = Duration::from_secs(8);

/// Idle window: past the idle timeout, so a connection nothing is keeping alive
/// is guaranteed to be gone by the end of it.
const IDLE_WINDOW: Duration = Duration::from_secs(40);

const _: () = assert!(
    IDLE_WINDOW.as_millis() > EXPECTED_IDLE_TIMEOUT.as_millis(),
    "the window must exceed the idle timeout or this proves nothing"
);

fn config(known_peers: Vec<SocketAddr>) -> P2pConfig {
    P2pConfig::builder()
        .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .known_peers(known_peers)
        .nat(NatConfig {
            allow_loopback: true,
            enable_relay_fallback: false,
            ..Default::default()
        })
        .pqc(PqcConfig::default())
        .build()
        .expect("build test config")
}

#[tokio::test(flavor = "multi_thread")]
async fn the_accept_side_backstop_stays_silent_while_the_dialler_pings() {
    let accepter = P2pEndpoint::new(config(vec![]))
        .await
        .expect("accepter endpoint");
    let peer = accepter.local_addr().expect("accepter address");
    let dialer = P2pEndpoint::new(config(vec![peer]))
        .await
        .expect("dialer endpoint");
    tokio::time::timeout(Duration::from_secs(20), dialer.connect(peer))
        .await
        .expect("connect did not time out")
        .expect("connect succeeded");

    // `(pings sent, pings received, acks received)` from the dialler's view.
    let counters = || async {
        match dialer.get_quic_connection(&peer).await {
            Ok(Some(conn)) => {
                let s = conn.stats();
                (s.frame_tx.ping, s.frame_rx.ping, s.frame_rx.acks)
            }
            other => panic!("no live connection to {peer} ({other:?})"),
        }
    };

    tokio::time::sleep(SETTLE).await;
    let (sent_before, received_before, acks_before) = counters().await;

    // Sample across the window so the PINGs can be timed, not just counted.
    // Each entry is (when the sampler saw the change, how many PINGs it covered).
    // A pass covering more than one cannot time them apart, so those are kept
    // and skipped rather than scored as arriving simultaneously.
    // (when the sampler saw the change, how many PINGs it covered, whether the
    // sampler was on schedule when it did).
    const POLL: Duration = Duration::from_millis(250);
    const LATE: Duration = Duration::from_secs(1);
    let started = tokio::time::Instant::now();
    let mut observations: Vec<(Duration, u64, bool)> = Vec::new();
    let mut seen = sent_before;
    let mut last_poll = tokio::time::Instant::now();
    while started.elapsed() < IDLE_WINDOW {
        tokio::time::sleep(POLL).await;
        // Both the timestamp and the on-schedule verdict are taken after the
        // counter read, because that read is itself an await that can stall.
        let (sent_now, _, _) = counters().await;
        let now = tokio::time::Instant::now();
        let on_schedule = now.duration_since(last_poll) < POLL + LATE;
        last_poll = now;
        if sent_now > seen {
            observations.push((started.elapsed(), sent_now - seen, on_schedule));
            seen = sent_now;
        }
    }
    let (sent_after, received_after, acks_after) = counters().await;
    if sent_after > seen {
        let on_schedule = tokio::time::Instant::now().duration_since(last_poll) < POLL + LATE;
        observations.push((started.elapsed(), sent_after - seen, on_schedule));
    }

    let pings_sent = sent_after - sent_before;
    let pings_received = received_after - received_before;
    let acks_received = acks_after - acks_before;
    let report = format!("sent={pings_sent} received={pings_received} acks={acks_received}");

    assert_eq!(
        pings_received, 0,
        "the accepting side's {EXPECTED_ACCEPT_BACKSTOP:?} backstop must not fire while \
         the dialling peer is pinging every {EXPECTED_KEEP_ALIVE:?} on a lossless path — \
         every packet it receives re-arms it, so firing here means the mechanism that \
         keeps the backstop cheap is not working: {report}"
    );
    assert!(
        acks_received > 0,
        "the accepting side must answer — its ACKs are the outbound packets that \
         refresh its NAT mapping in the reverse direction: {report}"
    );

    // At a 10 s cadence a 40 s window holds three or four, depending on where
    // the window opens relative to the timer.
    let expected = IDLE_WINDOW.as_secs() / EXPECTED_KEEP_ALIVE.as_secs();
    assert!(
        (expected - 1..=expected + 1).contains(&pings_sent),
        "expected about {expected} keep-alives over {IDLE_WINDOW:?} at a \
         {EXPECTED_KEEP_ALIVE:?} cadence: {report}"
    );
    assert_eq!(
        observations.iter().map(|(_, count, _)| count).sum::<u64>(),
        pings_sent,
        "every counted PING must be accounted for, or the spacing check below \
         is looking at a different population"
    );

    // Counting alone would let an unrelated PING — an MTU or PTO probe — stand
    // in for a keep-alive that never fired, since those cluster near other
    // traffic rather than arriving on a cadence. So require the gaps to be at
    // least half the interval.
    //
    // Deliberately one-sided: an upper bound would flake outright. A lower
    // bound is not unconditionally safe either, though — a sampler that runs
    // late stamps a PING after the fact, which shortens the *next* apparent
    // gap. So a pair is only scored when both of its observations covered
    // exactly one PING and both were taken on schedule; anything else makes
    // the interval unobservable rather than short.
    let minimum_gap = EXPECTED_KEEP_ALIVE / 2;
    let mut scored_gaps = 0u32;
    let mut scored_gaps_skipped = 0u32;
    for pair in observations.windows(2) {
        if let [(earlier, before, early_ok), (later, count, late_ok)] = pair {
            if *before != 1 || *count != 1 || !*early_ok || !*late_ok {
                scored_gaps_skipped += 1;
                continue;
            }
            scored_gaps += 1;
            let gap = later.saturating_sub(*earlier);
            assert!(
                gap >= minimum_gap,
                "PINGs {gap:?} apart, closer than the {minimum_gap:?} floor for a \
                 {EXPECTED_KEEP_ALIVE:?} cadence — something other than the keep-alive \
                 is emitting them: {report}"
            );
        }
    }

    // A window where every pair was batched would pass the spacing check by
    // scoring nothing at all, so say so rather than reporting a silent pass.
    assert!(
        scored_gaps > 0,
        "no PING pair was observed cleanly enough to time ({scored_gaps_skipped} \
         pairs skipped as batched or late-sampled); the spacing check proved nothing"
    );

    // `close_reason()` would only say the local driver has not processed a close
    // yet, so instead require the far side to still be answering. This shows the
    // peer is alive and reachable; it is not proof that this particular payload
    // was delivered, since ACK counters do not identify what they acknowledge.
    // Snapshot before the send. Taking it afterwards can miss the payload's own
    // ACK on a fast path, leaving the loop waiting on the next keep-alive
    // instead — a deadline the cadence only just covers.
    let acks_at_send = counters().await.2;
    dialer
        .send(&peer, b"post-idle liveness probe")
        .await
        .expect("an idle-but-alive connection must still accept a send");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if counters().await.2 > acks_at_send {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the peer did not acknowledge data sent after a {IDLE_WINDOW:?} idle window"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _ = tokio::time::timeout(Duration::from_secs(3), dialer.shutdown()).await;
    let _ = tokio::time::timeout(Duration::from_secs(3), accepter.shutdown()).await;
}

/// The case the backstop exists for: a peer that stops being heard.
///
/// The test above shows the backstop staying out of the way. This one shows it
/// doing its job. The dialling side is configured with no keep-alive at all, so
/// nothing but the backstop can hold the connection open, and the connection
/// outliving `max_idle_timeout` is only possible if the accepting side probes on
/// its own.
///
/// This goes through the low-level endpoint rather than `P2pEndpoint`, because
/// the cadences are crate-private constants with no configuration surface — a
/// silent dialler is not something the public API can express. What it locks
/// down is the property the accepting side's constant is chosen for: that one
/// side's timer is enough to carry a connection whose peer has gone quiet, and
/// that it does so at the expected interval rather than by accident.
#[tokio::test(flavor = "multi_thread")]
async fn the_accept_side_backstop_carries_a_connection_whose_peer_is_silent() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed certificate");
    let chain = vec![CertificateDer::from(cert.cert)];
    let key = PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());

    let mut accept_transport = TransportConfig::default();
    accept_transport.keep_alive_interval(Some(EXPECTED_ACCEPT_BACKSTOP));
    accept_transport.max_idle_timeout(Some(
        VarInt::from_u32(
            u32::try_from(EXPECTED_IDLE_TIMEOUT.as_millis()).expect("idle timeout fits in u32"),
        )
        .into(),
    ));
    let mut server_config =
        ServerConfig::with_single_cert(chain.clone(), key).expect("server config");
    server_config.transport_config(Arc::new(accept_transport));

    let server =
        Endpoint::server(server_config, (Ipv4Addr::LOCALHOST, 0).into()).expect("server endpoint");
    let peer = server.local_addr().expect("server address");
    tokio::spawn(async move {
        // Hold whatever is accepted: without a live handle the connection is
        // torn down and this would measure teardown, not the backstop.
        let mut held = Vec::new();
        while let Some(incoming) = server.accept().await {
            if let Ok(conn) = incoming.await {
                held.push(conn);
            }
        }
        drop(held);
    });

    let mut roots = rustls::RootCertStore::empty();
    for c in chain {
        roots.add(c).expect("trust the test certificate");
    }
    // No keep-alive on the dialling side, so the backstop is the only thing
    // that can hold this connection open.
    let mut dial_transport = TransportConfig::default();
    dial_transport.keep_alive_interval(None);
    dial_transport.max_idle_timeout(Some(
        VarInt::from_u32(
            u32::try_from(EXPECTED_IDLE_TIMEOUT.as_millis()).expect("idle timeout fits in u32"),
        )
        .into(),
    ));
    let mut client_config =
        ClientConfig::with_root_certificates(Arc::new(roots)).expect("client config");
    client_config.transport_config(Arc::new(dial_transport));

    let mut client = Endpoint::client((Ipv4Addr::LOCALHOST, 0).into()).expect("client endpoint");
    client.set_default_client_config(client_config);
    let conn = tokio::time::timeout(
        Duration::from_secs(20),
        client.connect(peer, "localhost").expect("start connect"),
    )
    .await
    .expect("connect did not time out")
    .expect("connect succeeded");

    tokio::time::sleep(SETTLE).await;
    let received_before = conn.stats().frame_rx.ping;
    tokio::time::sleep(IDLE_WINDOW).await;
    let received = conn.stats().frame_rx.ping - received_before;

    assert!(
        conn.close_reason().is_none(),
        "the connection died inside {IDLE_WINDOW:?} with only the accepting side's \
         {EXPECTED_ACCEPT_BACKSTOP:?} backstop holding it open: {:?}",
        conn.close_reason()
    );
    // A 40 s window at a 25 s backstop holds one or two, depending on where the
    // window opens relative to the timer. Zero means it never fired and the
    // connection survived for some other reason, which this test cannot claim.
    let most = IDLE_WINDOW
        .as_secs()
        .div_ceil(EXPECTED_ACCEPT_BACKSTOP.as_secs());
    assert!(
        (1..=most).contains(&received),
        "expected 1 to {most} backstop keep-alives from the accepting side over \
         {IDLE_WINDOW:?} at a {EXPECTED_ACCEPT_BACKSTOP:?} interval, saw {received}"
    );
}

/// The test that watching a healthy connection cannot substitute for.
///
/// The two tests above use hand-built endpoints, and the crate's unit tests
/// assert what the production builder and endpoint constructor produce. None of
/// that covers a configuration mutated on its way to the endpoint, because they
/// all read a value rather than watch behaviour.
///
/// This watches behaviour, through the production path: the acceptor is a real
/// `P2pEndpoint`, wired by the same constructor the fleet uses, and the dialler
/// is built here with no keep-alive at all so that nothing but the accepting
/// side's backstop can hold the connection open. It has to reach past the
/// public API for the dialler because production has no way to express a silent
/// one — that is the whole point of the arrangement under test.
#[tokio::test(flavor = "multi_thread")]
async fn a_production_endpoint_holds_a_connection_open_for_a_silent_peer() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let acceptor = P2pEndpoint::new(config(vec![]))
        .await
        .expect("production acceptor");
    let peer = acceptor.local_addr().expect("acceptor address");

    let (public_key, secret_key) = key_utils::generate_ml_dsa_keypair().expect("ML-DSA-65 keypair");
    let rpk = RawPublicKeyConfigBuilder::new()
        .with_client_key(public_key, secret_key)
        .allow_any_key()
        .with_pqc(PqcConfig::default())
        .build_rfc7250_client_config()
        .expect("RFC 7250 client config");
    let crypto = QuicClientConfig::try_from(rpk.inner().as_ref().clone()).expect("client crypto");

    let mut transport = TransportConfig::default();
    // Production enables this on both ends, and the acceptor sends an
    // ObservedAddress frame; a dialler without it kills the connection as a
    // protocol violation before the backstop is ever tested.
    transport.enable_address_discovery(true);
    transport.keep_alive_interval(None);
    transport.max_idle_timeout(Some(
        VarInt::from_u32(
            u32::try_from(EXPECTED_IDLE_TIMEOUT.as_millis()).expect("idle timeout fits in u32"),
        )
        .into(),
    ));
    let mut client_config = ClientConfig::new(Arc::new(crypto));
    client_config.transport_config(Arc::new(transport));

    let mut dialler = Endpoint::client((Ipv4Addr::LOCALHOST, 0).into()).expect("dialler endpoint");
    dialler.set_default_client_config(client_config);

    let connection = tokio::time::timeout(
        Duration::from_secs(30),
        dialler.connect(peer, "localhost").expect("start connect"),
    )
    .await
    .expect("connect did not time out")
    .expect("connect succeeded");

    tokio::time::sleep(IDLE_WINDOW).await;

    let pings_from_acceptor = connection.stats().frame_rx.ping;
    assert!(
        connection.close_reason().is_none(),
        "the connection closed inside {IDLE_WINDOW:?} while this side sent nothing, so the \
         production endpoint is not sending its {EXPECTED_ACCEPT_BACKSTOP:?} backstop: {:?}",
        connection.close_reason()
    );
    assert!(
        pings_from_acceptor > 0,
        "the connection survived without a single keep-alive arriving, so something \
         other than the backstop kept it open and this proves nothing"
    );

    let _ = tokio::time::timeout(Duration::from_secs(3), acceptor.shutdown()).await;
}
