//! A browser that leaves after ICE but before DTLS must not leave its session
//! behind. webrtc 0.14 never ended a DTLS server handshake still waiting for
//! the client's first flight, so each such session kept its peer connection
//! and tasks (about 60 KB) after the node closed it. vendor/webrtc cancels the
//! handshake when the connection closes.

// Counts live heap bytes for this test binary only.
#![allow(unsafe_code)]

use saorsa_transport::webrtc::{server_answer_sdp, WebRtcDirectEndpoint};
use saorsa_transport::webrtc_direct::{WebRtcCertificate, WebRtcDirectListener};
use std::alloc::{GlobalAlloc, Layout, System};
use std::error::Error;
use std::io;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::Duration;
use webrtc_stack::api::{setting_engine::SettingEngine, APIBuilder};
use webrtc_stack::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc_stack::ice::{mdns::MulticastDnsMode, network_type::NetworkType};
use webrtc_stack::ice_transport::ice_connection_state::RTCIceConnectionState;
use webrtc_stack::peer_connection::{
    configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription,
};

static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);

struct CountingAllocator;

#[allow(clippy::cast_possible_wrap)]
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let block = unsafe { System.alloc(layout) };
        if !block.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as isize, Ordering::Relaxed);
        }
        block
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        unsafe { System.dealloc(block, layout) };
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, block: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(block, layout, size) };
        if !moved.is_null() {
            LIVE_BYTES.fetch_add(size as isize - layout.size() as isize, Ordering::Relaxed);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const SESSIONS: isize = 6;
// A leaked session keeps about 60 KB on the node side alone.
const MAX_BYTES_KEPT_PER_SESSION: isize = 16 * 1024;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sessions_closed_during_the_dtls_handshake_free_their_memory() -> Result<(), Box<dyn Error>>
{
    let certificate = WebRtcCertificate::generate()?;
    let certificate_hash = certificate.sha256_digest()?;
    let mut listener = WebRtcDirectListener::bind("127.0.0.1:0".parse()?, certificate).await?;
    let endpoint = WebRtcDirectEndpoint {
        multiaddr: String::new(),
        host_protocol: "ip4".to_string(),
        host: "127.0.0.1".to_string(),
        port: listener.local_addr().port(),
        peer_id: "00".repeat(32),
        certificate_hash,
    };

    // The first session warms up lazily allocated state.
    stall_dtls_then_close(&mut listener, &endpoint, 0).await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let before = LIVE_BYTES.load(Ordering::Relaxed);
    for round in 1..=SESSIONS {
        stall_dtls_then_close(&mut listener, &endpoint, round).await?;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let kept = (LIVE_BYTES.load(Ordering::Relaxed) - before) / SESSIONS;
    assert!(
        kept < MAX_BYTES_KEPT_PER_SESSION,
        "each closed session kept {kept} bytes"
    );
    Ok(())
}

/// Connect ICE, then stall DTLS: the answer tells our peer that the node is
/// the active side, so both wait as DTLS servers for a `ClientHello`. The node
/// closes the session mid-handshake, as it does when its setup deadline passes.
async fn stall_dtls_then_close(
    listener: &mut WebRtcDirectListener,
    endpoint: &WebRtcDirectEndpoint,
    round: isize,
) -> Result<(), Box<dyn Error>> {
    let password = format!("stalledDtlsPassword{round:08}");
    let mut settings = SettingEngine::default();
    settings.set_ice_multicast_dns_mode(MulticastDnsMode::Disabled);
    settings.set_ice_credentials(format!("stalledDtls{round}"), password.clone());
    settings.set_network_types(vec![NetworkType::Udp4]);
    settings.set_include_loopback_candidate(true);
    settings.set_ip_filter(Box::new(|ip| ip.is_loopback()));
    let browser = APIBuilder::new()
        .with_setting_engine(settings)
        .build()
        .new_peer_connection(RTCConfiguration::default())
        .await?;
    browser
        .create_data_channel(
            "stalled",
            Some(RTCDataChannelInit {
                negotiated: Some(0),
                ..Default::default()
            }),
        )
        .await?;
    let offer = browser.create_offer(None).await?;
    browser.set_local_description(offer).await?;
    let answer = server_answer_sdp(endpoint, &format!("saorsa+webrtc+v2/{password}"))?
        .replace("a=setup:passive", "a=setup:active");
    browser
        .set_remote_description(RTCSessionDescription::answer(answer)?)
        .await?;

    let connection = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await??;
    tokio::time::timeout(Duration::from_secs(5), async {
        while browser.ice_connection_state() != RTCIceConnectionState::Connected {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| io::Error::other("ICE did not connect"))?;
    // Both sides are now waiting for a `ClientHello` that never comes.
    tokio::time::sleep(Duration::from_millis(300)).await;
    connection.close().await?;
    drop(connection);
    browser.close().await?;
    Ok(())
}
