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

//! Out-of-band control frames carried over the MASQUE relay tunnel.
//!
//! The data plane wraps every payload in an [`UncompressedDatagram`]
//! prefixed by a 4-byte big-endian length.  Any frame whose length
//! prefix equals the sentinel value [`CONTROL_FRAME_MARKER`] is a
//! control frame instead — the next four bytes are the body length,
//! followed by a 1-byte type tag and a type-specific payload.
//!
//! Currently only one control frame type exists: [`PmtuUpdate`], sent
//! from the relay-server to the relay-client when the relay's egress
//! UDP send to a third party fails with `EMSGSIZE`.  The relay-client's
//! [`crate::masque::MasqueRelaySocket`] then enforces the suggested
//! MTU on subsequent [`AsyncUdpSocket::try_send`] calls to that target,
//! simulating packet loss for Quinn's DPLPMTUD machinery so that the
//! inner connection's MTU estimate converges to the path reality
//! without an explicit Quinn-level API.
//!
//! [`UncompressedDatagram`]: crate::masque::UncompressedDatagram
//! [`AsyncUdpSocket::try_send`]: crate::high_level::AsyncUdpSocket::try_send
//! [`PmtuUpdate`]: TunnelControlFrame::PmtuUpdate

use bytes::{BufMut, Bytes, BytesMut};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// Sentinel length value that marks a control frame on the wire.
/// Chosen above any plausible data-frame length (the existing data
/// plane caps frames at 512 KiB), so a peer running an older build
/// will treat it as a corrupt-length error and tear the tunnel down
/// — which is the right outcome for a feature-mismatched session.
pub(crate) const CONTROL_FRAME_MARKER: u32 = 0xFFFF_FFFF;

/// Type tag for a path-MTU update control frame.
const CTRL_TYPE_PMTU_UPDATE: u8 = 0x01;

/// Address-family tag for the [`SocketAddr`] encoding inside a control
/// frame body. Only IPv4 (`4`) and IPv6 (`6`) are defined.
const ADDR_FAMILY_V4: u8 = 4;
const ADDR_FAMILY_V6: u8 = 6;

/// One-byte type tag + worst-case body for the largest defined frame
/// (PmtuUpdate over IPv6: 1 family + 16 addr + 2 port + 2 mtu = 21,
/// plus the type tag = 22).  Used as a safety cap on inbound control
/// frame length on the relay-client side so a malformed frame can't
/// allocate huge buffers.
pub(crate) const MAX_CONTROL_FRAME_BODY: u32 = 64;

/// A control frame carried over the MASQUE relay tunnel out-of-band
/// from data datagrams. See the module-level docs for the wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TunnelControlFrame {
    /// Relay-server tells the relay-client: "the egress path to
    /// `target` rejected my last datagram for being too large — please
    /// stop generating packets larger than `mtu` bytes for that
    /// target, so Quinn's DPLPMTUD lowers the connection MTU."
    PmtuUpdate { target: SocketAddr, mtu: u16 },
}

impl TunnelControlFrame {
    /// Encode the body of the frame (everything after the
    /// `[CONTROL_FRAME_MARKER][body_len]` header).
    pub(crate) fn encode_body(&self) -> Bytes {
        match self {
            Self::PmtuUpdate { target, mtu } => {
                let mut buf = BytesMut::with_capacity(32);
                buf.put_u8(CTRL_TYPE_PMTU_UPDATE);
                encode_socket_addr(&mut buf, *target);
                buf.put_u16(*mtu);
                buf.freeze()
            }
        }
    }

    /// Decode the body of a control frame (everything after the
    /// `[CONTROL_FRAME_MARKER][body_len]` header).  Returns `None` for
    /// unknown type tags or malformed bodies — callers should log and
    /// skip rather than tearing down the tunnel, so introducing a new
    /// control-frame type is not itself a breaking wire-format change.
    pub(crate) fn decode_body(body: &[u8]) -> Option<Self> {
        let (ctype, mut rest) = body.split_first()?;
        match *ctype {
            CTRL_TYPE_PMTU_UPDATE => {
                let target = decode_socket_addr(&mut rest)?;
                if rest.len() < 2 {
                    return None;
                }
                let mtu = u16::from_be_bytes([rest[0], rest[1]]);
                Some(Self::PmtuUpdate { target, mtu })
            }
            _ => None,
        }
    }
}

fn encode_socket_addr(buf: &mut BytesMut, addr: SocketAddr) {
    match addr {
        SocketAddr::V4(v4) => {
            buf.put_u8(ADDR_FAMILY_V4);
            buf.put_slice(&v4.ip().octets());
            buf.put_u16(v4.port());
        }
        SocketAddr::V6(v6) => {
            buf.put_u8(ADDR_FAMILY_V6);
            buf.put_slice(&v6.ip().octets());
            buf.put_u16(v6.port());
        }
    }
}

fn decode_socket_addr(buf: &mut &[u8]) -> Option<SocketAddr> {
    let (family, rest) = buf.split_first()?;
    *buf = rest;
    match *family {
        ADDR_FAMILY_V4 => {
            if buf.len() < 6 {
                return None;
            }
            let ip = Ipv4Addr::new(buf[0], buf[1], buf[2], buf[3]);
            let port = u16::from_be_bytes([buf[4], buf[5]]);
            *buf = &buf[6..];
            Some(SocketAddr::new(IpAddr::V4(ip), port))
        }
        ADDR_FAMILY_V6 => {
            if buf.len() < 18 {
                return None;
            }
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&buf[..16]);
            let ip = Ipv6Addr::from(octets);
            let port = u16::from_be_bytes([buf[16], buf[17]]);
            *buf = &buf[18..];
            Some(SocketAddr::new(IpAddr::V6(ip), port))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v4(a: u8, b: u8, c: u8, d: u8, port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(a, b, c, d)), port)
    }

    fn v6_loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port)
    }

    #[test]
    fn roundtrip_pmtu_update_v4() {
        let frame = TunnelControlFrame::PmtuUpdate {
            target: v4(192, 0, 2, 1, 9000),
            mtu: 1252,
        };
        let body = frame.encode_body();
        let decoded = TunnelControlFrame::decode_body(&body).expect("decode");
        assert_eq!(frame, decoded);
    }

    #[test]
    fn roundtrip_pmtu_update_v6() {
        let frame = TunnelControlFrame::PmtuUpdate {
            target: v6_loopback(443),
            mtu: 1452,
        };
        let body = frame.encode_body();
        let decoded = TunnelControlFrame::decode_body(&body).expect("decode");
        assert_eq!(frame, decoded);
    }

    #[test]
    fn unknown_type_returns_none() {
        // Body whose first byte is an undefined type tag.
        let body = [0xFE_u8, 0, 0, 0, 0];
        assert!(TunnelControlFrame::decode_body(&body).is_none());
    }

    #[test]
    fn truncated_body_returns_none() {
        let frame = TunnelControlFrame::PmtuUpdate {
            target: v4(127, 0, 0, 1, 1234),
            mtu: 1200,
        };
        let body = frame.encode_body();
        for short_len in 0..body.len() {
            assert!(
                TunnelControlFrame::decode_body(&body[..short_len]).is_none(),
                "truncated to {short_len} bytes should not decode"
            );
        }
    }
}
