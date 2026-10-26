//! The bytes of every message one node sends another, as v0.23.0+ds.15 writes
//! and reads them, and the variants of each that it knows; and of the messages
//! appended since, which go only to peers known to read them: paged neighbour
//! sync's page request and its answer, and the one-way "full" notice a full
//! node sends.
//!
//! A release that changes messages between nodes keeps the rule in the
//! README's "Messages between nodes": a new message type is appended to
//! `ReplicationMessageBody` and goes only to peers known to read it; no field
//! is added to an existing message; no variant is added to a reply older
//! nodes receive. An older node can't decode what breaks that rule, and an
//! auditor that can't decode a reply fails every key it challenged.
//!
//! Each sample below is pinned to its bytes. A change to an existing message
//! fails here, and so does a variant added to a message or to one of its
//! replies without a sample and its bytes, which come with a test of what an
//! older node's decoder makes of the new bytes (as
//! `old_decoder_tolerates_new_neighbor_sync_request` in
//! src/replication/protocol.rs does for the last field added, before the
//! rule). The mixed-version harness (`scripts/test_mixed_versions.py`) then
//! runs old and new nodes together.
//!
//! The payment proofs inside `FreshReplicationOffer`, `PaidNotify` and
//! `PointerFreshOffer` are opaque bytes here: `tests/fork_pins.rs` pins them as
//! encoded (`payment_encodings`), with the quotes inside them and each
//! quote's hash. A release that encodes a proof another way brings a test of
//! what ds.15's decoder makes of the new encoding, as for a message.
//!
//! On a mismatch the test prints every sample's bytes as they are now, for
//! review. Upstream has no such file, so a merge can't drop it while
//! resolving a conflict.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ant_node::replication::commitment::StorageCommitment;
use ant_node::replication::protocol::{
    AuditChallenge, AuditResponse, FetchRequest, FetchResponse, FreshReplicationOffer,
    FreshReplicationResponse, FullNotice, GetCommitmentByPin, GetCommitmentByPinResponse,
    KeyVerificationResult, NeighborSyncPageRequest, NeighborSyncPageResponse, NeighborSyncRequest,
    NeighborSyncResponse, PaidNotify, PointerFetchRequest, PointerFetchResponse, PointerFreshOffer,
    PointerHints, PointerStateRequest, PointerStateResponse, PointerStateSummary, RejectKind,
    ReplicationMessage, ReplicationMessageBody, SubtreeAuditChallenge, SubtreeAuditResponse,
    SubtreeSliceChallenge, SubtreeSliceItem, SubtreeSliceOpening, SubtreeSliceResponse,
    VerificationRequest, VerificationResponse,
};
use ant_node::replication::subtree::{SubtreeLeaf, SubtreeProof};
use std::fmt::Write as _;

/// The request id every sample is sent with: two bytes as a varint, so a
/// sample's bytes show where the id ends and the body starts.
const REQUEST_ID: u64 = 0x1234;

/// What postcard says when it reads a variant index no variant has.
const UNKNOWN_VARIANT: &str = "Serde Deserialization Error";

/// What postcard says when the bytes end inside a message.
const CUT_SHORT: &str = "Hit the end of buffer, expected more data";

fn key(byte: u8) -> [u8; 32] {
    [byte; 32]
}

/// A commitment with short stand-ins for the key and signature: only its
/// encoding is under test, not its signature.
fn commitment() -> StorageCommitment {
    StorageCommitment {
        root: key(0xC1),
        key_count: 3,
        sender_peer_id: key(0xC2),
        sender_public_key: vec![0xC3, 0xC4],
        signature: vec![0xC5, 0xC6, 0xC7],
    }
}

fn pointer_summary(byte: u8) -> PointerStateSummary {
    PointerStateSummary {
        address: key(byte),
        state_id: key(byte + 1),
        counter: 300,
        target_tag: 1,
        target_address: key(byte + 2),
    }
}

/// The name of a sample: its message, and the variant of a reply or a reply's
/// part. The match is exhaustive, so a variant added to any of them doesn't
/// compile until it is named here, and given a sample and bytes below.
#[allow(clippy::too_many_lines)]
fn name(body: &ReplicationMessageBody) -> String {
    use ReplicationMessageBody as B;
    let kind = |kind: &RejectKind| match kind {
        RejectKind::UnknownCommitment => "UnknownCommitment",
        RejectKind::Transient => "Transient",
        RejectKind::Protocol => "Protocol",
    };
    match body {
        B::FreshReplicationOffer(_) => "FreshReplicationOffer".into(),
        B::FreshReplicationResponse(FreshReplicationResponse::Accepted { .. }) => {
            "FreshReplicationResponse::Accepted".into()
        }
        B::FreshReplicationResponse(FreshReplicationResponse::Rejected { .. }) => {
            "FreshReplicationResponse::Rejected".into()
        }
        B::PaidNotify(_) => "PaidNotify".into(),
        B::NeighborSyncRequest(request) => format!(
            "NeighborSyncRequest, commitment {}",
            if request.commitment.is_some() {
                "Some"
            } else {
                "None"
            }
        ),
        B::NeighborSyncResponse(response) => format!(
            "NeighborSyncResponse, commitment {}",
            if response.commitment.is_some() {
                "Some"
            } else {
                "None"
            }
        ),
        B::VerificationRequest(_) => "VerificationRequest".into(),
        B::VerificationResponse(_) => "VerificationResponse".into(),
        B::FetchRequest(_) => "FetchRequest".into(),
        B::FetchResponse(FetchResponse::Success { .. }) => "FetchResponse::Success".into(),
        B::FetchResponse(FetchResponse::NotFound { .. }) => "FetchResponse::NotFound".into(),
        B::FetchResponse(FetchResponse::Error { .. }) => "FetchResponse::Error".into(),
        B::AuditChallenge(_) => "AuditChallenge".into(),
        B::AuditResponse(AuditResponse::Digests { .. }) => "AuditResponse::Digests".into(),
        B::AuditResponse(AuditResponse::Bootstrapping { .. }) => {
            "AuditResponse::Bootstrapping".into()
        }
        B::AuditResponse(AuditResponse::Rejected { .. }) => "AuditResponse::Rejected".into(),
        B::SubtreeAuditChallenge(_) => "SubtreeAuditChallenge".into(),
        B::SubtreeAuditResponse(SubtreeAuditResponse::Proof { .. }) => {
            "SubtreeAuditResponse::Proof".into()
        }
        B::SubtreeAuditResponse(SubtreeAuditResponse::Bootstrapping { .. }) => {
            "SubtreeAuditResponse::Bootstrapping".into()
        }
        B::SubtreeAuditResponse(SubtreeAuditResponse::Rejected { kind: k, .. }) => {
            format!("SubtreeAuditResponse::Rejected, {}", kind(k))
        }
        B::SubtreeSliceChallenge(_) => "SubtreeSliceChallenge".into(),
        B::SubtreeSliceResponse(SubtreeSliceResponse::Items { items, .. }) => {
            let items: Vec<&str> = items
                .iter()
                .map(|item| match item {
                    SubtreeSliceItem::Present { .. } => "Present",
                    SubtreeSliceItem::Absent { .. } => "Absent",
                    SubtreeSliceItem::PointerRecord { .. } => "PointerRecord",
                })
                .collect();
            format!("SubtreeSliceResponse::Items, {}", items.join(" "))
        }
        B::SubtreeSliceResponse(SubtreeSliceResponse::Bootstrapping { .. }) => {
            "SubtreeSliceResponse::Bootstrapping".into()
        }
        B::SubtreeSliceResponse(SubtreeSliceResponse::Rejected { kind: k, .. }) => {
            format!("SubtreeSliceResponse::Rejected, {}", kind(k))
        }
        B::GetCommitmentByPin(_) => "GetCommitmentByPin".into(),
        B::GetCommitmentByPinResponse(GetCommitmentByPinResponse::Found { .. }) => {
            "GetCommitmentByPinResponse::Found".into()
        }
        B::GetCommitmentByPinResponse(GetCommitmentByPinResponse::NotRetained { .. }) => {
            "GetCommitmentByPinResponse::NotRetained".into()
        }
        B::PointerFreshOffer(_) => "PointerFreshOffer".into(),
        B::PointerHints(_) => "PointerHints".into(),
        B::PointerFetchRequest(_) => "PointerFetchRequest".into(),
        B::PointerFetchResponse(response) => format!(
            "PointerFetchResponse, record {}",
            if response.record.is_some() {
                "Some"
            } else {
                "None"
            }
        ),
        B::PointerStateRequest(_) => "PointerStateRequest".into(),
        B::PointerStateResponse(_) => "PointerStateResponse".into(),
        B::NeighborSyncPageRequest(request) => format!(
            "NeighborSyncPageRequest, commitment {}",
            if request.commitment.is_some() {
                "Some"
            } else {
                "None"
            }
        ),
        B::NeighborSyncPageResponse(NeighborSyncPageResponse::Page { commitment, .. }) => {
            format!(
                "NeighborSyncPageResponse::Page, commitment {}",
                if commitment.is_some() { "Some" } else { "None" }
            )
        }
        B::NeighborSyncPageResponse(NeighborSyncPageResponse::Busy { .. }) => {
            "NeighborSyncPageResponse::Busy".into()
        }
        B::FullNotice(FullNotice { full: true }) => "FullNotice, full".into(),
        B::FullNotice(FullNotice { full: false }) => "FullNotice, no longer full".into(),
    }
}

/// One sample of every message, of every variant of its replies, and of each
/// optional part present and absent.
#[allow(clippy::too_many_lines)]
fn samples() -> Vec<ReplicationMessageBody> {
    use ReplicationMessageBody as B;
    let rejected_subtree = |kind| {
        B::SubtreeAuditResponse(SubtreeAuditResponse::Rejected {
            challenge_id: 9,
            kind,
            reason: "no".into(),
        })
    };
    let rejected_slice = |kind| {
        B::SubtreeSliceResponse(SubtreeSliceResponse::Rejected {
            challenge_id: 9,
            kind,
            reason: "no".into(),
        })
    };
    vec![
        B::FreshReplicationOffer(FreshReplicationOffer {
            key: key(0x01),
            data: vec![0xD0, 0xD1, 0xD2],
            proof_of_payment: vec![0x01, 0xE0],
        }),
        B::FreshReplicationResponse(FreshReplicationResponse::Accepted { key: key(0x02) }),
        B::FreshReplicationResponse(FreshReplicationResponse::Rejected {
            key: key(0x03),
            reason: "full".into(),
        }),
        B::PaidNotify(PaidNotify {
            key: key(0x04),
            proof_of_payment: vec![0x01, 0xE1],
        }),
        B::NeighborSyncRequest(NeighborSyncRequest {
            replica_hints: vec![key(0x05), key(0x06)],
            paid_hints: vec![key(0x07)],
            bootstrapping: true,
            commitment: None,
        }),
        B::NeighborSyncRequest(NeighborSyncRequest {
            replica_hints: vec![],
            paid_hints: vec![],
            bootstrapping: false,
            commitment: Some(commitment()),
        }),
        B::NeighborSyncResponse(NeighborSyncResponse {
            replica_hints: vec![key(0x08)],
            paid_hints: vec![],
            bootstrapping: false,
            rejected_keys: vec![key(0x09)],
            commitment: None,
        }),
        B::NeighborSyncResponse(NeighborSyncResponse {
            replica_hints: vec![],
            paid_hints: vec![key(0x0A)],
            bootstrapping: true,
            rejected_keys: vec![],
            commitment: Some(commitment()),
        }),
        B::VerificationRequest(VerificationRequest {
            keys: vec![key(0x0B), key(0x0C)],
            paid_list_check_indices: vec![1, 300],
        }),
        B::VerificationResponse(VerificationResponse {
            results: vec![
                KeyVerificationResult {
                    key: key(0x0D),
                    present: true,
                    paid: None,
                },
                KeyVerificationResult {
                    key: key(0x0E),
                    present: false,
                    paid: Some(true),
                },
                KeyVerificationResult {
                    key: key(0x0F),
                    present: false,
                    paid: Some(false),
                },
            ],
        }),
        B::FetchRequest(FetchRequest { key: key(0x10) }),
        B::FetchResponse(FetchResponse::Success {
            key: key(0x11),
            data: vec![0xD3, 0xD4],
        }),
        B::FetchResponse(FetchResponse::NotFound { key: key(0x12) }),
        B::FetchResponse(FetchResponse::Error {
            key: key(0x13),
            reason: "io".into(),
        }),
        B::AuditChallenge(AuditChallenge {
            challenge_id: 7,
            nonce: key(0x14),
            challenged_peer_id: key(0x15),
            keys: vec![key(0x16)],
        }),
        B::AuditResponse(AuditResponse::Digests {
            challenge_id: 7,
            digests: vec![key(0x17), [0u8; 32]],
        }),
        B::AuditResponse(AuditResponse::Bootstrapping { challenge_id: 7 }),
        B::AuditResponse(AuditResponse::Rejected {
            challenge_id: 7,
            reason: "busy".into(),
        }),
        B::SubtreeAuditChallenge(SubtreeAuditChallenge {
            challenge_id: 8,
            nonce: key(0x18),
            challenged_peer_id: key(0x19),
            expected_commitment_hash: key(0x1A),
        }),
        B::SubtreeAuditResponse(SubtreeAuditResponse::Proof {
            challenge_id: 8,
            commitment: commitment(),
            proof: SubtreeProof {
                leaves: vec![SubtreeLeaf {
                    key: key(0x1B),
                    bytes_hash: key(0x1B),
                    content_len: 4096,
                    nonced_root: key(0x1C),
                }],
                sibling_cut_hashes: vec![key(0x1D)],
            },
        }),
        B::SubtreeAuditResponse(SubtreeAuditResponse::Bootstrapping { challenge_id: 8 }),
        rejected_subtree(RejectKind::UnknownCommitment),
        rejected_subtree(RejectKind::Transient),
        rejected_subtree(RejectKind::Protocol),
        B::SubtreeSliceChallenge(SubtreeSliceChallenge {
            challenge_id: 9,
            nonce: key(0x1E),
            challenged_peer_id: key(0x1F),
            expected_commitment_hash: key(0x20),
            openings: vec![SubtreeSliceOpening {
                key: key(0x21),
                block_index: 2,
            }],
        }),
        B::SubtreeSliceResponse(SubtreeSliceResponse::Items {
            challenge_id: 9,
            items: vec![
                SubtreeSliceItem::Present {
                    key: key(0x22),
                    block_index: 2,
                    bao_slice: vec![0xB0, 0xB1],
                    nonced_siblings: vec![key(0x23)],
                },
                SubtreeSliceItem::Absent { key: key(0x24) },
                SubtreeSliceItem::PointerRecord {
                    key: key(0x25),
                    records: vec![vec![0xA0], vec![]],
                },
            ],
        }),
        B::SubtreeSliceResponse(SubtreeSliceResponse::Bootstrapping { challenge_id: 9 }),
        rejected_slice(RejectKind::UnknownCommitment),
        rejected_slice(RejectKind::Transient),
        rejected_slice(RejectKind::Protocol),
        B::GetCommitmentByPin(GetCommitmentByPin { pin: key(0x26) }),
        B::GetCommitmentByPinResponse(GetCommitmentByPinResponse::Found {
            commitment: commitment(),
        }),
        B::GetCommitmentByPinResponse(GetCommitmentByPinResponse::NotRetained { pin: key(0x27) }),
        B::PointerFreshOffer(PointerFreshOffer {
            record: vec![0xF0, 0xF1],
            proof_of_payment: vec![0x01, 0xE2],
        }),
        B::PointerHints(PointerHints {
            hints: vec![pointer_summary(0x28)],
        }),
        B::PointerFetchRequest(PointerFetchRequest { address: key(0x2B) }),
        B::PointerFetchResponse(PointerFetchResponse {
            address: key(0x2C),
            record: Some(vec![0xF2]),
        }),
        B::PointerFetchResponse(PointerFetchResponse {
            address: key(0x2D),
            record: None,
        }),
        B::PointerStateRequest(PointerStateRequest {
            addresses: vec![key(0x2E)],
        }),
        B::PointerStateResponse(PointerStateResponse {
            states: vec![Some(pointer_summary(0x2F)), None],
        }),
        B::NeighborSyncPageRequest(NeighborSyncPageRequest {
            cycle: 0x0102_0304_0506_0708,
            page: 0,
            pages: 3,
            replica_hints: vec![key(0x32)],
            paid_hints: vec![key(0x33)],
            bootstrapping: true,
            commitment: Some(commitment()),
        }),
        B::NeighborSyncPageRequest(NeighborSyncPageRequest {
            cycle: 0x0102_0304_0506_0708,
            page: 2,
            pages: 3,
            replica_hints: vec![],
            paid_hints: vec![key(0x34), key(0x35)],
            bootstrapping: false,
            commitment: None,
        }),
        B::NeighborSyncPageResponse(NeighborSyncPageResponse::Page {
            cycle: 0x0102_0304_0506_0708,
            page: 0,
            pages: 300,
            replica_hints: vec![key(0x36), key(0x37)],
            paid_hints: vec![],
            bootstrapping: false,
            commitment: Some(commitment()),
        }),
        B::NeighborSyncPageResponse(NeighborSyncPageResponse::Page {
            cycle: 0x0102_0304_0506_0708,
            page: 299,
            pages: 300,
            replica_hints: vec![],
            paid_hints: vec![key(0x38)],
            bootstrapping: true,
            commitment: None,
        }),
        B::NeighborSyncPageResponse(NeighborSyncPageResponse::Busy {
            cycle: 0x0102_0304_0506_0708,
            page: 1,
        }),
        B::FullNotice(FullNotice { full: true }),
        B::FullNotice(FullNotice { full: false }),
    ]
}

/// The bytes of each sample, sent with `REQUEST_ID`, as ds.15 writes them.
const PINNED: &[(&str, &str)] = &[
    (
        "FreshReplicationOffer",
        "b42400010101010101010101010101010101010101010101010101010101010101010103d0d1d20201e0",
    ),
    (
        "FreshReplicationResponse::Accepted",
        "b42401000202020202020202020202020202020202020202020202020202020202020202",
    ),
    (
        "FreshReplicationResponse::Rejected",
        "b424010103030303030303030303030303030303030303030303030303030303030303030466756c6c",
    ),
    (
        "PaidNotify",
        "b4240204040404040404040404040404040404040404040404040404040404040404040201e1",
    ),
    (
        "NeighborSyncRequest, commitment None",
        "b4240302050505050505050505050505050505050505050505050505050505050505050506060606060606060606060606060606060606060606060606060606060606060107070707070707070707070707070707070707070707070707070707070707070100",
    ),
    (
        "NeighborSyncRequest, commitment Some",
        "b4240300000001c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c103c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c202c3c403c5c6c7",
    ),
    (
        "NeighborSyncResponse, commitment None",
        "b42404010808080808080808080808080808080808080808080808080808080808080808000001090909090909090909090909090909090909090909090909090909090909090900",
    ),
    (
        "NeighborSyncResponse, commitment Some",
        "b4240400010a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a010001c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c103c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c202c3c403c5c6c7",
    ),
    (
        "VerificationRequest",
        "b42405020b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0201ac02",
    ),
    (
        "VerificationResponse",
        "b42406030d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d01000e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0001010f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f000100",
    ),
    (
        "FetchRequest",
        "b424071010101010101010101010101010101010101010101010101010101010101010",
    ),
    (
        "FetchResponse::Success",
        "b4240800111111111111111111111111111111111111111111111111111111111111111102d3d4",
    ),
    (
        "FetchResponse::NotFound",
        "b42408011212121212121212121212121212121212121212121212121212121212121212",
    ),
    (
        "FetchResponse::Error",
        "b4240802131313131313131313131313131313131313131313131313131313131313131302696f",
    ),
    (
        "AuditChallenge",
        "b424090714141414141414141414141414141414141414141414141414141414141414141515151515151515151515151515151515151515151515151515151515151515011616161616161616161616161616161616161616161616161616161616161616",
    ),
    (
        "AuditResponse::Digests",
        "b4240a00070217171717171717171717171717171717171717171717171717171717171717170000000000000000000000000000000000000000000000000000000000000000",
    ),
    (
        "AuditResponse::Bootstrapping",
        "b4240a0107",
    ),
    (
        "AuditResponse::Rejected",
        "b4240a02070462757379",
    ),
    (
        "SubtreeAuditChallenge",
        "b4240b08181818181818181818181818181818181818181818181818181818181818181819191919191919191919191919191919191919191919191919191919191919191a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a",
    ),
    (
        "SubtreeAuditResponse::Proof",
        "b4240c0008c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c103c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c202c3c403c5c6c7011b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b80201c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c011d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d",
    ),
    (
        "SubtreeAuditResponse::Bootstrapping",
        "b4240c0108",
    ),
    (
        "SubtreeAuditResponse::Rejected, UnknownCommitment",
        "b4240c020900026e6f",
    ),
    (
        "SubtreeAuditResponse::Rejected, Transient",
        "b4240c020901026e6f",
    ),
    (
        "SubtreeAuditResponse::Rejected, Protocol",
        "b4240c020902026e6f",
    ),
    (
        "SubtreeSliceChallenge",
        "b4240d091e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f202020202020202020202020202020202020202020202020202020202020202001212121212121212121212121212121212121212121212121212121212121212102",
    ),
    (
        "SubtreeSliceResponse::Items, Present Absent PointerRecord",
        "b4240e0009030022222222222222222222222222222222222222222222222222222222222222220202b0b10123232323232323232323232323232323232323232323232323232323232323230124242424242424242424242424242424242424242424242424242424242424240225252525252525252525252525252525252525252525252525252525252525250201a000",
    ),
    (
        "SubtreeSliceResponse::Bootstrapping",
        "b4240e0109",
    ),
    (
        "SubtreeSliceResponse::Rejected, UnknownCommitment",
        "b4240e020900026e6f",
    ),
    (
        "SubtreeSliceResponse::Rejected, Transient",
        "b4240e020901026e6f",
    ),
    (
        "SubtreeSliceResponse::Rejected, Protocol",
        "b4240e020902026e6f",
    ),
    (
        "GetCommitmentByPin",
        "b4240f2626262626262626262626262626262626262626262626262626262626262626",
    ),
    (
        "GetCommitmentByPinResponse::Found",
        "b4241000c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c103c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c202c3c403c5c6c7",
    ),
    (
        "GetCommitmentByPinResponse::NotRetained",
        "b42410012727272727272727272727272727272727272727272727272727272727272727",
    ),
    (
        "PointerFreshOffer",
        "b4241102f0f10201e2",
    ),
    (
        "PointerHints",
        "b424120128282828282828282828282828282828282828282828282828282828282828282929292929292929292929292929292929292929292929292929292929292929ac02012a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a",
    ),
    (
        "PointerFetchRequest",
        "b424132b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b",
    ),
    (
        "PointerFetchResponse, record Some",
        "b424142c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c0101f2",
    ),
    (
        "PointerFetchResponse, record None",
        "b424142d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d00",
    ),
    (
        "PointerStateRequest",
        "b42415012e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e",
    ),
    (
        "PointerStateResponse",
        "b4241602012f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f3030303030303030303030303030303030303030303030303030303030303030ac0201313131313131313131313131313131313131313131313131313131313131313100",
    ),
    (
        "NeighborSyncPageRequest, commitment Some",
        "b42417888e98a8c0e080810100030132323232323232323232323232323232323232323232323232323232323232320133333333333333333333333333333333333333333333333333333333333333330101c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c103c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c202c3c403c5c6c7",
    ),
    (
        "NeighborSyncPageRequest, commitment None",
        "b42417888e98a8c0e080810102030002343434343434343434343434343434343434343434343434343434343434343435353535353535353535353535353535353535353535353535353535353535350000",
    ),
    (
        "NeighborSyncPageResponse::Page, commitment Some",
        "b4241800888e98a8c0e080810100ac020236363636363636363636363636363636363636363636363636363636363636363737373737373737373737373737373737373737373737373737373737373737000001c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c103c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c202c3c403c5c6c7",
    ),
    (
        "NeighborSyncPageResponse::Page, commitment None",
        "b4241800888e98a8c0e0808101ab02ac02000138383838383838383838383838383838383838383838383838383838383838380100",
    ),
    (
        "NeighborSyncPageResponse::Busy",
        "b4241801888e98a8c0e080810101",
    ),
    // Appended since ds.15, whose message enum ends at index 22, and sent only
    // to peers known to read them; ds.15 drops the notice (`a_node_before_the_full_notice_reads_every_earlier_body_and_drops_the_notice`
    // in src/replication/protocol.rs).
    // Its index byte is FULL_NOTICE_INDEX (checked below): 25, after
    // node-1b-sync's page messages (23, 24), which were merged before it.
    ("FullNotice, full", "b4241901"),
    ("FullNotice, no longer full", "b4241900"),
];

/// The notice's pinned bytes carry the index the protocol gives it, so the
/// merge that moves it past node-1b-sync's messages re-pins them here.
#[test]
fn the_full_notice_pins_carry_its_index() {
    use ant_node::replication::protocol::FULL_NOTICE_INDEX;
    // After node-1b-sync's page messages, 23 and 24 (build plan, ADR-0024).
    assert_eq!(FULL_NOTICE_INDEX, 25);
    for (name, flag) in [
        ("FullNotice, full", "01"),
        ("FullNotice, no longer full", "00"),
    ] {
        let pinned = PINNED
            .iter()
            .find(|(pinned, _)| *pinned == name)
            .map(|(_, bytes)| *bytes)
            .unwrap();
        assert_eq!(
            pinned,
            format!("b424{FULL_NOTICE_INDEX:02x}{flag}"),
            "{name}"
        );
    }
}

fn encode(body: ReplicationMessageBody) -> Vec<u8> {
    ReplicationMessage {
        request_id: REQUEST_ID,
        body,
    }
    .encode()
    .unwrap()
}

#[test]
fn every_message_between_nodes_keeps_its_bytes() {
    let samples = samples();
    let found: Vec<(String, String)> = samples
        .iter()
        .map(|body| (name(body), hex::encode(encode(body.clone()))))
        .collect();

    let mut names: Vec<&str> = found.iter().map(|(name, _)| name.as_str()).collect();
    names.sort_unstable();
    let before = names.len();
    names.dedup();
    assert_eq!(names.len(), before, "two samples share a name: {names:?}");

    let mut table = String::new();
    for (name, bytes) in &found {
        writeln!(
            table,
            "    (\n        \"{name}\",\n        \"{bytes}\",\n    ),"
        )
        .unwrap();
    }
    let pinned: Vec<(String, String)> = PINNED
        .iter()
        .map(|(name, bytes)| ((*name).to_string(), (*bytes).to_string()))
        .collect();
    assert!(
        found == pinned,
        "\nThe messages between nodes no longer match their pinned bytes in tests/wire_format.rs.\n\
         A message older nodes read must keep its bytes, and a new one is appended, with \n\
         its sample, its bytes and a test of what an older node's decoder makes of them \n\
         (see \"Messages between nodes\" in the README). Each sample's bytes now:\n\n{table}"
    );

    // Every pinned message decodes, and encodes back to the same bytes.
    for (name, bytes) in &pinned {
        let bytes = hex::decode(bytes).unwrap();
        let decoded = ReplicationMessage::decode(&bytes)
            .unwrap_or_else(|e| panic!("{name}: the pinned bytes don't decode: {e}"));
        assert_eq!(decoded.request_id, REQUEST_ID, "{name}");
        assert_eq!(&self::name(&decoded.body), name);
        assert_eq!(
            encode(decoded.body),
            bytes,
            "{name}: decoded and encoded again"
        );
    }
}

/// A varint, as postcard writes integers.
fn varint(mut value: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let low = u8::try_from(value & 0x7F).unwrap();
        value >>= 7;
        if value == 0 {
            bytes.push(low);
            return bytes;
        }
        bytes.push(low | 0x80);
    }
}

/// The error decoding `bytes` gives, or "decoded" if it decodes.
fn decode_error(bytes: &[u8]) -> String {
    ReplicationMessage::decode(bytes).map_or_else(|e| e.to_string(), |_| "decoded".to_string())
}

/// The variants older nodes know of each message and reply, and nothing
/// after them: the next variant index of each is one no node knows yet. Each
/// case is a message cut short right after a variant index: the last known
/// index then runs out of bytes, and the next is unknown.
#[test]
fn the_variants_are_the_pinned_ones() {
    let id = varint(REQUEST_ID);
    let frame = |parts: &[&[u8]]| -> Vec<u8> {
        let mut bytes = id.clone();
        for part in parts {
            bytes.extend_from_slice(part);
        }
        bytes
    };
    let challenge = varint(9);
    // (what, the bytes up to the variant index, how many variants it has)
    let cases: Vec<(&str, Vec<u8>, u8)> = vec![
        // 23 variants up to ds.15, then paged sync's two, then the "full" notice.
        ("ReplicationMessageBody", frame(&[]), 26),
        ("FreshReplicationResponse", frame(&[&[1]]), 2),
        ("FetchResponse", frame(&[&[8]]), 3),
        ("AuditResponse", frame(&[&[10]]), 3),
        ("SubtreeAuditResponse", frame(&[&[12]]), 3),
        (
            "RejectKind, in SubtreeAuditResponse::Rejected",
            frame(&[&[12, 2], &challenge]),
            3,
        ),
        ("SubtreeSliceResponse", frame(&[&[14]]), 3),
        (
            "SubtreeSliceItem, in SubtreeSliceResponse::Items",
            frame(&[&[14, 0], &challenge, &[1]]),
            3,
        ),
        (
            "RejectKind, in SubtreeSliceResponse::Rejected",
            frame(&[&[14, 2], &challenge]),
            3,
        ),
        ("GetCommitmentByPinResponse", frame(&[&[16]]), 2),
        ("NeighborSyncPageResponse", frame(&[&[24]]), 2),
    ];
    for (what, prefix, count) in cases {
        let with = |index: u8| {
            let mut bytes = prefix.clone();
            bytes.push(index);
            bytes
        };
        assert_eq!(
            decode_error(&with(count - 1)),
            format!("replication deserialization failed: {CUT_SHORT}"),
            "{what}: variant {} is its last, and its fields are missing",
            count - 1
        );
        assert_eq!(
            decode_error(&with(count)),
            format!("replication deserialization failed: {UNKNOWN_VARIANT}"),
            "\n{what} has a variant {count}, which nodes up to ds.15 can't decode. A new message \n\
             between nodes is appended to ReplicationMessageBody and sent only to peers known to \n\
             read it; a reply older nodes receive never gets a new variant (\"Messages between \n\
             nodes\" in the README). Pin the new variant here and in \n\
             every_message_between_nodes_keeps_its_bytes, with a test of what an older node's \n\
             decoder makes of it.\n"
        );
    }
}
