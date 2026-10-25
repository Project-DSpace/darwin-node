//! The line every hourly commitment tick logs, rotated or not: the number of
//! records the current commitment commits to (K), and whether the tick signed
//! a new commitment. A file of its own, which upstream doesn't have, so a
//! merge can't drop it while resolving a conflict in the engine's tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::{CommitmentTick, COMMITMENT_TICK_LINE};
use crate::replication::commitment::pointer_leaf_hash;
use crate::replication::commitment_state::BuiltCommitment;
use saorsa_pqc::api::sig::ml_dsa_65;
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::writer::{MakeWriter, MutexGuardWriter};
use tracing_subscriber::prelude::*;

/// A commitment to `chunks` chunks and `pointers` pointers.
fn built(chunks: u8, pointers: u8) -> BuiltCommitment {
    let (pk, sk) = ml_dsa_65().generate_keypair().unwrap();
    let mut entries: Vec<([u8; 32], [u8; 32])> = (1..=chunks).map(|i| ([i; 32], [i; 32])).collect();
    entries.extend((1..=pointers).map(|i| {
        let address = [100 + i; 32];
        (address, pointer_leaf_hash(&address))
    }));
    BuiltCommitment::build(entries, &[0xAB; 32], &sk, &pk.to_bytes()).unwrap()
}

#[derive(Clone, Default)]
struct Sink(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for Sink {
    type Writer = MutexGuardWriter<'a, Vec<u8>>;

    fn make_writer(&'a self) -> Self::Writer {
        self.0.make_writer()
    }
}

/// The text lines `tick` logs.
fn logged(tick: &CommitmentTick) -> String {
    let sink = Sink::default();
    let layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(sink.clone());
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || tick.log(COMMITMENT_TICK_LINE));
    let bytes = sink.0.lock().unwrap().clone();
    String::from_utf8(bytes).unwrap()
}

#[test]
fn every_tick_logs_its_key_count_and_whether_it_rotated() {
    let commitment = built(5, 1);
    let hash = hex::encode(commitment.hash());

    let unchanged = CommitmentTick::unchanged(&commitment);
    assert_eq!(unchanged.key_count, 6, "chunks and pointers");
    assert_eq!(unchanged.pointers, 1);
    assert!(!unchanged.rotated);
    let line = logged(&unchanged);
    assert_eq!(line.lines().count(), 1, "{line}");
    assert!(line.contains(" INFO "), "{line}");
    assert!(
        line.contains(&format!(
            "Storage commitment tick key_count=6 pointers=1 rotated=false hash={hash}"
        )),
        "{line}"
    );

    let rotated = CommitmentTick {
        rotated: true,
        ..CommitmentTick::unchanged(&commitment)
    };
    assert!(logged(&rotated).contains("key_count=6 pointers=1 rotated=true"));

    let none = logged(&CommitmentTick::NONE);
    assert!(
        none.contains("Storage commitment tick key_count=0 pointers=0 rotated=false hash=none"),
        "{none}"
    );
}
