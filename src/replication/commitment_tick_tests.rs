//! The line every hourly commitment tick logs, rotated or not: the number of
//! records the current commitment commits to (K), whether the tick signed a
//! new commitment, and the weight total (m) the node quotes clients that pay
//! by size at. A file of its own, which upstream doesn't have, so a
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

    assert!(line.contains(" weight_total=unknown"), "{line}");

    let rotated = CommitmentTick {
        rotated: true,
        ..CommitmentTick::unchanged(&commitment)
    };
    assert!(logged(&rotated).contains("key_count=6 pointers=1 rotated=true"));

    let none = logged(&CommitmentTick::NONE);
    assert!(
        none.contains(
            "Storage commitment tick key_count=0 pointers=0 rotated=false hash=none weight_total=0"
        ),
        "{none}"
    );
}

#[test]
fn the_tick_logs_the_weight_total_once_it_is_known() {
    let commitment = built(5, 1);
    // Five full chunks and a pointer: 5 + 259/1,024, rounded up.
    let m = commitment.set_weight_total(&[Some(4 * 1024 * 1024); 5]);
    assert_eq!(m, 6);
    let line = logged(&CommitmentTick::unchanged(&commitment));
    assert!(line.contains(" weight_total=6"), "{line}");
    let tiny = built(8, 0);
    assert_eq!(tiny.set_weight_total(&[Some(1); 8]), 3);
    assert!(logged(&CommitmentTick::unchanged(&tiny)).contains("key_count=8 pointers=0"));
    assert!(logged(&CommitmentTick::unchanged(&tiny)).contains(" weight_total=3"));
}

/// A restarted node reloads its commitment without m, and the first tick,
/// whose root is unchanged, weighs it once from the stored lengths, so the
/// node quotes clients that pay by size at m. Later ticks with the same root
/// don't read the lengths again: weighing it against a store that holds
/// nothing leaves m as it was.
#[tokio::test]
async fn a_reloaded_commitment_is_weighed_once_and_never_read_again() {
    use super::weigh_commitment;
    use crate::payment::quote::CommitmentSource;
    use crate::replication::commitment_state::ResponderCommitmentState;
    use crate::storage::{ChunkStore, ChunkStoreConfig};

    let store_at = |dir: &tempfile::TempDir| ChunkStoreConfig {
        root_dir: dir.path().to_path_buf(),
        verify_on_read: true,
        disk_reserve: 0,
    };
    let dir = tempfile::tempdir().unwrap();
    let storage = ChunkStore::new(store_at(&dir)).await.unwrap();
    // Eight tiny chunks: m = ceil(8 × 259 / 1,024) = 3, against K = 8.
    let mut entries = Vec::new();
    for i in 0..8u8 {
        let content = vec![i; 100];
        let address = crate::client::compute_address(&content);
        storage.put(&address, &content).await.unwrap();
        entries.push((address, address));
    }
    let (pk, sk) = ml_dsa_65().generate_keypair().unwrap();
    let built = BuiltCommitment::build(entries, &[0xAB; 32], &sk, &pk.to_bytes()).unwrap();
    let before_restart = ResponderCommitmentState::new();
    before_restart.rotate(built);

    let state = ResponderCommitmentState::new();
    state.restore(&before_restart.snapshot());
    let current = state.current().expect("reloaded");
    assert_eq!(current.weight_total(), None);
    let binding = state.current_binding_snapshot().expect("binding");
    assert_eq!((binding.key_count, binding.weight_total), (8, None));

    weigh_commitment(&storage, &current).await;
    assert_eq!(current.weight_total(), Some(3));
    let binding = state.current_binding_for_quote().expect("binding");
    assert_eq!((binding.key_count, binding.weight_total), (8, Some(3)));

    let empty_dir = tempfile::tempdir().unwrap();
    let empty = ChunkStore::new(store_at(&empty_dir)).await.unwrap();
    weigh_commitment(&empty, &current).await;
    assert_eq!(
        current.weight_total(),
        Some(3),
        "an unchanged root keeps its m"
    );
}
