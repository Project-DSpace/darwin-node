//! Paid notifies of keys this node already knows, and of keys another notify
//! is handling: dropped only when the paid list holds the key, one handled at
//! a time per key, and never lost to another notify's failed write. The
//! payment checks read a chain served over JSON-RPC on 127.0.0.1
//! (`payment::settlement::mock`), so a test sees each chain read.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::payment::settlement::mock::{self, Bundle, Chain, Spec};
use std::time::Duration;
use tempfile::TempDir;

/// A paid list in a fresh folder.
async fn paid_list() -> (Arc<PaidList>, TempDir) {
    let dir = TempDir::new().unwrap();
    let list = Arc::new(PaidList::new(dir.path()).await.unwrap());
    (list, dir)
}

/// The notify a close-group peer sends for `bundle`'s chunk.
fn notify(bundle: &Bundle) -> protocol::PaidNotify {
    protocol::PaidNotify {
        key: bundle.address,
        proof_of_payment: bundle.proof(&[]),
    }
}

/// A chain on which `bundle`'s quote was paid, served by a provider that
/// takes `delay` over each answer, and a payment check reading it.
async fn paid_chain(bundle: &Bundle, delay: Duration) -> (mock::Provider, Arc<PaymentVerifier>) {
    let chain = Chain::shared();
    chain.lock().pay(mock::tx(1), vec![bundle.entry()]);
    let provider = mock::provider(
        &chain,
        Spec {
            delay,
            ..Spec::default()
        },
    )
    .await;
    let verifier = Arc::new(mock::verifier(&[&provider]));
    (provider, verifier)
}

/// "Known" is the paid list holding the key: one local read.
#[tokio::test]
async fn a_key_is_known_only_once_the_paid_list_holds_it() {
    let (paid_list, _dir) = paid_list().await;
    let key = [0x11; 32];
    assert!(!paid_notify_key_known(&paid_list, &key));
    paid_list.insert(&key).await.unwrap();
    assert!(paid_notify_key_known(&paid_list, &key));
}

/// A notify arriving after another's paid-list write failed records the
/// key, from the payment check's cache, with no chain read of its own.
#[tokio::test]
async fn a_notify_after_a_failed_paid_list_write_records_the_key() {
    let bundle = Bundle::new([0x21; 32]);
    let (provider, verifier) = paid_chain(&bundle, Duration::ZERO).await;
    let (paid_list, _dir) = paid_list().await;
    let shutdown = CancellationToken::new();

    paid_list.fail_next_insert_for_tests();
    record_paid_notify(&notify(&bundle), &paid_list, &verifier, &shutdown)
        .await
        .unwrap();
    assert!(
        !paid_notify_key_known(&paid_list, &bundle.address),
        "the first notify's write failed"
    );
    assert_eq!(provider.record_reads().len(), 1);

    // Not known, so not dropped: verified from the cache, and written.
    record_paid_notify(&notify(&bundle), &paid_list, &verifier, &shutdown)
        .await
        .unwrap();
    assert!(paid_notify_key_known(&paid_list, &bundle.address));
    assert_eq!(provider.record_reads().len(), 1, "no chain read of its own");
}

/// A client PUT whose payment verified but whose store then failed at the
/// full line leaves the key out of the paid list. A notify for it records
/// the key, from the payment check's cache, with no chain read.
#[tokio::test]
async fn a_notify_after_a_put_whose_store_failed_records_the_key() {
    let bundle = Bundle::new([0x31; 32]);
    let (provider, verifier) = paid_chain(&bundle, Duration::ZERO).await;
    let (paid_list, _dir) = paid_list().await;

    // The PUT's payment check, then a store that fails: nothing written to
    // the paid list.
    assert_eq!(
        verifier
            .verify_payment(
                &bundle.address,
                crate::payment::Held::Bytes(ant_protocol::chunk::MAX_CHUNK_SIZE),
                Some(&bundle.proof(&[])),
                VerificationContext::ClientPut
            )
            .await
            .unwrap(),
        crate::payment::PaymentStatus::PaymentVerified
    );
    assert!(!paid_notify_key_known(&paid_list, &bundle.address));

    record_paid_notify(
        &notify(&bundle),
        &paid_list,
        &verifier,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(paid_notify_key_known(&paid_list, &bundle.address));
    assert_eq!(provider.record_reads().len(), 1, "the PUT's read alone");
}

/// A key the payment check has cached but the paid list doesn't hold is
/// handled, not dropped, and costs no chain read.
#[tokio::test]
async fn a_key_cached_but_not_listed_costs_no_chain_read() {
    let bundle = Bundle::new([0x41; 32]);
    let (provider, verifier) = paid_chain(&bundle, Duration::ZERO).await;
    let (paid_list, _dir) = paid_list().await;
    verifier.cache_insert(bundle.address);
    assert!(!paid_notify_key_known(&paid_list, &bundle.address));

    record_paid_notify(
        &notify(&bundle),
        &paid_list,
        &verifier,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(paid_notify_key_known(&paid_list, &bundle.address));
    assert!(provider.received().is_empty(), "{:?}", provider.received());
}

/// Two notifies of one key at once: the second waits for the first, then
/// checks the paid list again. The first's write failed, so the second
/// records the key itself, from the cache, with no chain read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_notify_waiting_on_one_whose_write_failed_records_the_key() {
    let bundle = Bundle::new([0x51; 32]);
    let (provider, verifier) = paid_chain(&bundle, Duration::from_millis(300)).await;
    let (paid_list, _dir) = paid_list().await;
    let flights = Arc::new(PaidNotifyFlights::default());
    let shutdown = CancellationToken::new();
    paid_list.fail_next_insert_for_tests();

    let key = bundle.address;
    let first = async {
        let turn = paid_notify_turn(&flights, &paid_list, &key).await;
        assert!(matches!(turn, Some(PaidNotifyTurn::First { .. })));
        record_paid_notify(&notify(&bundle), &paid_list, &verifier, &shutdown)
            .await
            .unwrap();
        assert!(!paid_notify_key_known(&paid_list, &key), "its write failed");
    };
    let second = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let turn = paid_notify_turn(&flights, &paid_list, &key).await;
        assert!(
            matches!(turn, Some(PaidNotifyTurn::After)),
            "it waited, and the key wasn't there"
        );
        record_paid_notify(&notify(&bundle), &paid_list, &verifier, &shutdown)
            .await
            .unwrap();
    };
    tokio::join!(first, second);
    assert!(paid_notify_key_known(&paid_list, &key));
    assert_eq!(provider.record_reads().len(), 1);
}

/// Two notifies of one key at once, the first recording it: the second
/// waits for it and has nothing left to do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_notify_waiting_on_one_that_recorded_the_key_has_nothing_to_do() {
    let bundle = Bundle::new([0x61; 32]);
    let (provider, verifier) = paid_chain(&bundle, Duration::from_millis(300)).await;
    let (paid_list, _dir) = paid_list().await;
    let flights = Arc::new(PaidNotifyFlights::default());
    let shutdown = CancellationToken::new();

    let key = bundle.address;
    let first = async {
        let _turn = paid_notify_turn(&flights, &paid_list, &key).await.unwrap();
        record_paid_notify(&notify(&bundle), &paid_list, &verifier, &shutdown)
            .await
            .unwrap();
    };
    let second = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        paid_notify_turn(&flights, &paid_list, &key).await.is_none()
    };
    let ((), nothing_to_do) = tokio::join!(first, second);
    assert!(nothing_to_do);
    assert!(paid_notify_key_known(&paid_list, &key));
    assert_eq!(provider.record_reads().len(), 1);

    // Once the first has finished, a notify of another key goes at once.
    let other = [0x62; 32];
    assert!(matches!(
        paid_notify_turn(&flights, &paid_list, &other).await,
        Some(PaidNotifyTurn::First { .. })
    ));
}

/// A notify whose turn ends without recording the key, dropped or
/// cancelled, lets the one waiting on it go, and that one handles the key.
#[tokio::test]
async fn a_notify_dropped_mid_turn_lets_the_next_handle_the_key() {
    let (paid_list, _dir) = paid_list().await;
    let flights = Arc::new(PaidNotifyFlights::default());
    let key = [0x71; 32];
    let first = paid_notify_turn(&flights, &paid_list, &key).await;
    assert!(matches!(first, Some(PaidNotifyTurn::First { .. })));
    let waiting = tokio::spawn({
        let (flights, paid_list) = (Arc::clone(&flights), Arc::clone(&paid_list));
        async move { paid_notify_turn(&flights, &paid_list, &key).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished(), "it waits while the first lasts");
    drop(first);
    assert!(matches!(
        waiting.await.unwrap(),
        Some(PaidNotifyTurn::After)
    ));
}
