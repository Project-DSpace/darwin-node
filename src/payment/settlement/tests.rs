//! The payment reads, against the mock chain: through the pool, the queue,
//! the shared reads and the cache, as the payment check makes them.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::mock::{self, Act, Batches, Chain, Spec};
use super::*;

const QUOTE: [u8; 32] = [0x42; 32];
const PAYEE: [u8; 20] = [0x5a; 20];
const AMOUNT: u128 = 3_000_000;

fn prefix(payee: &[u8; 20]) -> [u8; 16] {
    let mut prefix = [0u8; 16];
    prefix.copy_from_slice(&payee[..16]);
    prefix
}

fn paid(payee: &[u8; 20], amount: u128) -> Settlement {
    Settlement {
        amount: Amount::from(amount),
        payee_prefix: prefix(payee),
    }
}

/// A chain on which `QUOTE` was paid to `PAYEE`.
fn chain_paying_quote() -> Arc<Mutex<Chain>> {
    let chain = Chain::shared();
    chain.lock().pay(mock::tx(1), vec![(QUOTE, PAYEE, AMOUNT)]);
    chain
}

/// The read of a record is the vault's `completedPayments` of the quote
/// hash, at the latest block: the call the vault's interface defines.
#[test]
fn a_record_read_is_the_vaults_completed_payments_of_the_quote_hash() {
    use alloy::sol_types::SolCall;
    use evmlib::contract::payment_vault::interface::IPaymentVault;

    assert_eq!(
        IPaymentVault::completedPaymentsCall::SELECTOR,
        COMPLETED_PAYMENTS_SELECTOR
    );
    let call = <IPaymentVault::completedPaymentsCall as SolCall>::new((QUOTE.into(),)).abi_encode();
    let request = record_request(7, &mock::VAULT, &QUOTE);
    assert_eq!(request["method"], "eth_call");
    assert_eq!(request["id"], 7);
    assert_eq!(
        request["params"][0]["to"],
        format!("0x{}", hex::encode(mock::VAULT))
    );
    assert_eq!(
        request["params"][0]["input"],
        format!("0x{}", hex::encode(call))
    );
    assert_eq!(request["params"][1], "latest");
}

/// A record is the payee's leading 16 bytes and the amount, each in a word.
/// An answer without the payee, or not shaped as the record, is no record:
/// it is never read as one paying the quote.
#[test]
fn a_record_holds_its_payee_and_its_amount_and_nothing_less() {
    let mut words = [0u8; 64];
    words[..16].copy_from_slice(&prefix(&PAYEE));
    words[48..].copy_from_slice(&AMOUNT.to_be_bytes());
    let record = json!(format!("0x{}", hex::encode(words)));
    assert_eq!(decode_record(&record), Ok(paid(&PAYEE, AMOUNT)));

    let amount_alone = json!(format!("0x{}", hex::encode(&words[32..])));
    assert!(decode_record(&amount_alone).is_err());
    let mut padded = words;
    padded[20] = 1;
    assert!(decode_record(&json!(format!("0x{}", hex::encode(padded)))).is_err());
    assert!(decode_record(&json!("0x")).is_err());
    assert!(decode_record(&Value::Null).is_err());
    assert!(decode_record(&json!(format!("0x{}00", hex::encode(words)))).is_err());

    let empty = json!(format!("0x{}", hex::encode([0u8; 64])));
    let empty = decode_record(&empty).unwrap();
    assert!(empty.is_empty() && !empty.is_settled());
}

/// Only payments the vault recorded are kept: an empty record isn't, nor a
/// receipt the RPC didn't know, nor a read that failed.
#[tokio::test]
async fn only_positive_settled_facts_are_kept() {
    let chain = chain_paying_quote();
    let good = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&good]);

    let unpaid = [0x24; 32];
    let empty = reads.settlement(unpaid, Lane::Put).await.unwrap();
    assert!(empty.value.is_empty());
    assert_eq!(reads.fact(&unpaid), None, "an empty record is no fact");

    let unknown = reads.receipt(mock::tx(9), Lane::Put, false).await.unwrap();
    assert_eq!(unknown.value, None);
    assert!(
        reads.receipts.lock().is_empty(),
        "a missing receipt isn't kept"
    );

    let down = mock::provider(&chain, Spec::acting(|_| Act::Busy)).await;
    let failing = mock::reads(&[&down]);
    assert!(failing.settlement(QUOTE, Lane::Put).await.is_err());
    assert_eq!(failing.fact(&QUOTE), None, "a failed read is no fact");

    let read = reads.settlement(QUOTE, Lane::Put).await.unwrap();
    assert_eq!(read.value, paid(&PAYEE, AMOUNT));
    assert_eq!(reads.fact(&QUOTE), Some(paid(&PAYEE, AMOUNT)));
}

/// One receipt read fills the facts of every payment the vault recorded in
/// the transaction, so a 64-chunk wave's payments cost one read.
#[tokio::test]
async fn one_receipt_fills_the_facts_of_a_whole_wave() {
    let chain = Chain::shared();
    let entries: Vec<mock::Entry> = (0..64u8)
        .map(|i| ([i; 32], [i.wrapping_add(1); 20], 1_000 + u128::from(i)))
        .collect();
    chain.lock().pay(mock::tx(7), entries.clone());
    let provider = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&provider]);

    reads
        .fill_from_receipts(&[mock::tx(7), mock::tx(7)], Lane::Put)
        .await;
    for (quote_hash, payee, amount) in &entries {
        assert_eq!(reads.fact(quote_hash), Some(paid(payee, *amount)));
    }
    assert_eq!(provider.receipt_reads().len(), 1, "one read of the receipt");
    assert!(provider.record_reads().is_empty());

    // A receipt kept is not read again, and its facts come back with it.
    reads.facts.lock().clear();
    reads.fill_from_receipts(&[mock::tx(7)], Lane::Put).await;
    assert_eq!(provider.receipt_reads().len(), 1);
    assert!(reads.fact(&entries[5].0).is_some());

    // Only the vault's payment logs are kept of a receipt.
    let kept = reads.receipts.lock().get(&mock::tx(7)).cloned().unwrap();
    assert_eq!(kept.logs.len(), 64);
    assert!(kept.logs.iter().all(|log| log.address == mock::VAULT));
}

/// A proof's transactions are read once each, at most `MAX_PROOF_TXS` of
/// them, however many it names.
#[tokio::test]
async fn a_proofs_receipts_are_read_once_each_and_only_a_few() {
    let chain = chain_paying_quote();
    let provider = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&provider]);
    let mut named: Vec<[u8; 32]> = (0..200u32)
        .map(|i| {
            let mut tx_hash = [0xee; 32];
            tx_hash[..4].copy_from_slice(&i.to_be_bytes());
            tx_hash
        })
        .collect();
    named.insert(1, named[0]);
    reads.fill_from_receipts(&named, Lane::Background).await;
    let read: HashSet<String> = provider
        .receipt_reads()
        .iter()
        .map(|call| call.subject.clone())
        .collect();
    assert_eq!(provider.receipt_reads().len(), MAX_PROOF_TXS);
    assert_eq!(read.len(), MAX_PROOF_TXS, "each read once: {read:?}");
}

/// A log counts only from a receipt whose transaction succeeded, and only if
/// the vault emitted it, shaped as its payment event, with an amount its
/// record can hold.
#[tokio::test]
async fn only_the_vaults_logs_in_a_receipt_that_succeeded_are_facts() {
    let chain = Chain::shared();
    {
        let mut chain = chain.lock();
        // Reverted, though its receipt shows a vault log, as no honest RPC
        // shows one.
        let reverted = chain.pay(mock::tx(1), Vec::new());
        reverted.success = false;
        reverted
            .extra_logs
            .push(mock::payment_log(mock::VAULT, [1; 32], PAYEE, AMOUNT));
        // Another contract's log, shaped as the vault's.
        chain
            .pay(mock::tx(2), Vec::new())
            .extra_logs
            .push(mock::payment_log([0x99; 20], [2; 32], PAYEE, AMOUNT));
        // The vault's event with no amount, and one wider than 128 bits.
        let odd = chain.pay(mock::tx(3), Vec::new());
        odd.extra_logs
            .push(mock::payment_log(mock::VAULT, [3; 32], PAYEE, 0));
        let mut wide = mock::payment_log(mock::VAULT, [4; 32], PAYEE, 1);
        wide.topics[2][0] = 1;
        odd.extra_logs.push(wide);
        // A payee topic that isn't an address.
        let mut not_an_address = mock::payment_log(mock::VAULT, [5; 32], PAYEE, AMOUNT);
        not_an_address.topics[1][0] = 1;
        odd.extra_logs.push(not_an_address);
    }
    let provider = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&provider]);
    reads
        .fill_from_receipts(&[mock::tx(1), mock::tx(2), mock::tx(3)], Lane::Put)
        .await;
    for quote_hash in 1..=5u8 {
        assert_eq!(reads.fact(&[quote_hash; 32]), None, "quote {quote_hash}");
    }
}

/// Reads of one record at once share one call while it is in flight.
#[tokio::test]
async fn reads_of_one_record_at_once_share_one_call() {
    let chain = chain_paying_quote();
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(200),
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    let (first, second, third) = tokio::join!(
        reads.settlement(QUOTE, Lane::Put),
        reads.settlement(QUOTE, Lane::Put),
        reads.settlement(QUOTE, Lane::Put),
    );
    for read in [first, second, third] {
        assert_eq!(read.unwrap().value, paid(&PAYEE, AMOUNT));
    }
    assert_eq!(provider.record_reads().len(), 1);

    // Once it has come back, a new read is a new call: nothing is shared
    // after the read lands.
    let again = Chain::shared();
    let empty = mock::provider(&again, Spec::default()).await;
    let reads = mock::reads(&[&empty]);
    reads.settlement(QUOTE, Lane::Put).await.unwrap();
    reads.settlement(QUOTE, Lane::Put).await.unwrap();
    assert_eq!(empty.record_reads().len(), 2);
}

/// A caller that joined a read that came back empty reads for itself: the
/// record may hold the payment by then.
#[tokio::test]
async fn a_caller_that_joined_an_empty_read_reads_for_itself() {
    let chain = Chain::shared();
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(300),
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    let leader = reads.settlement(QUOTE, Lane::Put);
    let follower = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let read = reads.settlement(QUOTE, Lane::Put);
        // The payment lands while the shared read is in flight.
        tokio::time::sleep(Duration::from_millis(100)).await;
        chain.lock().pay(mock::tx(1), vec![(QUOTE, PAYEE, AMOUNT)]);
        read.await
    };
    let (leader, follower) = tokio::join!(leader, follower);
    assert!(leader.unwrap().value.is_empty(), "the shared read: empty");
    assert_eq!(
        follower.unwrap().value,
        paid(&PAYEE, AMOUNT),
        "the caller that joined it read for itself"
    );
    let calls = provider.record_reads();
    assert_eq!(calls.len(), 2, "{calls:?}");
}

/// A caller that joined a read that failed reads for itself, and is never
/// refused for another's failure.
#[tokio::test]
async fn a_caller_that_joined_a_failed_read_reads_for_itself() {
    let chain = chain_paying_quote();
    // The shared read's two rounds are answered busy, later calls answered.
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(100),
            ..Spec::acting(|n| if n <= 2 { Act::Busy } else { Act::Answer })
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    let (leader, follower) = tokio::join!(reads.settlement(QUOTE, Lane::Put), async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        reads.settlement(QUOTE, Lane::Put).await
    });
    assert!(leader.is_err(), "the shared read failed: {leader:?}");
    assert_eq!(follower.unwrap().value, paid(&PAYEE, AMOUNT));
    assert_eq!(provider.record_reads().len(), 3);
}

/// A shared read that times out is not taken on either: the caller that
/// joined it reads for itself.
#[tokio::test]
async fn a_caller_that_joined_a_timed_out_read_reads_for_itself() {
    let chain = chain_paying_quote();
    let provider = mock::provider(
        &chain,
        Spec::acting(|n| if n <= 2 { Act::Stall } else { Act::Answer }),
    )
    .await;
    let reads = mock::reads(&[&provider]);
    let started = Instant::now();
    let (leader, follower) = tokio::join!(reads.settlement(QUOTE, Lane::Put), async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        reads.settlement(QUOTE, Lane::Put).await
    });
    assert!(leader.is_err(), "both rounds timed out: {leader:?}");
    assert_eq!(follower.unwrap().value, paid(&PAYEE, AMOUNT));
    assert!(
        started.elapsed() >= PUT_CALL_TIMEOUT * 2,
        "{:?}",
        started.elapsed()
    );
}

/// A read whose task is dropped before it lands is taken off the reads in
/// flight, and those who joined it read for themselves.
#[tokio::test]
async fn a_read_dropped_before_it_lands_leaves_those_who_joined_to_read() {
    let flights: Arc<Flights<RecordRead>> = Flights::new();
    let Joined::Leader(landing, _) = flights.join(QUOTE) else {
        panic!("the first to join leads");
    };
    let Joined::Follower(follower) = flights.join(QUOTE) else {
        panic!("the second follows");
    };
    drop(landing);
    assert_eq!(shared(follower).await, None, "dropped, not landed");
    assert!(
        matches!(flights.join(QUOTE), Joined::Leader(..)),
        "the dropped read is no longer joined"
    );

    // Landed, a read's result goes to every caller that joined it.
    let flights: Arc<Flights<u32>> = Flights::new();
    let Joined::Leader(landing, leader) = flights.join(QUOTE) else {
        panic!("the first to join leads");
    };
    let Joined::Follower(follower) = flights.join(QUOTE) else {
        panic!("the second follows");
    };
    landing.land(7);
    assert!(
        matches!(flights.join(QUOTE), Joined::Leader(..)),
        "a landed read is no longer joined"
    );
    assert_eq!(shared(leader).await, Some(7));
    assert_eq!(shared(follower).await, Some(7));
}

/// Through `settlement`: a read whose caller goes away still finishes and
/// keeps the payment as a fact, and a caller that joined it gets it.
#[tokio::test]
async fn a_read_whose_caller_goes_away_still_lands() {
    let chain = chain_paying_quote();
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(300),
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    let first = tokio::spawn({
        let reads = Arc::clone(&reads);
        async move { reads.settlement(QUOTE, Lane::Put).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let joined = tokio::spawn({
        let reads = Arc::clone(&reads);
        async move { reads.settlement(QUOTE, Lane::Put).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    first.abort();
    assert_eq!(joined.await.unwrap().unwrap().value, paid(&PAYEE, AMOUNT));
    assert_eq!(reads.fact(&QUOTE), Some(paid(&PAYEE, AMOUNT)));
    assert_eq!(provider.record_reads().len(), 1);
}

/// A send of queued reads that panics frees its lane: the reads it carried
/// are told at once that they were dropped, and the next read in the lane is
/// sent.
#[tokio::test]
async fn a_send_of_queued_reads_that_panics_frees_its_lane() {
    let chain = chain_paying_quote();
    let provider = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&provider]);

    reads.panic_next_send.store(true, Ordering::SeqCst);
    let dropped = tokio::time::timeout(
        Duration::from_secs(5),
        reads.queued_record(QUOTE, Lane::Put),
    )
    .await
    .expect("told at once");
    assert!(
        matches!(&dropped, Err(why) if why == "the read was dropped"),
        "{dropped:?}"
    );
    assert!(!reads.queues[Lane::Put.index()].lock().in_flight);

    let read = tokio::time::timeout(Duration::from_secs(5), reads.settlement(QUOTE, Lane::Put))
        .await
        .expect("the lane is free")
        .unwrap();
    assert_eq!(read.value, paid(&PAYEE, AMOUNT));
    assert_eq!(provider.record_reads().len(), 1);
}

/// Records read again later come back in the order asked.
#[tokio::test]
async fn records_read_again_later_come_back_in_order() {
    let chain = chain_paying_quote();
    let provider = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&provider]);

    let unpaid = [0x24; 32];
    let mut again = reads.settlements_again_later(
        vec![Some((unpaid, 0)), None, Some((QUOTE, 0))],
        Lane::Put,
        Duration::from_millis(100),
    );
    assert!(again
        .recv()
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .value
        .is_empty());
    assert!(again.recv().await.unwrap().is_none());
    let read = again.recv().await.unwrap().unwrap().unwrap();
    assert_eq!(read.value, paid(&PAYEE, AMOUNT));
    assert!(again.recv().await.is_none(), "nothing more");
    assert_eq!(provider.record_reads().len(), 2);
}

/// Records read again later are read and kept as facts even when their
/// caller has gone during the pause, as when a PUT's deadline falls.
#[tokio::test]
async fn records_read_again_later_land_when_their_caller_has_gone() {
    let chain = chain_paying_quote();
    let provider = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&provider]);

    drop(reads.settlements_again_later(
        vec![Some((QUOTE, 0))],
        Lane::Put,
        Duration::from_millis(100),
    ));
    let started = Instant::now();
    while reads.fact(&QUOTE).is_none() {
        assert!(started.elapsed() < Duration::from_secs(5), "never kept");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(reads.fact(&QUOTE), Some(paid(&PAYEE, AMOUNT)));
}

/// Reads made while one is in flight wait for it, then go as one batch.
#[tokio::test]
async fn reads_queued_while_one_is_in_flight_go_as_one_batch() {
    let chain = Chain::shared();
    let quotes: Vec<[u8; 32]> = (1..=11u8).map(|i| [i; 32]).collect();
    chain.lock().pay(
        mock::tx(1),
        quotes
            .iter()
            .map(|quote_hash| (*quote_hash, PAYEE, AMOUNT + u128::from(quote_hash[0])))
            .collect(),
    );
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(300),
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    let first = reads.settlement(quotes[0], Lane::Put);
    let rest = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        futures::future::join_all(quotes[1..].iter().map(|q| reads.settlement(*q, Lane::Put))).await
    };
    let (first, rest) = tokio::join!(first, rest);
    assert_eq!(first.unwrap().value, paid(&PAYEE, AMOUNT + 1));
    for (quote_hash, read) in quotes[1..].iter().zip(rest) {
        assert_eq!(
            read.unwrap().value,
            paid(&PAYEE, AMOUNT + u128::from(quote_hash[0]))
        );
    }
    let calls = provider.record_reads();
    assert_eq!(
        provider.requests(),
        2,
        "one call, then one batch: {calls:?}"
    );
    assert_eq!(calls[0].batch, 1);
    assert!(calls[1..].iter().all(|call| call.batch == 10), "{calls:?}");
}

/// Ten reads queued for a provider that refuses batches: the batch is
/// refused once, and every read is made on its own, as are later ones for a
/// while.
#[tokio::test]
async fn a_provider_refusing_batches_is_read_call_by_call() {
    let chain = Chain::shared();
    let quotes: Vec<[u8; 32]> = (1..=11u8).map(|i| [i; 32]).collect();
    chain.lock().pay(
        mock::tx(1),
        quotes.iter().map(|q| (*q, PAYEE, AMOUNT)).collect(),
    );
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(200),
            batches: Batches::Refuse,
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    let first = reads.settlement(quotes[0], Lane::Put);
    let rest = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        futures::future::join_all(quotes[1..].iter().map(|q| reads.settlement(*q, Lane::Put))).await
    };
    let (first, rest) = tokio::join!(first, rest);
    assert!(first.is_ok());
    assert!(rest.into_iter().all(|read| read.is_ok()));
    let batches = provider
        .record_reads()
        .iter()
        .filter(|call| call.batch > 1)
        .map(|call| call.request)
        .collect::<HashSet<_>>()
        .len();
    assert_eq!(batches, 1, "one batch tried");
    let alone = provider
        .record_reads()
        .iter()
        .filter(|call| call.batch == 1)
        .count();
    assert_eq!(alone, 11, "the first read, and each of the ten on its own");

    // Reads queued again go one by one, not as another refused batch.
    let later: Vec<[u8; 32]> = (20..=25u8).map(|i| [i; 32]).collect();
    let first = reads.settlement(later[0], Lane::Put);
    let rest = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        futures::future::join_all(later[1..].iter().map(|q| reads.settlement(*q, Lane::Put))).await
    };
    let _ = tokio::join!(first, rest);
    let batches_now = provider
        .record_reads()
        .iter()
        .filter(|call| call.batch > 1)
        .map(|call| call.request)
        .collect::<HashSet<_>>()
        .len();
    assert_eq!(batches_now, 1, "no batch sent to it again");
}

/// Calls a batch's answer leaves out, as a provider capping batches does,
/// are read on their own.
#[tokio::test]
async fn calls_a_batch_answer_leaves_out_are_read_on_their_own() {
    let chain = Chain::shared();
    let quotes: Vec<[u8; 32]> = (1..=11u8).map(|i| [i; 32]).collect();
    chain.lock().pay(
        mock::tx(1),
        quotes.iter().map(|q| (*q, PAYEE, AMOUNT)).collect(),
    );
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(200),
            batches: Batches::Cap(3),
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    let first = reads.settlement(quotes[0], Lane::Put);
    let rest = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        futures::future::join_all(quotes[1..].iter().map(|q| reads.settlement(*q, Lane::Put))).await
    };
    let (first, rest) = tokio::join!(first, rest);
    assert_eq!(first.unwrap().value, paid(&PAYEE, AMOUNT));
    for read in rest {
        assert_eq!(read.unwrap().value, paid(&PAYEE, AMOUNT));
    }
    let alone = provider
        .record_reads()
        .iter()
        .filter(|call| call.batch == 1)
        .count();
    assert_eq!(alone, 1 + 7, "the first, and the seven left out");
}

/// An answer without the payee is no record, alone or in a batch: the read
/// fails, and never passes for a payment.
#[tokio::test]
async fn a_record_answered_without_its_payee_is_no_record() {
    let chain = chain_paying_quote();
    let provider = mock::provider(
        &chain,
        Spec {
            short_records: true,
            delay: Duration::from_millis(100),
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    let other = [0x43; 32];
    let (alone, batched) = tokio::join!(reads.settlement(QUOTE, Lane::Put), async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        futures::future::join_all([
            reads.settlement(other, Lane::Put),
            reads.settlement([0x44; 32], Lane::Put),
        ])
        .await
    });
    let error = alone.unwrap_err();
    assert!(error.contains("not the payee and the amount"), "{error}");
    for read in batched {
        assert!(read.is_err(), "{read:?}");
    }
    assert_eq!(reads.fact(&QUOTE), None);
}

/// On a store's path, a call the first RPC hasn't answered after
/// `PUT_SECOND_RPC_AFTER` goes to the next as well, and the first good
/// answer counts. The next read starts with the RPC that answered.
#[tokio::test]
async fn a_slow_rpcs_call_goes_to_the_next_as_well_on_a_stores_path() {
    let chain = chain_paying_quote();
    let stalled = mock::provider(&chain, Spec::stalled()).await;
    let good = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&stalled, &good]);

    let started = Instant::now();
    let read = reads.settlement(QUOTE, Lane::Put).await.unwrap();
    let took = started.elapsed();
    assert_eq!(read.value, paid(&PAYEE, AMOUNT));
    assert_eq!(read.rpc, 1, "answered by the second RPC");
    assert!(took >= PUT_SECOND_RPC_AFTER, "{took:?}");

    reads.settlement([0x77; 32], Lane::Put).await.unwrap();
    assert_eq!(stalled.record_reads().len(), 1, "not asked first again");
    assert_eq!(good.record_reads().len(), 2);
}

/// A call left unanswered when another RPC answered first, after the time a
/// store's read waits before asking the next RPC as well, counts as a failed
/// call and holds its RPC off, as one that timed out would: a stalled first
/// RPC shows in the figures, and isn't asked first again.
#[tokio::test]
async fn a_call_left_unanswered_by_a_stalled_rpc_is_counted_and_held_off() {
    use crate::payment::traffic::counts;

    let chain = chain_paying_quote();
    let stalled = mock::provider(&chain, Spec::stalled()).await;
    let good = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&stalled, &good]);
    let errors = counts(EvmRpcCall::CompletedPayments).1;
    let read = reads.settlement(QUOTE, Lane::Put).await.unwrap();
    assert_eq!(read.rpc, 1);
    assert!(reads.pool.rpcs()[0].held_off(Instant::now()));
    assert!(!reads.pool.rpcs()[1].held_off(Instant::now()));
    assert!(counts(EvmRpcCall::CompletedPayments).1 > errors);
}

/// A call that only lost a race, started less than the store's wait before
/// the other RPC answered, is neither counted as failed nor held off.
#[tokio::test]
async fn a_call_that_only_lost_a_race_is_not_held_off() {
    let chain = chain_paying_quote();
    // The first answers 400 ms after the second is asked as well; the second
    // takes a second, so its call is left unanswered 400 ms old.
    let slow = mock::provider(
        &chain,
        Spec {
            delay: PUT_SECOND_RPC_AFTER + Duration::from_millis(400),
            ..Spec::default()
        },
    )
    .await;
    let late = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_secs(1),
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&slow, &late]);
    assert_eq!(reads.settlement(QUOTE, Lane::Put).await.unwrap().rpc, 0);
    assert_eq!(late.record_reads().len(), 1, "the second was asked as well");
    assert!(!reads.pool.rpcs()[0].held_off(Instant::now()));
    assert!(!reads.pool.rpcs()[1].held_off(Instant::now()));
}

/// Nobody waits on a background read, so its calls get their whole time,
/// and a slow RPC isn't doubled up.
#[tokio::test]
async fn a_background_read_waits_for_a_slow_rpc() {
    let chain = chain_paying_quote();
    let slow = mock::provider(
        &chain,
        Spec {
            delay: PUT_SECOND_RPC_AFTER + Duration::from_millis(800),
            ..Spec::default()
        },
    )
    .await;
    let good = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&slow, &good]);
    let read = reads.settlement(QUOTE, Lane::Background).await.unwrap();
    assert_eq!(read.rpc, 0);
    assert!(good.record_reads().is_empty());
}

/// An RPC that answered busy is asked after the others while it is held
/// off, and is still asked when they fail.
#[tokio::test]
async fn an_rpc_that_answered_busy_is_asked_after_the_others() {
    let chain = chain_paying_quote();
    // The first answers, then is busy, then answers again.
    let first = mock::provider(
        &chain,
        Spec::acting(|n| if n == 2 { Act::Busy } else { Act::Answer }),
    )
    .await;
    // The second drops its first call, then answers.
    let second = mock::provider(
        &chain,
        Spec::acting(|n| if n == 1 { Act::Drop } else { Act::Answer }),
    )
    .await;
    let reads = mock::reads(&[&first, &second]);
    assert_eq!(reads.settlement([1; 32], Lane::Put).await.unwrap().rpc, 0);

    // The first is busy and held off, the second drops the call: the read's
    // second round asks the second first, the first answered last but held
    // off.
    let read = reads.settlement([2; 32], Lane::Put).await.unwrap();
    assert_eq!(read.rpc, 1);
    let order: Vec<(usize, Instant)> = first
        .record_reads()
        .iter()
        .map(|call| (0, call.at))
        .chain(second.record_reads().iter().map(|call| (1, call.at)))
        .filter(|(_, at)| *at > first.record_reads()[0].at)
        .collect::<Vec<_>>();
    let mut order = order;
    order.sort_by_key(|(_, at)| *at);
    let rpcs: Vec<usize> = order.iter().map(|(rpc, _)| *rpc).collect();
    assert_eq!(rpcs, [0, 1, 1], "busy, dropped, then the held-off one last");

    // With every RPC failing, the held-off one is still asked.
    let busy = mock::provider(&chain, Spec::acting(|_| Act::Busy)).await;
    let reads = mock::reads(&[&busy]);
    assert!(reads.settlement(QUOTE, Lane::Put).await.is_err());
    assert_eq!(busy.record_reads().len(), PAYMENT_READ_ROUNDS as usize);
}

/// A record read again goes on a fresh connection, to the RPC after the one
/// that answered empty; with one RPC, to that one on a fresh connection.
#[tokio::test]
async fn a_record_read_again_goes_on_a_fresh_connection_to_the_next_rpc() {
    let chain = chain_paying_quote();
    let first = mock::provider(&chain, Spec::default()).await;
    let second = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&first, &second]);
    reads.settlement([1; 32], Lane::Put).await.unwrap();
    let again = reads.settlement_again(QUOTE, Lane::Put, 0).await.unwrap();
    assert_eq!(again.rpc, 1);
    assert_eq!(again.value, paid(&PAYEE, AMOUNT));
    assert_eq!(second.record_reads().len(), 1);

    let only = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&only]);
    reads.settlement([1; 32], Lane::Put).await.unwrap();
    reads.settlement([2; 32], Lane::Put).await.unwrap();
    reads.settlement_again(QUOTE, Lane::Put, 0).await.unwrap();
    let connections: Vec<usize> = only.record_reads().iter().map(|c| c.connection).collect();
    assert_eq!(connections[0], connections[1], "pooled: {connections:?}");
    assert_ne!(connections[2], connections[0], "fresh: {connections:?}");
}

/// A receipt read again goes on a fresh connection, to the RPC after the
/// one that didn't know it, and isn't joined to a read in flight.
#[tokio::test]
async fn a_receipt_read_again_goes_on_a_fresh_connection_to_the_next_rpc() {
    let chain = chain_paying_quote();
    let first = mock::provider(&chain, Spec::default()).await;
    let second = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&first, &second]);
    let again = reads
        .receipt_again(mock::tx(1), Lane::Put, 0)
        .await
        .unwrap();
    assert_eq!(again.rpc, 1);
    assert!(again.value.is_some());
    assert_eq!(second.receipt_reads().len(), 1);
    assert!(first.receipt_reads().is_empty());
}

/// Every RPC stalled: a read on a store's path fails within its rounds of
/// short calls, and never waits on a background call's time.
#[tokio::test]
async fn every_rpc_stalled_fails_a_stores_read_within_its_rounds() {
    let chain = chain_paying_quote();
    let one = mock::provider(&chain, Spec::stalled()).await;
    let two = mock::provider(&chain, Spec::stalled()).await;
    let reads = mock::reads(&[&one, &two]);
    let started = Instant::now();
    assert!(reads.settlement(QUOTE, Lane::Put).await.is_err());
    let took = started.elapsed();
    // Each round: the first RPC, the second as well after its pause, and
    // both calls' time; then the next round after its pause.
    let bound = (PUT_SECOND_RPC_AFTER + PUT_CALL_TIMEOUT) * PAYMENT_READ_ROUNDS
        + PAYMENT_READ_BACKOFF
        + Duration::from_secs(3);
    assert!(took < bound, "{took:?}");
    assert!(took < BACKGROUND_CALL_TIMEOUT);
}

/// Each new kind of call is counted as its own kind in the RPC traffic
/// figures, answered or not: the receipt read, the batch of record reads,
/// and the block read.
#[tokio::test]
async fn each_new_kind_of_call_is_counted_apart() {
    use crate::payment::traffic::counts;

    let receipts = counts(EvmRpcCall::TransactionReceipt);
    let batches = counts(EvmRpcCall::CompletedPaymentsBatch);
    let blocks = counts(EvmRpcCall::BlockTimestamp);
    let chain = chain_paying_quote();
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(200),
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    reads.receipt(mock::tx(1), Lane::Put, false).await.unwrap();
    reads.block_timestamp(100, Lane::Put).await.unwrap();
    let (first, rest) = tokio::join!(reads.settlement([1; 32], Lane::Put), async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        futures::future::join_all([
            reads.settlement([2; 32], Lane::Put),
            reads.settlement([3; 32], Lane::Put),
        ])
        .await
    });
    assert!(first.is_ok() && rest.iter().all(Result::is_ok));
    assert!(counts(EvmRpcCall::TransactionReceipt).0 > receipts.0);
    assert!(counts(EvmRpcCall::CompletedPaymentsBatch).0 > batches.0);
    assert!(counts(EvmRpcCall::BlockTimestamp).0 > blocks.0);

    let busy = mock::provider(&chain, Spec::acting(|_| Act::Busy)).await;
    let failing = mock::reads(&[&busy]);
    let errors = counts(EvmRpcCall::TransactionReceipt).1;
    assert!(failing
        .receipt(mock::tx(1), Lane::Put, false)
        .await
        .is_err());
    assert!(counts(EvmRpcCall::TransactionReceipt).1 > errors);
}

/// The reads of `quotes`: the first on its own, the rest queued while it is
/// in flight, so they go together.
async fn queue_behind_one(reads: &Arc<SettlementReads>, quotes: &[[u8; 32]]) -> Vec<RecordRead> {
    let first = reads.settlement(quotes[0], Lane::Put);
    let rest = async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        futures::future::join_all(quotes[1..].iter().map(|q| reads.settlement(*q, Lane::Put))).await
    };
    let (first, mut rest) = tokio::join!(first, rest);
    rest.insert(0, first);
    rest
}

/// A chain on which each of `quotes` was paid to `PAYEE`.
fn chain_paying(quotes: &[[u8; 32]]) -> Arc<Mutex<Chain>> {
    let chain = Chain::shared();
    chain.lock().pay(
        mock::tx(1),
        quotes.iter().map(|q| (*q, PAYEE, AMOUNT)).collect(),
    );
    chain
}

/// A batch is never larger than `MAX_BATCH`: more queued reads go as more
/// batches. Robinhood Chain's own RPC answers a batch of more than about 55
/// calls with HTTP 429.
#[tokio::test]
async fn queued_reads_go_in_batches_of_at_most_max_batch() {
    let quotes: Vec<[u8; 32]> = (0..=(MAX_BATCH + 8))
        .map(|i| {
            let mut q = [0x31; 32];
            q[..8].copy_from_slice(&(i as u64).to_be_bytes());
            q
        })
        .collect();
    let chain = chain_paying(&quotes);
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(300),
            batches: Batches::TooManyOver(55),
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    for read in queue_behind_one(&reads, &quotes).await {
        assert_eq!(read.unwrap().value, paid(&PAYEE, AMOUNT));
    }
    let calls = provider.record_reads();
    assert!(
        calls.iter().all(|call| call.batch <= MAX_BATCH),
        "{calls:?}"
    );
    assert_eq!(calls.len(), quotes.len(), "each record read once");
    assert_eq!(MAX_BATCH, 32);
}

/// A batch answered with HTTP 429, as Robinhood Chain's own RPC answers one
/// over its cap, is a batch refused: its calls are read one at a time, and
/// later reads for that RPC go one at a time for a while.
#[tokio::test]
async fn a_batch_answered_too_many_requests_is_read_call_by_call() {
    let quotes: Vec<[u8; 32]> = (1..=6u8).map(|i| [i; 32]).collect();
    let chain = chain_paying(&quotes);
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(200),
            batches: Batches::TooManyOver(1),
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    for read in queue_behind_one(&reads, &quotes).await {
        assert_eq!(read.unwrap().value, paid(&PAYEE, AMOUNT));
    }
    let batches: HashSet<usize> = provider
        .record_reads()
        .iter()
        .filter(|call| call.batch > 1)
        .map(|call| call.request)
        .collect();
    assert_eq!(batches.len(), 1, "one batch tried");
    let alone = provider
        .record_reads()
        .iter()
        .filter(|call| call.batch == 1)
        .count();
    assert_eq!(alone, quotes.len(), "the first, and each of the rest alone");
}

/// A batch every call of which came back busy is a busy RPC: it is held off,
/// and the batch goes to the next RPC.
#[tokio::test]
async fn a_batch_answered_busy_call_by_call_goes_to_the_next_rpc() {
    let quotes: Vec<[u8; 32]> = (1..=6u8).map(|i| [i; 32]).collect();
    let chain = chain_paying(&quotes);
    let busy = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(200),
            batches: Batches::EachBusy,
            ..Spec::default()
        },
    )
    .await;
    let good = mock::provider(&chain, Spec::default()).await;
    let reads = mock::reads(&[&busy, &good]);
    let reads_made = queue_behind_one(&reads, &quotes).await;
    for read in &reads_made[1..] {
        let read = read.as_ref().unwrap();
        assert_eq!(read.value, paid(&PAYEE, AMOUNT));
        assert_eq!(read.rpc, 1, "answered by the next RPC");
    }
    assert!(
        good.record_reads().iter().any(|call| call.batch > 1),
        "the batch went to the next RPC: {:?}",
        good.record_reads()
    );
    assert!(reads.pool.rpcs()[0].held_off(Instant::now()));
}

/// A batch no RPC answers in time, while calls on their own are answered:
/// each read of it is made on its own, and every one is answered.
#[tokio::test]
async fn a_batch_no_rpc_answers_in_time_is_read_call_by_call() {
    let quotes: Vec<[u8; 32]> = (1..=11u8).map(|i| [i; 32]).collect();
    let chain = chain_paying(&quotes);
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(200),
            batches: Batches::Stall,
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&provider]);
    for read in queue_behind_one(&reads, &quotes).await {
        assert_eq!(read.unwrap().value, paid(&PAYEE, AMOUNT));
    }
    let calls = provider.record_reads();
    let batches: HashSet<usize> = calls
        .iter()
        .filter(|call| call.batch > 1)
        .map(|call| call.request)
        .collect();
    assert_eq!(batches.len(), PAYMENT_READ_ROUNDS as usize, "{calls:?}");
    let alone = calls.iter().filter(|call| call.batch == 1).count();
    assert_eq!(alone, quotes.len(), "the first, and each of the ten alone");
}

/// A batch every RPC answers busy is refused: its reads are not made one at
/// a time, which would only add to a busy RPC's load.
#[tokio::test]
async fn a_batch_every_rpc_answers_busy_is_not_read_call_by_call() {
    let quotes: Vec<[u8; 32]> = (1..=6u8).map(|i| [i; 32]).collect();
    let chain = chain_paying(&quotes);
    let busy = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(200),
            batches: Batches::EachBusy,
            ..Spec::default()
        },
    )
    .await;
    let reads = mock::reads(&[&busy]);
    let made = queue_behind_one(&reads, &quotes).await;
    assert!(made[0].is_ok(), "the first, on its own, answered");
    assert!(made[1..].iter().all(Result::is_err), "{made:?}");
    let alone = busy
        .record_reads()
        .iter()
        .filter(|call| call.batch == 1)
        .count();
    assert_eq!(alone, 1, "only the first went on its own");
}

/// Read-only, against the live chain: a past payment of the production
/// vault, through these reads, from each RPC a node reads payments through
/// by default, alone: the public RPCs built in
/// ([`crate::listings::listings_rpc_urls`]) and the node image's default
/// list (`RPC_URLS` in docker/Dockerfile). On each: the receipt, whose vault
/// payments become facts, and the block's time; each payment's record read on
/// its own, matching its fact, and read again on a fresh connection; the
/// records read together through the queue, as a batch; the records in one
/// batch of [`MAX_BATCH`] calls, unpaid hashes filling the rest; and the
/// receipt read again on a fresh connection. Every RPC is tried, and the
/// test fails naming each one that didn't agree.
///
/// Run by hand before a release that changes these reads, naming a payment
/// transaction of the vault:
/// `ANT_LIVE_PAYMENT_TX=0x… cargo test -p ant-node --lib live_reads -- --ignored --nocapture`
#[tokio::test]
#[ignore = "reads the live chain"]
#[allow(clippy::too_many_lines)]
async fn live_reads_of_a_past_payment_agree_on_every_default_rpc() {
    let tx_hash: [u8; 32] = hex::decode(
        std::env::var("ANT_LIVE_PAYMENT_TX")
            .expect("ANT_LIVE_PAYMENT_TX names a payment transaction")
            .trim_start_matches("0x"),
    )
    .unwrap()
    .try_into()
    .unwrap();
    let vault: [u8; 20] =
        hex::decode(crate::config::PRODUCTION_PAYMENT_VAULT.trim_start_matches("0x"))
            .unwrap()
            .try_into()
            .unwrap();
    let dockerfile = include_str!("../../../docker/Dockerfile");
    let image_rpcs = dockerfile
        .lines()
        .find_map(|line| line.trim().strip_prefix("RPC_URLS=\""))
        .and_then(|rest| rest.split('"').next())
        .expect("the image's default RPCs");
    let mut rpcs: Vec<String> =
        crate::listings::listings_rpc_urls("https://rpc.mainnet.chain.robinhood.com");
    rpcs.extend(image_rpcs.split_whitespace().map(str::to_string));
    let mut seen = HashSet::new();
    rpcs.retain(|rpc| seen.insert(rpc.trim_end_matches('/').to_string()));
    assert!(rpcs.len() >= 3, "{rpcs:?}");

    let mut failed = Vec::new();
    for rpc in rpcs {
        let url: reqwest::Url = rpc.parse().unwrap();
        let host = url.host_str().unwrap_or_default().to_string();
        match live_check(SettlementReads::new(vault, vec![url]), tx_hash, vault).await {
            Ok(summary) => println!("{host}: {summary}"),
            Err(why) => {
                println!("{host}: FAILED: {why}");
                failed.push(format!("{host}: {why}"));
            }
        }
    }
    assert!(failed.is_empty(), "{failed:#?}");
}

/// One RPC's part of the live check: what it agreed on, or where it didn't.
#[allow(clippy::too_many_lines)]
async fn live_check(
    reads: Arc<SettlementReads>,
    tx_hash: [u8; 32],
    vault: [u8; 20],
) -> Result<String, String> {
    let receipt = reads
        .receipt(tx_hash, Lane::Background, true)
        .await
        .map_err(|e| format!("receipt: {e}"))?
        .value
        .ok_or("the receipt is unknown")?;
    if !receipt.success {
        return Err("the transaction failed".into());
    }
    let payments = vault_payments(&receipt, &vault);
    if payments.is_empty() {
        return Err("no vault payment in the receipt".into());
    }
    let timestamp = reads
        .block_timestamp(receipt.block_number, Lane::Put)
        .await
        .map_err(|e| format!("block: {e}"))?
        .ok_or("the block is unknown")?;

    let facts: Vec<Settlement> = payments
        .iter()
        .map(|payment| Settlement {
            amount: Amount::from(payment.amount),
            payee_prefix: payment.payee_prefix(),
        })
        .collect();
    for (payment, fact) in payments.iter().zip(&facts) {
        if reads.fact(&payment.quote_hash) != Some(*fact) {
            return Err("a receipt's payment isn't kept as a fact".into());
        }
        let alone = reads
            .read_alone(payment.quote_hash, Lane::Put)
            .await
            .map_err(|e| format!("record: {e}"))?;
        if alone.value != *fact {
            return Err(format!("a record differs from the receipt: {alone:?}"));
        }
        let again = reads
            .settlement_again(payment.quote_hash, Lane::Put, 0)
            .await
            .map_err(|e| format!("record again: {e}"))?;
        if again.value != *fact {
            return Err(format!("a record read again differs: {again:?}"));
        }
    }

    let queued = futures::future::join_all(
        payments
            .iter()
            .map(|payment| reads.queued_record(payment.quote_hash, Lane::Put)),
    )
    .await;
    for (read, fact) in queued.into_iter().zip(&facts) {
        let read = read.map_err(|e| format!("queued record: {e}"))?;
        if read.value != *fact {
            return Err(format!("a queued record differs: {read:?}"));
        }
    }

    let mut quote_hashes: Vec<[u8; 32]> = payments.iter().map(|p| p.quote_hash).collect();
    while quote_hashes.len() < MAX_BATCH {
        let mut unpaid = [0xd5; 32];
        unpaid[..8].copy_from_slice(&(quote_hashes.len() as u64).to_be_bytes());
        quote_hashes.push(unpaid);
    }
    let body = Value::Array(
        quote_hashes
            .iter()
            .enumerate()
            .map(|(id, quote_hash)| record_request(id, &vault, quote_hash))
            .collect(),
    );
    let batch = reads
        .pool
        .call(&Call {
            body: &body,
            kind: EvmRpcCall::CompletedPaymentsBatch,
            expect: Expect::Batch,
            lane: Lane::Put,
            rounds: 1,
            again_after: None,
        })
        .await
        .map_err(|e| format!("a batch of {MAX_BATCH}: {e}"))?;
    let Value::Array(answers) = batch.value else {
        return Err("the batch's answer is not a list".into());
    };
    if answers.len() != MAX_BATCH {
        return Err(format!("{} of {MAX_BATCH} calls answered", answers.len()));
    }
    for answer in answers {
        let id = answer["id"]
            .as_u64()
            .and_then(|id| usize::try_from(id).ok())
            .ok_or("an answer without its id")?;
        let record = rpc_result(answer)
            .map_err(|e| format!("a batched call: {e}"))
            .and_then(|result| decode_record(&result))?;
        let agrees = match facts.get(id) {
            Some(fact) => record == *fact,
            None => record.is_empty(),
        };
        if !agrees {
            return Err(format!("batched call {id} differs: {record:?}"));
        }
    }

    let again = reads
        .receipt_again(tx_hash, Lane::Put, 0)
        .await
        .map_err(|e| format!("receipt again: {e}"))?
        .value
        .ok_or("the receipt read again is unknown")?;
    if again != receipt {
        return Err("the receipt read again differs".into());
    }
    Ok(format!(
        "the receipt, of block {} at {timestamp}, holds {} vault payment(s); records read \
         alone, again, queued and in a batch of {MAX_BATCH} agree with it",
        receipt.block_number,
        payments.len()
    ))
}
