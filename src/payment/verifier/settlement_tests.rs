//! The payment check through its real reads, against a chain served over
//! JSON-RPC on 127.0.0.1 (`settlement::mock`), not through the test override
//! of the vault's records: what the receipts of a proof's transactions may
//! settle, what only the per-quote read may refuse, and that a node reading
//! receipts reaches the verdicts of a node reading records alone.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::payment::settlement::mock::{self, Act, Bundle, Chain, Spec};
use crate::payment::settlement::CLIENT_STORE_WAIT;
use std::collections::HashMap;
use std::time::Duration;

/// The contexts a payment is checked in.
const CONTEXTS: [VerificationContext; 3] = [
    VerificationContext::ClientPut,
    VerificationContext::FreshReplication,
    VerificationContext::PaidListAdmission,
];

/// Check `bundle`'s payment, its proof naming `tx_hashes`.
async fn check(
    verifier: &PaymentVerifier,
    bundle: &Bundle,
    tx_hashes: &[[u8; 32]],
    context: VerificationContext,
) -> Result<PaymentStatus> {
    verifier
        .verify_payment(
            &bundle.address,
            Held::Bytes(ant_protocol::chunk::MAX_CHUNK_SIZE),
            Some(&bundle.proof(tx_hashes)),
            context,
        )
        .await
}

/// How many times the provider read each record.
fn reads_per_record(provider: &mock::Provider) -> HashMap<String, usize> {
    let mut reads = HashMap::new();
    for call in provider.record_reads() {
        *reads.entry(call.subject).or_insert(0) += 1;
    }
    reads
}

/// A whole 64-chunk wave, paid in one transaction, checked through an RPC a
/// block behind: at first it doesn't know the transaction and the records
/// read empty; read again a moment later, the payments are there. Every
/// chunk verifies, within the client's wait, and no record is read more
/// than twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wave_paid_through_an_rpc_a_block_behind_verifies_whole() {
    let chain = Chain::shared();
    let wave: Vec<Bundle> = (0..64u8).map(|i| Bundle::new([i; 32])).collect();
    chain
        .lock()
        .pay(mock::tx(0xAA), wave.iter().map(Bundle::entry).collect());
    let behind = mock::provider(
        &chain,
        Spec {
            behind: Duration::from_secs(1),
            ..Spec::default()
        },
    )
    .await;
    let verifier = mock::verifier(&[&behind]);

    let named = [mock::tx(0xAA)];
    let started = Instant::now();
    let outcomes = futures::future::join_all(
        wave.iter()
            .map(|chunk| check(&verifier, chunk, &named, VerificationContext::ClientPut)),
    )
    .await;
    let took = started.elapsed();
    for (chunk, outcome) in wave.iter().zip(outcomes) {
        assert_eq!(
            outcome.unwrap_or_else(|e| panic!("{}: {e}", hex::encode(chunk.address))),
            PaymentStatus::PaymentVerified
        );
    }
    assert!(took < CLIENT_STORE_WAIT, "{took:?}");
    let reads = reads_per_record(&behind);
    assert!(
        reads.values().all(|reads| *reads <= 2),
        "a record read more than twice: {reads:?}"
    );
    assert!(
        behind.receipt_reads().len() <= wave.len(),
        "{} receipt reads",
        behind.receipt_reads().len()
    );
}

/// Paid proofs whose receipts can't settle their quote, checked by the
/// per-quote read, as before: no transaction named; one the chain doesn't
/// know; the gateway's borrowed proof, naming a transaction that paid other
/// quotes; the browser's single hash, naming the upload's other transaction;
/// and a quote paid earlier and recovered, naming a later payment the vault
/// skipped. Each verifies, with one read of its record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paid_proofs_the_receipts_cant_settle_verify_through_the_record_read() {
    let chain = Chain::shared();
    let none = Bundle::new([0x01; 32]);
    let unknown = Bundle::new([0x02; 32]);
    let borrowed = Bundle::new([0x03; 32]);
    let single = Bundle::new([0x04; 32]);
    let recovered = Bundle::new([0x05; 32]);
    let other = Bundle::new([0x06; 32]);
    let another = Bundle::new([0x07; 32]);
    {
        let mut chain = chain.lock();
        chain.pay(mock::tx(0x10), vec![none.entry(), unknown.entry()]);
        // The gateway pays its own wave of other quotes; this one was paid
        // on its own.
        chain.pay(mock::tx(0x11), vec![other.entry()]);
        chain.pay(mock::tx(0x12), vec![borrowed.entry()]);
        // A browser's upload paid in two transactions, the proof naming the
        // first only.
        chain.pay(mock::tx(0x13), vec![another.entry()]);
        chain.pay(mock::tx(0x14), vec![single.entry()]);
        // Paid, then paid again when the upload was recovered: the vault
        // skips the repeat, with no log.
        chain.pay(mock::tx(0x15), vec![recovered.entry()]);
        chain.pay(mock::tx(0x16), vec![recovered.entry()]);
    }
    let cases: [(&str, &Bundle, Vec<[u8; 32]>, usize); 5] = [
        ("no transaction", &none, vec![], 0),
        ("an unknown transaction", &unknown, vec![mock::tx(0xEE)], 1),
        (
            "the gateway's borrowed proof",
            &borrowed,
            vec![mock::tx(0x11)],
            1,
        ),
        (
            "the browser's single hash",
            &single,
            vec![mock::tx(0x13)],
            1,
        ),
        (
            "a recovered quote paid earlier",
            &recovered,
            vec![mock::tx(0x16)],
            1,
        ),
    ];
    for (name, bundle, tx_hashes, receipt_reads) in &cases {
        for context in CONTEXTS {
            let provider = mock::provider(&chain, Spec::default()).await;
            let verifier = mock::verifier(&[&provider]);
            let outcome = check(&verifier, bundle, tx_hashes, context).await;
            assert_eq!(
                outcome.unwrap_or_else(|e| panic!("{name}, {context:?}: {e}")),
                PaymentStatus::PaymentVerified,
                "{name}, {context:?}"
            );
            assert_eq!(provider.record_reads().len(), 1, "{name}, {context:?}");
            assert_eq!(
                provider.receipt_reads().len(),
                *receipt_reads,
                "{name}, {context:?}"
            );
        }
    }
}

/// A quote someone else's dust payment recorded first: the client's payment
/// of it was skipped, with no log, so the receipt it names holds nothing for
/// it, and the record read refuses it.
#[tokio::test]
async fn an_occupied_quote_is_refused() {
    let chain = Chain::shared();
    let bundle = Bundle::new([0x21; 32]);
    {
        let mut chain = chain.lock();
        chain.pay(mock::tx(0x20), vec![(bundle.quote_hash(), [0xEE; 20], 1)]);
        chain.pay(mock::tx(0x21), vec![bundle.entry()]);
    }
    for context in CONTEXTS {
        let provider = mock::provider(&chain, Spec::default()).await;
        let verifier = mock::verifier(&[&provider]);
        let error = check(&verifier, &bundle, &[mock::tx(0x21)], context)
            .await
            .expect_err("an occupied quote is refused")
            .to_string();
        assert!(error.contains("redirected"), "{context:?}: {error}");
        assert_eq!(provider.record_reads().len(), 1, "{context:?}");
    }
}

/// A reverted transaction's receipt showing a vault log, and another
/// contract's log shaped as the vault's, are ignored: the record read
/// decides, refusing a quote nobody paid and taking one paid elsewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reverted_receipt_and_another_contracts_log_are_ignored() {
    let chain = Chain::shared();
    let unpaid = Bundle::new([0x31; 32]);
    let paid_elsewhere = Bundle::new([0x32; 32]);
    let foreign = Bundle::new([0x33; 32]);
    {
        let mut chain = chain.lock();
        let reverted = chain.pay(mock::tx(0x30), Vec::new());
        reverted.success = false;
        for bundle in [&unpaid, &paid_elsewhere] {
            reverted.extra_logs.push(mock::payment_log(
                mock::VAULT,
                bundle.quote_hash(),
                bundle.payee(),
                bundle.amount(),
            ));
        }
        chain
            .pay(mock::tx(0x31), Vec::new())
            .extra_logs
            .push(mock::payment_log(
                [0x99; 20],
                foreign.quote_hash(),
                foreign.payee(),
                foreign.amount(),
            ));
        chain.pay(mock::tx(0x32), vec![paid_elsewhere.entry()]);
    }
    let cases = [
        ("a reverted receipt, unpaid", &unpaid, mock::tx(0x30), false),
        (
            "a reverted receipt, paid elsewhere",
            &paid_elsewhere,
            mock::tx(0x30),
            true,
        ),
        (
            "another contract's log, unpaid",
            &foreign,
            mock::tx(0x31),
            false,
        ),
    ];
    for (name, bundle, tx_hash, verified) in cases {
        let provider = mock::provider(&chain, Spec::default()).await;
        let verifier = mock::verifier(&[&provider]);
        let outcome = check(
            &verifier,
            bundle,
            &[tx_hash],
            VerificationContext::ClientPut,
        )
        .await;
        assert_eq!(outcome.is_ok(), verified, "{name}: {outcome:?}");
        assert!(
            !provider.record_reads().is_empty(),
            "{name}: a record read decides"
        );
        assert_eq!(
            verifier.reads.fact(&bundle.quote_hash()).is_some(),
            verified,
            "{name}: only a payment the record shows is a fact"
        );
    }
}

/// An overpayment the receipt shows is taken, with no record read: a client
/// may pay more than the quote asks.
#[tokio::test]
async fn an_overpayment_in_a_receipt_is_taken_without_a_record_read() {
    let chain = Chain::shared();
    let bundle = Bundle::new([0x41; 32]);
    chain.lock().pay(
        mock::tx(0x40),
        vec![(bundle.quote_hash(), bundle.payee(), bundle.amount() * 10)],
    );
    for context in CONTEXTS {
        let provider = mock::provider(&chain, Spec::default()).await;
        let verifier = mock::verifier(&[&provider]);
        assert_eq!(
            check(&verifier, &bundle, &[mock::tx(0x40)], context)
                .await
                .unwrap(),
            PaymentStatus::PaymentVerified
        );
        assert!(provider.record_reads().is_empty(), "{context:?}");
        assert_eq!(provider.receipt_reads().len(), 1, "{context:?}");
    }
}

/// A payment to another payee, however large, is refused, and the refusal
/// comes from the record read, not from the receipt: a fact that doesn't pay
/// the quote says nothing. The record isn't empty, so it is refused at once.
#[tokio::test]
async fn a_payment_to_another_payee_in_a_receipt_is_refused_by_the_record_read() {
    let chain = Chain::shared();
    let bundle = Bundle::new([0x51; 32]);
    chain.lock().pay(
        mock::tx(0x50),
        vec![(bundle.quote_hash(), [0xEE; 20], bundle.amount() * 10)],
    );
    for context in CONTEXTS {
        let provider = mock::provider(&chain, Spec::default()).await;
        let verifier = mock::verifier(&[&provider]);
        let error = check(&verifier, &bundle, &[mock::tx(0x50)], context)
            .await
            .expect_err("a redirected payment is refused")
            .to_string();
        assert!(error.contains("redirected"), "{context:?}: {error}");
        assert_eq!(
            provider.record_reads().len(),
            1,
            "{context:?}: one read, and none again after a pause"
        );
    }
}

/// A batch answered without the payees is refused, never taken as paying
/// the quotes: each call is read again on its own, and still has no payee.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_answered_without_the_payee_is_refused() {
    let chain = Chain::shared();
    let bundles: Vec<Bundle> = (0x61..0x64u8).map(|i| Bundle::new([i; 32])).collect();
    chain
        .lock()
        .pay(mock::tx(0x60), bundles.iter().map(Bundle::entry).collect());
    let provider = mock::provider(
        &chain,
        Spec {
            short_records: true,
            delay: Duration::from_millis(500),
            ..Spec::default()
        },
    )
    .await;
    let verifier = mock::verifier(&[&provider]);
    let outcomes = futures::future::join_all(
        bundles
            .iter()
            .map(|bundle| check(&verifier, bundle, &[], VerificationContext::ClientPut)),
    )
    .await;
    for outcome in outcomes {
        let error = outcome.expect_err("no payee, no payment").to_string();
        assert!(error.contains("completedPayments lookup failed"), "{error}");
    }
    assert!(
        provider.record_reads().iter().any(|call| call.batch > 1),
        "a batch was read: {:?}",
        provider.record_reads()
    );
}

/// A check whose read another check of the same quote joined is cancelled:
/// the read goes on, the check that joined it verifies, and the payment is
/// kept as a fact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_check_joined_to_a_cancelled_one_still_verifies() {
    let chain = Chain::shared();
    let bundle = Arc::new(Bundle::new([0x71; 32]));
    chain.lock().pay(mock::tx(0x70), vec![bundle.entry()]);
    let provider = mock::provider(
        &chain,
        Spec {
            delay: Duration::from_millis(400),
            ..Spec::default()
        },
    )
    .await;
    let verifier = Arc::new(mock::verifier(&[&provider]));
    let spawn_check = || {
        let (verifier, bundle) = (Arc::clone(&verifier), Arc::clone(&bundle));
        tokio::spawn(
            async move { check(&verifier, &bundle, &[], VerificationContext::ClientPut).await },
        )
    };
    let first = spawn_check();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let joined = spawn_check();
    tokio::time::sleep(Duration::from_millis(100)).await;
    first.abort();
    assert_eq!(
        joined.await.unwrap().unwrap(),
        PaymentStatus::PaymentVerified
    );
    assert_eq!(provider.record_reads().len(), 1);
    assert!(verifier.reads.fact(&bundle.quote_hash()).is_some());
}

/// A shared read that times out doesn't refuse the check that joined it:
/// that check reads for itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_shared_read_does_not_refuse_the_check_that_joined_it() {
    let chain = Chain::shared();
    let bundle = Bundle::new([0x81; 32]);
    chain.lock().pay(mock::tx(0x80), vec![bundle.entry()]);
    let provider = mock::provider(
        &chain,
        Spec::acting(|n| if n <= 2 { Act::Stall } else { Act::Answer }),
    )
    .await;
    let verifier = mock::verifier(&[&provider]);
    let (leader, joined) = tokio::join!(
        check(&verifier, &bundle, &[], VerificationContext::ClientPut),
        async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            check(&verifier, &bundle, &[], VerificationContext::ClientPut).await
        }
    );
    assert!(leader.is_err(), "its own read timed out: {leader:?}");
    assert_eq!(joined.unwrap(), PaymentStatus::PaymentVerified);
}

/// With a cold cache, the first check of a transaction's payments reads its
/// receipt, once; the next chunk the transaction paid needs no read at all.
#[tokio::test]
async fn a_cold_cache_reads_a_transactions_receipt_once_for_all_its_payments() {
    let chain = Chain::shared();
    let first = Bundle::new([0x91; 32]);
    let second = Bundle::new([0x92; 32]);
    chain
        .lock()
        .pay(mock::tx(0x90), vec![first.entry(), second.entry()]);
    let provider = mock::provider(&chain, Spec::default()).await;
    let verifier = mock::verifier(&[&provider]);
    for bundle in [&first, &second] {
        assert_eq!(
            check(
                &verifier,
                bundle,
                &[mock::tx(0x90)],
                VerificationContext::ClientPut
            )
            .await
            .unwrap(),
            PaymentStatus::PaymentVerified
        );
    }
    assert_eq!(provider.receipt_reads().len(), 1);
    assert!(provider.record_reads().is_empty());
}

/// The paid amount a check needs, whatever it is, is read through the same
/// path: a quote with no fact is read, never taken as paid nothing, and a
/// read that fails is an error, never an amount of zero. A weighted
/// requirement below the full one takes a payment the full one refuses.
#[tokio::test]
async fn a_check_needing_another_amount_reads_the_paid_amount_on_a_miss() {
    let chain = Chain::shared();
    let bundle = Bundle::new([0xA1; 32]);
    let full = bundle.amount();
    chain.lock().pay(
        mock::tx(0xA0),
        vec![(bundle.quote_hash(), bundle.payee(), full / 2)],
    );
    let provider = mock::provider(&chain, Spec::default()).await;
    let verifier = mock::verifier(&[&provider]);
    let (peer, quote) = bundle
        .quotes
        .iter()
        .find(|(_, quote)| quote.hash() == bundle.median.hash())
        .unwrap();
    let needing = |amount: u128| LegacyMedianCandidate {
        encoded_peer_id: peer,
        quote,
        expected_amount: Amount::from(amount),
    };

    let mut receipts = ProofReceipts::new(&[]);
    let settled = verifier
        .settle(needing(full / 4), &mut receipts, Lane::Put)
        .await
        .unwrap();
    assert!(
        matches!(settled, CandidateSettlement::Paid(amount) if amount == Amount::from(full / 2)),
        "the amount read, not zero"
    );
    assert_eq!(provider.record_reads().len(), 1, "a miss is read");

    // The full requirement refuses the same payment, by a read of the
    // record, as the fact alone can't refuse; the amount named is the one
    // paid.
    let refused = verifier
        .settle(needing(full), &mut ProofReceipts::new(&[]), Lane::Put)
        .await;
    let Err(error) = refused else {
        panic!("half the full amount is refused under the full requirement");
    };
    assert!(
        error.to_string().contains(&format!("got {}", full / 2)),
        "{error}"
    );
    assert_eq!(provider.record_reads().len(), 2);

    // No answer is no amount.
    let busy = mock::provider(&chain, Spec::acting(|_| Act::Busy)).await;
    let verifier = mock::verifier(&[&busy]);
    let failed = verifier
        .settle(needing(full / 4), &mut ProofReceipts::new(&[]), Lane::Put)
        .await;
    let Err(error) = failed else {
        panic!("a failed read is no payment");
    };
    assert!(error.to_string().contains("lookup failed"), "{error}");
}

/// What one scenario puts on the chain for `bundle`, and the transactions its
/// proof names.
type Scenario = fn(&mut Chain, &Bundle) -> Vec<[u8; 32]>;

/// A node that reads receipts reaches the verdict of a node that reads
/// records alone, as nodes up to ds.15 do, on the same proofs, in every
/// context: through every way a proof's transactions can relate to its
/// quote's payment. And both reach the vault's verdict: paid, to the quote's
/// payee, at least three times the median price.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn nodes_reading_receipts_reach_the_verdicts_of_nodes_reading_records() {
    let scenarios: Vec<(&str, Scenario)> = vec![
        ("paid as quoted", |chain, b| {
            chain.pay(mock::tx(1), vec![b.entry()]);
            vec![mock::tx(1)]
        }),
        ("overpaid", |chain, b| {
            chain.pay(
                mock::tx(1),
                vec![(b.quote_hash(), b.payee(), b.amount() * 10)],
            );
            vec![mock::tx(1)]
        }),
        ("underpaid by one", |chain, b| {
            chain.pay(
                mock::tx(1),
                vec![(b.quote_hash(), b.payee(), b.amount() - 1)],
            );
            vec![mock::tx(1)]
        }),
        ("paid to another payee", |chain, b| {
            chain.pay(
                mock::tx(1),
                vec![(b.quote_hash(), [0xEE; 20], b.amount() * 10)],
            );
            vec![mock::tx(1)]
        }),
        ("occupied by another's dust", |chain, b| {
            chain.pay(mock::tx(1), vec![(b.quote_hash(), [0xEE; 20], 1)]);
            chain.pay(mock::tx(2), vec![b.entry()]);
            vec![mock::tx(2)]
        }),
        ("occupied by dust to its own payee", |chain, b| {
            chain.pay(mock::tx(1), vec![(b.quote_hash(), b.payee(), 1)]);
            chain.pay(mock::tx(2), vec![b.entry()]);
            vec![mock::tx(2)]
        }),
        ("paid in a transaction it doesn't name", |chain, b| {
            chain.pay(mock::tx(1), vec![b.entry()]);
            chain.pay(mock::tx(2), vec![([0x0B; 32], [0x0C; 20], 5)]);
            vec![mock::tx(2)]
        }),
        (
            "paid earlier, naming the repeat the vault skipped",
            |chain, b| {
                chain.pay(mock::tx(1), vec![b.entry()]);
                chain.pay(mock::tx(2), vec![b.entry()]);
                vec![mock::tx(2)]
            },
        ),
        ("unpaid, naming an unknown transaction", |_, _| {
            vec![mock::tx(0x99)]
        }),
        ("unpaid, naming nothing", |_, _| Vec::new()),
        (
            "unpaid, naming a reverted transaction showing its log",
            |chain, b| {
                let reverted = chain.pay(mock::tx(1), Vec::new());
                reverted.success = false;
                reverted.extra_logs.push(mock::payment_log(
                    mock::VAULT,
                    b.quote_hash(),
                    b.payee(),
                    b.amount(),
                ));
                vec![mock::tx(1)]
            },
        ),
        ("unpaid, naming another contract's log", |chain, b| {
            chain
                .pay(mock::tx(1), Vec::new())
                .extra_logs
                .push(mock::payment_log(
                    [0x99; 20],
                    b.quote_hash(),
                    b.payee(),
                    b.amount(),
                ));
            vec![mock::tx(1)]
        }),
        (
            "paid, naming a reverted transaction then the paying one",
            |chain, b| {
                chain.pay(mock::tx(1), Vec::new()).success = false;
                chain.pay(mock::tx(2), vec![b.entry()]);
                vec![mock::tx(1), mock::tx(2)]
            },
        ),
        (
            "paid, its transaction named among unknown ones",
            |chain, b| {
                chain.pay(mock::tx(5), vec![b.entry()]);
                vec![mock::tx(0x90), mock::tx(0x91), mock::tx(0x92), mock::tx(5)]
            },
        ),
        (
            "paid, its transaction named past the few read",
            |chain, b| {
                chain.pay(mock::tx(5), vec![b.entry()]);
                vec![
                    mock::tx(0x90),
                    mock::tx(0x91),
                    mock::tx(0x92),
                    mock::tx(0x93),
                    mock::tx(5),
                ]
            },
        ),
    ];

    let mut checks = Vec::new();
    for (index, (name, scenario)) in scenarios.iter().enumerate() {
        let bundle = Arc::new(Bundle::new([u8::try_from(index).unwrap() + 0xC0; 32]));
        let chain = Chain::shared();
        let tx_hashes = scenario(&mut chain.lock(), &bundle);
        let (payee_prefix, amount) = chain.lock().record(&bundle.quote_hash(), Duration::ZERO);
        let vault_says = payee_prefix
            == PaymentVerifier::rewards_address_prefix(&bundle.median.rewards_address)
            && amount >= bundle.amount();
        for context in CONTEXTS {
            let (chain, bundle, tx_hashes) =
                (Arc::clone(&chain), Arc::clone(&bundle), tx_hashes.clone());
            checks.push(async move {
                let reading_receipts = mock::provider(&chain, Spec::default()).await;
                let reading_records = mock::provider(&chain, Spec::default()).await;
                let (receipts_verdict, records_verdict) = tokio::join!(
                    async {
                        let verifier = mock::verifier(&[&reading_receipts]);
                        check(&verifier, &bundle, &tx_hashes, context).await
                    },
                    async {
                        let verifier = mock::verifier(&[&reading_records]);
                        check(&verifier, &bundle, &[], context).await
                    }
                );
                assert!(
                    reading_records.receipt_reads().is_empty(),
                    "{name}: a node reading records alone reads no receipt"
                );
                (
                    *name,
                    context,
                    vault_says,
                    receipts_verdict.is_ok(),
                    records_verdict.is_ok(),
                    reading_receipts.record_reads().len(),
                    reading_records.record_reads().len(),
                )
            });
        }
    }
    for (name, context, vault_says, receipts, records, receipt_path_reads, record_path_reads) in
        futures::future::join_all(checks).await
    {
        assert_eq!(
            receipts, records,
            "{name}, {context:?}: reading receipts came to another verdict"
        );
        assert_eq!(
            receipts, vault_says,
            "{name}, {context:?}: not the vault's verdict"
        );
        assert!(
            receipt_path_reads <= record_path_reads,
            "{name}, {context:?}: {receipt_path_reads} record reads with receipts, \
             {record_path_reads} without"
        );
    }
}

/// Against a chain node, not the stub: an Anvil chain with the payment vault
/// deployed, a wave of quotes paid in one transaction by a funded wallet, as
/// a client pays. A proof naming the transaction verifies from its receipt,
/// every payment in it kept as a fact; a proof naming nothing verifies from
/// the vault's records, read at once, so alone and in a batch; a quote nobody
/// paid is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payments_on_a_chain_node_verify_from_receipts_and_from_records() {
    let testnet = evmlib::testnet::Testnet::new()
        .await
        .expect("an Anvil chain with the vault (anvil on the PATH)");
    let network = testnet.to_network();
    let wallet = evmlib::wallet::Wallet::new_from_private_key(
        network.clone(),
        &testnet.default_wallet_private_key().unwrap(),
    )
    .unwrap();
    let wave: Vec<Bundle> = (0..8u8).map(|i| Bundle::new([0xE0 + i; 32])).collect();
    let (paid, _gas) = wallet
        .pay_for_quotes(wave.iter().map(|bundle| {
            (
                bundle.median.hash(),
                bundle.median.rewards_address,
                Amount::from(bundle.amount()),
            )
        }))
        .await
        .unwrap();
    let transactions: std::collections::HashSet<[u8; 32]> =
        paid.values().map(|tx_hash| tx_hash.0).collect();
    assert_eq!(transactions.len(), 1, "one transaction pays the wave");
    let tx_hash = *transactions.iter().next().unwrap();
    let verifier = || {
        let verifier = PaymentVerifier::new(PaymentVerifierConfig {
            evm: EvmVerifierConfig {
                network: network.clone(),
            },
            cache_capacity: 1_000,
            close_group_size: CLOSE_GROUP_SIZE,
            local_rewards_address: RewardsAddress::new([0xD1; 20]),
            price_floor: PriceFloorConfig::default(),
        });
        verifier.set_paid_quote_k_closest_for_tests(mock::issuer_peers());
        verifier
    };

    let from_receipts = verifier();
    for bundle in &wave {
        assert_eq!(
            check(
                &from_receipts,
                bundle,
                &[tx_hash],
                VerificationContext::ClientPut
            )
            .await
            .unwrap(),
            PaymentStatus::PaymentVerified
        );
    }
    for bundle in &wave {
        let fact = from_receipts.reads.fact(&bundle.quote_hash()).unwrap();
        assert_eq!(fact.amount, Amount::from(bundle.amount()));
        assert_eq!(
            fact.payee_prefix,
            PaymentVerifier::rewards_address_prefix(&bundle.median.rewards_address)
        );
    }

    let from_records = verifier();
    let outcomes = futures::future::join_all(
        wave.iter()
            .map(|bundle| check(&from_records, bundle, &[], VerificationContext::ClientPut)),
    )
    .await;
    for outcome in outcomes {
        assert_eq!(outcome.unwrap(), PaymentStatus::PaymentVerified);
    }

    let unpaid = Bundle::new([0xEF; 32]);
    assert!(check(
        &verifier(),
        &unpaid,
        &[tx_hash],
        VerificationContext::ClientPut
    )
    .await
    .is_err());
}
