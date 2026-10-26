//! Root proofs (tag 0x03) through the payment check's real reads, against a
//! chain served over JSON-RPC on 127.0.0.1 whose root vault records waves by
//! key (`settlement::mock`): a recorded wave verifies and is kept for good, a
//! proof out of shape or underpaid is refused before anything is read, the
//! key binds the payee's whole address, and neither vault's payment stands in
//! for the other's.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::payment::settlement::mock::{self, Bundle, Chain, Spec};

/// Check `bundle`'s address against `proof`.
async fn check(
    verifier: &PaymentVerifier,
    bundle: &Bundle,
    proof: &[u8],
    context: VerificationContext,
) -> Result<PaymentStatus> {
    verifier
        .verify_payment(
            &bundle.address,
            Held::Bytes(ant_protocol::chunk::MAX_CHUNK_SIZE),
            Some(proof),
            context,
        )
        .await
}

/// Three chunks' payments in one wave.
fn three() -> (Bundle, Bundle, Bundle) {
    (
        Bundle::new([0x31; 32]),
        Bundle::new([0x32; 32]),
        Bundle::new([0x33; 32]),
    )
}

/// A recorded wave verifies each of its payments, in every context. The
/// record is read once, and kept: a second payment of the wave reads nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recorded_wave_verifies_and_its_record_is_kept() {
    let chain = Chain::shared();
    let (a, b, c) = three();
    let wave = mock::wave_of(&[&a, &b, &c]);
    chain.lock().record_wave(&wave);
    let provider = mock::provider(&chain, Spec::default()).await;
    let verifier = mock::root_verifier(&[&provider]);

    let named = [mock::tx(0xAA)];
    assert_eq!(
        check(
            &verifier,
            &a,
            &a.root_proof_in(&wave, &named),
            VerificationContext::ClientPut
        )
        .await
        .unwrap(),
        PaymentStatus::PaymentVerified
    );
    assert_eq!(provider.calls("eth_call").len(), 1);
    for (bundle, context) in [
        (&b, VerificationContext::FreshReplication),
        (&c, VerificationContext::PaidListAdmission),
    ] {
        assert_eq!(
            check(
                &verifier,
                bundle,
                &bundle.root_proof_in(&wave, &named),
                context
            )
            .await
            .unwrap(),
            PaymentStatus::PaymentVerified
        );
    }
    assert_eq!(
        provider.calls("eth_call").len(),
        1,
        "a recorded wave is read once"
    );
    assert!(
        provider.receipt_reads().is_empty(),
        "a root proof's settlement is the wave's record, not a receipt"
    );
}

/// A wave whose record reads empty is read once more, after the pause, and
/// then refused. A per-quote payment of the same quote, in the transaction
/// the proof names, never stands in for the wave's record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unrecorded_wave_is_refused_after_one_read_again_whatever_the_per_quote_vault_holds() {
    let chain = Chain::shared();
    let (a, b, c) = three();
    let wave = mock::wave_of(&[&a, &b, &c]);
    chain.lock().pay(mock::tx(0xAB), vec![a.entry()]);
    let provider = mock::provider(&chain, Spec::default()).await;
    let verifier = mock::root_verifier(&[&provider]);

    let error = check(
        &verifier,
        &a,
        &a.root_proof_in(&wave, &[mock::tx(0xAB)]),
        VerificationContext::ClientPut,
    )
    .await
    .expect_err("an unrecorded wave is refused");
    assert!(error.to_string().contains("no wave recorded"), "{error}");
    assert_eq!(
        provider.calls("eth_call").len(),
        2,
        "read, then read once more after the pause"
    );
    assert!(provider.receipt_reads().is_empty());
}

/// A proof out of shape is refused before anything is read: a leaf count of 0
/// or above the leaf limit, a place outside the wave, and paths a hash short
/// or long, up to one of 10 hashes. So is a payment below three times the
/// median price, and a root proof at a node that knows no root vault.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_root_proof_out_of_shape_or_underpaid_is_refused_unread() {
    let chain = Chain::shared();
    let (a, b, c) = three();
    let wave = mock::wave_of(&[&a, &b, &c]);
    chain.lock().record_wave(&wave);
    let provider = mock::provider(&chain, Spec::default()).await;
    let verifier = mock::root_verifier(&[&provider]);
    let named = [mock::tx(0xAA)];
    let amount = Amount::from(a.amount());
    let index = wave
        .payments()
        .iter()
        .position(|payment| payment.quote_hash == a.quote_hash())
        .unwrap();
    let path = wave.path(index).unwrap();
    let index = index as u64;

    let mut short = path.clone();
    short.pop();
    let mut long = path.clone();
    long.push([0x77; 32]);
    let ten = vec![[0x77; 32]; 10];
    let refused = [
        ("a leaf count of 0", a.root_proof(amount, 0, 0, &[], &named)),
        (
            "a leaf count above the limit",
            a.root_proof(amount, index, 513, &ten[..9], &named),
        ),
        (
            "a place outside the wave",
            a.root_proof(amount, 3, 3, &path, &named),
        ),
        (
            "a path a hash short",
            a.root_proof(amount, index, 3, &short, &named),
        ),
        (
            "a path a hash long",
            a.root_proof(amount, index, 3, &long, &named),
        ),
        (
            "a path of 10 hashes",
            a.root_proof(amount, index, 512, &ten, &named),
        ),
        (
            "a payment below three times the median price",
            a.root_proof(amount - Amount::from(1u64), index, 3, &path, &named),
        ),
    ];
    for (what, proof) in refused {
        let outcome = check(&verifier, &a, &proof, VerificationContext::ClientPut).await;
        assert!(outcome.is_err(), "{what}: {outcome:?}");
    }
    let without_root = mock::verifier(&[&provider]);
    let error = check(
        &without_root,
        &a,
        &a.root_proof_in(&wave, &named),
        VerificationContext::ClientPut,
    )
    .await
    .expect_err("a node with no root vault refuses root proofs");
    assert!(error.to_string().contains("no root vault"), "{error}");
    assert!(
        provider.received().is_empty(),
        "nothing is read for a proof refused on its shape: {:?}",
        provider.received()
    );
}

/// The key binds the payee's whole address. A wave recorded paying an
/// address that shares the quote's payee's leading 16 bytes, which is all
/// the per-quote vault's record keeps, is not the quote's payment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_key_binds_the_payees_whole_address() {
    let chain = Chain::shared();
    let a = Bundle::new([0x34; 32]);
    let mut payee = a.payee();
    payee[19] ^= 0xFF;
    let mut paid = a.root_payment();
    paid.payee = payee;
    let wave = ant_listings::root::Wave::new(vec![paid]).unwrap();
    chain.lock().record_wave(&wave);
    let provider = mock::provider(&chain, Spec::default()).await;
    let verifier = mock::root_verifier(&[&provider]);

    // The proof is the wave's own: the place, the leaf count and the path
    // lead to the recorded key from the leaf paying `payee`.
    let proof = a.root_proof_in(&wave, &[mock::tx(0xAA)]);
    let error = check(&verifier, &a, &proof, VerificationContext::ClientPut)
        .await
        .expect_err("a payment to another payee is refused");
    assert!(error.to_string().contains("no wave recorded"), "{error}");
}

/// A root vault's `DataPaymentMade` log, in the transaction a single-node
/// proof names, never settles that proof: single-node proofs settle in the
/// per-quote vault alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_root_vaults_log_never_settles_a_single_node_proof() {
    let chain = Chain::shared();
    let a = Bundle::new([0x35; 32]);
    chain.lock().pay(mock::tx(0xAC), Vec::new()).extra_logs = vec![mock::payment_log(
        mock::ROOT_VAULT,
        a.quote_hash(),
        a.payee(),
        a.amount(),
    )];
    let provider = mock::provider(&chain, Spec::default()).await;
    let verifier = mock::root_verifier(&[&provider]);

    let outcome = verifier
        .verify_payment(
            &a.address,
            Held::Bytes(ant_protocol::chunk::MAX_CHUNK_SIZE),
            Some(&a.proof(&[mock::tx(0xAC)])),
            VerificationContext::ClientPut,
        )
        .await;
    assert!(outcome.is_err(), "{outcome:?}");
}

/// The metrics label names each proof type by its tag.
#[test]
fn each_proof_tag_has_its_metrics_label() {
    for (tag, label) in [
        (crate::ant_protocol::PROOF_TAG_SINGLE_NODE, "single_node"),
        (crate::ant_protocol::PROOF_TAG_MERKLE, "merkle"),
        (crate::ant_protocol::PROOF_TAG_ROOT, "root"),
        (0x04, "unknown"),
    ] {
        assert_eq!(payment_proof_type_label(Some(&[tag, 0, 1])), label);
    }
    assert_eq!(payment_proof_type_label(None), "none");
}

/// A merkle proof is refused before anything is read, at a node that takes
/// root proofs too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_merkle_proof_is_refused_first_beside_root_proofs() {
    let chain = Chain::shared();
    let a = Bundle::new([0x36; 32]);
    let provider = mock::provider(&chain, Spec::default()).await;
    let verifier = mock::root_verifier(&[&provider]);
    let mut merkle = a.root_proof_in(&mock::wave_of(&[&a]), &[mock::tx(0xAA)]);
    merkle[0] = crate::ant_protocol::PROOF_TAG_MERKLE;
    let error = check(&verifier, &a, &merkle, VerificationContext::ClientPut)
        .await
        .expect_err("a merkle proof is refused");
    assert!(
        error.to_string().contains(BATCH_PAYMENTS_REFUSED),
        "{error}"
    );
    assert!(provider.received().is_empty());
}
