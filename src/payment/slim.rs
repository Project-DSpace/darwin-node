//! Slim proofs: the form of a payment proof a node passes to other nodes.
//!
//! A client's PUT carries every quote it was given, each with its commitment
//! sidecar. Once a storer has accepted the record, the nodes it tells about the
//! record need only what their own check reads: the quotes at the median price
//! (the payment check's candidates, normally one) and the transaction hashes.
//! A slim proof keeps exactly those, in the client's order, with no sidecars,
//! and writes every byte field as a msgpack byte string. The tag byte and the
//! field order are unchanged, so every release decodes it with the decoder it
//! already has.
//!
//! One builder, [`slim_proof`], serves every place that puts a proof into a
//! message to another node: notices, offers and pointer offers. It takes a
//! parsed proof of a kind the verifier accepts, never raw bytes, and has no
//! pass-through case: a proof it can't slim is not forwarded. A proof the
//! verifier comes to accept in a later release gets its slim form here, in the
//! same change.

use std::fmt;

use evmlib::ProofOfPayment;
use serde::Serialize;

use crate::ant_protocol::{XorName, PROOF_TAG_SINGLE_NODE};
use crate::payment::proof::{
    deserialize_single_node_proof, detect_proof_type, PaymentProof, ProofType,
};
use crate::payment::verifier::{
    PaymentVerifier, MAX_PAYMENT_PROOF_SIZE_BYTES, MIN_PAYMENT_PROOF_SIZE_BYTES,
};
use crate::payment::verify_quote_content;

/// The proof kinds the builder slims, by their tag byte. A proof kind the
/// verifier accepts must appear here, or be forwarded whole as a reviewed
/// exception.
pub const SLIMMED_PROOF_TAGS: &[u8] = &[PROOF_TAG_SINGLE_NODE];

/// Why a proof was not slimmed, and so is not forwarded.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SlimDeclined {
    /// Shorter than the verifier's smallest proof.
    TooSmall(usize),
    /// Longer than the verifier's largest proof.
    TooLarge(usize),
    /// Not a single-node proof: merkle (refused in every context) or an
    /// unknown tag.
    NotSingleNode,
    /// The bytes did not decode as a single-node proof.
    Unparseable,
    /// The quotes failed the verifier's structure check (none, more than the
    /// close group, or a repeated peer).
    Structure,
    /// A quote's price is not the one its signed count gives.
    OffCurve,
    /// No median price, or a zero one.
    NoMedian,
    /// No median-priced quote names the record's content address: a proof
    /// for another record, which every receiver refuses.
    OtherContent,
    /// The slim proof could not be written.
    Encode,
}

impl fmt::Display for SlimDeclined {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooSmall(len) => write!(f, "proof of {len} bytes is under the smallest proof"),
            Self::TooLarge(len) => write!(f, "proof of {len} bytes is over the largest proof"),
            Self::NotSingleNode => f.write_str("not a single-node proof"),
            Self::Unparseable => f.write_str("the proof does not decode"),
            Self::Structure => f.write_str("the quotes fail the structure check"),
            Self::OffCurve => f.write_str("a quote's price is off the pricing curve"),
            Self::NoMedian => f.write_str("the quotes have no median price"),
            Self::OtherContent => {
                f.write_str("no median-priced quote names the record's content address")
            }
            Self::Encode => f.write_str("the slim proof could not be written"),
        }
    }
}

/// Parse a proof for forwarding.
///
/// The checks are the verifier's size bounds, its single-node tag and the
/// decode: the local checks a proof meets before the verifier's chain reads.
/// A proof that passed the payment check passes them too; a PUT accepted from
/// the payment cache, whose proof was never read, is held to them here.
///
/// # Errors
///
/// The first check the bytes fail.
pub fn parse_forwardable(bytes: &[u8]) -> Result<PaymentProof, SlimDeclined> {
    let len = bytes.len();
    if len < MIN_PAYMENT_PROOF_SIZE_BYTES {
        return Err(SlimDeclined::TooSmall(len));
    }
    if len > MAX_PAYMENT_PROOF_SIZE_BYTES {
        return Err(SlimDeclined::TooLarge(len));
    }
    if detect_proof_type(bytes) != Some(ProofType::SingleNode) {
        return Err(SlimDeclined::NotSingleNode);
    }
    deserialize_single_node_proof(bytes).map_err(|_| SlimDeclined::Unparseable)
}

/// Build the slim form of a parsed single-node proof: the quotes at the
/// verifier's median price, in the client's order, every transaction hash, no
/// sidecars, byte fields as msgpack byte strings.
///
/// Before choosing, the builder runs the verifier's own local checks of the
/// quotes: the structure check, the price re-check of every quote, and the
/// content check of the median-priced ones: at least one must name `content`,
/// the address the payment is for (a chunk's address, a pointer's
/// `state_id`), by the same comparison the verifier's content check makes. On
/// a fresh accept the verifier has run them already; after a PUT accepted
/// from the payment cache they are what stands in for the skipped read, since
/// the unpaid quotes are what receivers no longer see, and a proof for
/// another record is what every receiver would refuse.
///
/// # Errors
///
/// The check the quotes fail, or [`SlimDeclined::Encode`].
pub fn slim_proof(proof: &PaymentProof, content: &XorName) -> Result<Vec<u8>, SlimDeclined> {
    let payment = &proof.proof_of_payment;
    PaymentVerifier::validate_quote_structure(payment).map_err(|_| SlimDeclined::Structure)?;
    PaymentVerifier::validate_quote_arithmetic(payment).map_err(|_| SlimDeclined::OffCurve)?;
    let median =
        PaymentVerifier::single_node_median_price(payment).map_err(|_| SlimDeclined::NoMedian)?;
    let kept: Vec<_> = payment
        .peer_quotes
        .iter()
        .filter(|(_, quote)| quote.price == median)
        .cloned()
        .collect();
    if !kept
        .iter()
        .any(|(_, quote)| verify_quote_content(quote, content))
    {
        return Err(SlimDeclined::OtherContent);
    }
    let slim = PaymentProof {
        proof_of_payment: ProofOfPayment { peer_quotes: kept },
        tx_hashes: proof.tx_hashes.clone(),
        commitment_sidecars: Vec::new(),
    };
    encode_single_node(&slim).map_err(|_| SlimDeclined::Encode)
}

/// [`parse_forwardable`], then [`slim_proof`]: what a node puts into a notice,
/// an offer or a pointer offer in place of the proof it was handed, for the
/// record whose payment names `content`.
///
/// # Errors
///
/// The first check the bytes fail.
pub fn forwardable_slim_proof(bytes: &[u8], content: &XorName) -> Result<Vec<u8>, SlimDeclined> {
    let slim = parse_forwardable(bytes).and_then(|proof| slim_proof(&proof, content));
    if slim.is_err() {
        crate::replication::fresh_metrics::bump(
            crate::replication::fresh_metrics::FreshCounter::SlimProofsDeclined,
        );
    }
    slim
}

/// Whether a received proof is refused for its form: a single-node proof
/// whose bytes don't decode, or whose quotes fail the structure check.
/// Counted by where it arrived; a refusal for any other reason is not one.
#[must_use]
pub fn refused_for_form(bytes: &[u8]) -> bool {
    match parse_forwardable(bytes) {
        Err(SlimDeclined::Unparseable) => true,
        Ok(proof) => PaymentVerifier::validate_quote_structure(&proof.proof_of_payment).is_err(),
        Err(_) => false,
    }
}

/// Write a single-node proof with its tag byte and the default field order,
/// every `Vec<u8>` and other byte sequence as a msgpack byte string
/// (`BytesMode::ForceIterables`); fixed-size arrays keep their form. The
/// decoder of every release reads a byte string into a `Vec<u8>`, and quote
/// hashes and signatures are computed from the fields' values, not from the
/// bytes on the wire.
fn encode_single_node(proof: &PaymentProof) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let mut out = vec![PROOF_TAG_SINGLE_NODE];
    let mut serializer = rmp_serde::Serializer::new(&mut out)
        .with_bytes(rmp_serde::config::BytesMode::ForceIterables);
    proof.serialize(&mut serializer)?;
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::payment::pricing::calculate_price;
    use crate::payment::proof::serialize_single_node_proof;
    use evmlib::common::{Amount, TxHash};
    use evmlib::{EncodedPeerId, PaymentQuote, RewardsAddress};
    use rand::seq::SliceRandom;
    use rand::Rng;
    use std::time::SystemTime;

    /// The address every test quote names.
    const CONTENT: XorName = [7u8; 32];

    /// An on-curve quote at `records` stored records, with a filled key and
    /// signature so the byte fields have weight. The builder never checks
    /// signatures; the verifier does, on the bytes' values.
    fn quote_at(records: usize, seed: u8) -> (EncodedPeerId, PaymentQuote) {
        let quote = PaymentQuote {
            content: xor_name::XorName(CONTENT),
            timestamp: SystemTime::now(),
            price: calculate_price(records),
            rewards_address: RewardsAddress::new([seed; 20]),
            committed_key_count: u32::try_from(records).unwrap(),
            commitment_pin: (records > 0).then_some([0xA5u8; 32]),
            pub_key: vec![seed; 1952],
            signature: vec![seed.wrapping_add(1); 3309],
        };
        (EncodedPeerId::new(rand::random()), quote)
    }

    fn full_proof(records: &[usize]) -> PaymentProof {
        let peer_quotes = records
            .iter()
            .enumerate()
            .map(|(i, r)| quote_at(*r, u8::try_from(i + 1).unwrap()))
            .collect();
        PaymentProof {
            proof_of_payment: ProofOfPayment { peer_quotes },
            tx_hashes: vec![TxHash::from([0x11u8; 32]), TxHash::from([0x22u8; 32])],
            commitment_sidecars: vec![vec![0x33u8; 4000]; records.len()],
        }
    }

    fn median_price(records: &[usize]) -> Amount {
        let mut prices: Vec<Amount> = records.iter().map(|r| calculate_price(*r)).collect();
        prices.sort();
        prices[prices.len() / 2]
    }

    #[test]
    fn a_slim_proof_decodes_with_both_decoders_and_keeps_the_quotes_values() {
        let full = full_proof(&[10, 20, 30, 40, 50, 60, 70]);
        let slim = slim_proof(&full, &CONTENT).unwrap();
        assert_eq!(slim[0], PROOF_TAG_SINGLE_NODE);

        let decoded = deserialize_single_node_proof(&slim).unwrap();
        let plain: PaymentProof = rmp_serde::from_slice(&slim[1..]).unwrap();
        assert_eq!(decoded.proof_of_payment.peer_quotes.len(), 1);
        assert_eq!(plain.proof_of_payment.peer_quotes.len(), 1);

        let (kept_id, kept) = &decoded.proof_of_payment.peer_quotes[0];
        let (orig_id, orig) = &full.proof_of_payment.peer_quotes[3];
        assert_eq!(kept_id, orig_id);
        assert_eq!(kept.hash(), orig.hash());
        assert_eq!(kept.bytes_for_sig(), orig.bytes_for_sig());
        assert_eq!(kept.signature, orig.signature);
        assert_eq!(decoded.tx_hashes, full.tx_hashes);
        assert!(decoded.commitment_sidecars.is_empty());

        // Re-encoded the old way, the slim proof's content round-trips.
        let again = serialize_single_node_proof(&decoded).unwrap();
        let back = deserialize_single_node_proof(&again).unwrap();
        assert_eq!(back.proof_of_payment.peer_quotes[0].1.hash(), orig.hash());
    }

    #[test]
    fn a_slim_proof_is_a_fraction_of_the_whole_one() {
        let full = full_proof(&[10, 20, 30, 40, 50, 60, 70]);
        let whole = serialize_single_node_proof(&full).unwrap();
        let slim = slim_proof(&full, &CONTENT).unwrap();
        // Byte strings, one quote, no sidecars: well under a sixth.
        assert!(
            slim.len() * 6 < whole.len(),
            "slim {} whole {}",
            slim.len(),
            whole.len()
        );
        // A byte string spends one byte per byte of key and signature.
        assert!(slim.len() < 1952 + 3309 + 600, "slim {}", slim.len());
    }

    #[test]
    fn the_kept_quotes_are_the_verifiers_candidates_over_random_prices_with_ties() {
        let mut rng = rand::thread_rng();
        for _ in 0..200 {
            let n = rng.gen_range(1..=7);
            let records: Vec<usize> = (0..n).map(|_| rng.gen_range(0..4) * 1000).collect();
            let full = full_proof(&records);
            let slim =
                deserialize_single_node_proof(&slim_proof(&full, &CONTENT).unwrap()).unwrap();
            let median = median_price(&records);
            let expected: Vec<_> = full
                .proof_of_payment
                .peer_quotes
                .iter()
                .filter(|(_, q)| q.price == median)
                .map(|(id, q)| (id.clone(), q.hash()))
                .collect();
            let kept: Vec<_> = slim
                .proof_of_payment
                .peer_quotes
                .iter()
                .map(|(id, q)| (id.clone(), q.hash()))
                .collect();
            assert_eq!(kept, expected, "records {records:?}");
            assert_eq!(
                PaymentVerifier::single_node_median_price(&slim.proof_of_payment).unwrap(),
                median
            );
        }
    }

    #[test]
    fn the_kept_quotes_stay_in_the_clients_order() {
        let mut records = vec![500, 500, 500, 10, 20, 900, 900];
        records.shuffle(&mut rand::thread_rng());
        let full = full_proof(&records);
        let slim = deserialize_single_node_proof(&slim_proof(&full, &CONTENT).unwrap()).unwrap();
        let order: Vec<_> = full
            .proof_of_payment
            .peer_quotes
            .iter()
            .filter(|(_, q)| q.price == calculate_price(500))
            .map(|(id, _)| id.clone())
            .collect();
        let kept: Vec<_> = slim
            .proof_of_payment
            .peer_quotes
            .iter()
            .map(|(id, _)| id.clone())
            .collect();
        assert_eq!(kept, order);
    }

    #[test]
    fn a_proof_that_is_not_a_good_single_node_proof_is_never_slimmed() {
        let whole = serialize_single_node_proof(&full_proof(&[1, 2, 3, 4, 5, 6, 7])).unwrap();
        assert!(forwardable_slim_proof(&whole, &CONTENT).is_ok());

        assert_eq!(
            forwardable_slim_proof(&[], &CONTENT),
            Err(SlimDeclined::TooSmall(0))
        );
        assert_eq!(
            forwardable_slim_proof(&[PROOF_TAG_SINGLE_NODE; 16], &CONTENT),
            Err(SlimDeclined::TooSmall(16))
        );
        let huge = vec![PROOF_TAG_SINGLE_NODE; MAX_PAYMENT_PROOF_SIZE_BYTES + 1];
        assert!(matches!(
            forwardable_slim_proof(&huge, &CONTENT),
            Err(SlimDeclined::TooLarge(_))
        ));

        let mut merkle = whole.clone();
        merkle[0] = crate::ant_protocol::PROOF_TAG_MERKLE;
        assert_eq!(
            forwardable_slim_proof(&merkle, &CONTENT),
            Err(SlimDeclined::NotSingleNode)
        );
        let mut unknown = whole;
        unknown[0] = 0x7F;
        assert_eq!(
            forwardable_slim_proof(&unknown, &CONTENT),
            Err(SlimDeclined::NotSingleNode)
        );

        let garbage = [PROOF_TAG_SINGLE_NODE; 64];
        assert_eq!(
            forwardable_slim_proof(&garbage, &CONTENT),
            Err(SlimDeclined::Unparseable)
        );

        let mut off_curve = full_proof(&[1, 2, 3, 4, 5, 6, 7]);
        off_curve.proof_of_payment.peer_quotes[0].1.price = Amount::from(12_345u64);
        let off_curve = serialize_single_node_proof(&off_curve).unwrap();
        if crate::replication::config::QUOTE_ARITHMETIC_RECHECK_ENABLED {
            assert_eq!(
                forwardable_slim_proof(&off_curve, &CONTENT),
                Err(SlimDeclined::OffCurve)
            );
        }

        let mut empty = full_proof(&[1]);
        empty.proof_of_payment.peer_quotes.clear();
        assert_eq!(slim_proof(&empty, &CONTENT), Err(SlimDeclined::Structure));
        let eight = full_proof(&[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(slim_proof(&eight, &CONTENT), Err(SlimDeclined::Structure));
    }

    /// A well-formed proof for another record is not forwarded: every receiver
    /// would refuse it, and the offered peers would then fail a possession
    /// check they could never pass. One median-priced quote naming the
    /// record is enough, as it is for the verifier.
    #[test]
    fn a_well_formed_proof_for_another_record_is_never_slimmed() {
        let full = full_proof(&[10, 20, 30, 40, 50, 60, 70]);
        let whole = serialize_single_node_proof(&full).unwrap();
        let other = [8u8; 32];
        assert_eq!(slim_proof(&full, &other), Err(SlimDeclined::OtherContent));
        assert_eq!(
            forwardable_slim_proof(&whole, &other),
            Err(SlimDeclined::OtherContent)
        );

        // A tie at the median where only one quote names the record.
        let mut tie = full_proof(&[500, 500, 500, 10, 20, 900, 900]);
        for (_, quote) in tie.proof_of_payment.peer_quotes.iter_mut().take(2) {
            quote.content = xor_name::XorName(other);
        }
        assert!(slim_proof(&tie, &CONTENT).is_ok());

        // Only quotes off the median name the record: they are not the
        // ones a receiver checks.
        for (_, quote) in tie.proof_of_payment.peer_quotes.iter_mut().take(3) {
            quote.content = xor_name::XorName(other);
        }
        assert_eq!(slim_proof(&tie, &CONTENT), Err(SlimDeclined::OtherContent));
    }

    /// A proof is refused for its form when it doesn't decode or its quotes
    /// fail the structure check; a good proof, a merkle one and an off-curve
    /// one are refused (or not) for other reasons, and not counted as form.
    #[test]
    fn only_undecodable_or_misshapen_proofs_are_refused_for_their_form() {
        let whole = serialize_single_node_proof(&full_proof(&[1, 2, 3, 4, 5, 6, 7])).unwrap();
        assert!(!refused_for_form(&whole));
        assert!(refused_for_form(&[PROOF_TAG_SINGLE_NODE; 64]));
        let eight = serialize_single_node_proof(&full_proof(&[1, 2, 3, 4, 5, 6, 7, 8])).unwrap();
        assert!(refused_for_form(&eight));
        let mut merkle = whole.clone();
        merkle[0] = crate::ant_protocol::PROOF_TAG_MERKLE;
        assert!(!refused_for_form(&merkle));
        let mut off_curve = full_proof(&[1, 2, 3]);
        off_curve.proof_of_payment.peer_quotes[0].1.price = Amount::from(12_345u64);
        assert!(!refused_for_form(
            &serialize_single_node_proof(&off_curve).unwrap()
        ));
    }

    /// Every proof kind the protocol tells apart is either slimmed here or
    /// refused by the verifier in every context (merkle). A new kind the
    /// verifier comes to accept fails this until the builder has its case.
    #[test]
    fn every_proof_kind_is_slimmed_or_refused() {
        let whole = serialize_single_node_proof(&full_proof(&[1, 2, 3, 4, 5, 6, 7])).unwrap();
        for tag in 0..=u8::MAX {
            let mut bytes = whole.clone();
            bytes[0] = tag;
            match detect_proof_type(&bytes) {
                None | Some(ProofType::Merkle) => {
                    assert!(!SLIMMED_PROOF_TAGS.contains(&tag), "tag {tag:#04x}");
                    assert_eq!(
                        forwardable_slim_proof(&bytes, &CONTENT),
                        Err(SlimDeclined::NotSingleNode),
                        "tag {tag:#04x}"
                    );
                }
                Some(ProofType::SingleNode) => {
                    assert!(SLIMMED_PROOF_TAGS.contains(&tag), "tag {tag:#04x}");
                    assert!(
                        forwardable_slim_proof(&bytes, &CONTENT).is_ok(),
                        "tag {tag:#04x}"
                    );
                }
                Some(other) => {
                    panic!("proof kind {other:?} (tag {tag:#04x}) has no case in the slim builder")
                }
            }
        }
        assert_eq!(SLIMMED_PROOF_TAGS, &[PROOF_TAG_SINGLE_NODE]);
    }
}
