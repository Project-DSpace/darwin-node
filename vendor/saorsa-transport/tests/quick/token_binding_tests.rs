// This software is licensed under the MIT license <LICENSE-MIT or
// https://opensource.org/licenses/MIT> or the Apache License, Version 2.0
// <LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0>, at your
// option. This file may not be copied, modified, or distributed except
// according to those terms.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//! Tests for token_v2 binding to (fingerprint || CID || nonce)

#![allow(clippy::unwrap_used, clippy::expect_used)]

use saorsa_transport::shared::ConnectionId;

#[test]
fn binding_token_round_trip_binds_peer_and_cid() {
    let mut rng = rand::thread_rng();
    let key = saorsa_transport::token_v2::test_key_from_rng(&mut rng);

    let fingerprint: [u8; 32] = [7u8; 32];
    let cid = ConnectionId::new(&[9u8; 8]); // use 8-byte cid

    let tok = saorsa_transport::token_v2::encode_binding_token(&key, &fingerprint, &cid).unwrap();
    let dec = saorsa_transport::token_v2::decode_binding_token(&key, &tok).expect("decodes");

    assert_eq!(dec.spki_fingerprint, fingerprint);
    assert_eq!(dec.cid, cid);
}
