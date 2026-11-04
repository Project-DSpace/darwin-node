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

// This function is non-inline to prevent the optimizer from looking inside it.
#[inline(never)]
fn constant_time_ne(a: &[u8], b: &[u8]) -> u8 {
    assert!(a.len() == b.len());

    // These useless slices make the optimizer elide the bounds checks.
    // See the comment in clone_from_slice() added on Rust commit 6a7bc47.
    let len = a.len();
    let a = &a[..len];
    let b = &b[..len];

    let mut tmp = 0;
    for i in 0..len {
        tmp |= a[i] ^ b[i];
    }
    tmp // The compare with 0 must happen outside this function.
}

/// Compares byte strings in constant time.
pub(crate) fn eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && constant_time_ne(a, b) == 0
}
