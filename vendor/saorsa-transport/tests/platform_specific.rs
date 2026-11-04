// This software is licensed under the MIT license <LICENSE-MIT or
// https://opensource.org/licenses/MIT> or the Apache License, Version 2.0
// <LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0>, at your
// option. This file may not be copied, modified, or distributed except
// according to those terms.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//! Platform-specific test harness

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "platform_specific/mod.rs"]
mod platform_specific;

// Re-export tests
#[allow(unused_imports)]
pub use platform_specific::*;
