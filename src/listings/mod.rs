//! Listings: the payment records of files stored with an envelope.
//!
//! A file can store its data map, or a salted hash of it, in an envelope (see
//! the `ant-listings` crate). When this node stores an envelope chunk it records
//! a listing that carries the chunk's on-chain payment, with only the payment
//! fields the envelope's flags show, and checks it with the chunk's close group
//! shortly after. Listings in the index are kept in index order; the others
//! only by address. A listing is deleted with its chunk.
//!
//! The node answers listing requests on [`ant_listings::TOPIC`]
//! (`listings.v2`), separately from the chunk protocol, with permits of
//! their own: list requests page through the listings in the index, get
//! requests return any listing by address, and changes and range list
//! requests read the change feed of the listings it serves in the index for
//! an address range. A reader can watch a range and be pushed each new
//! record in it. A request of a kind it doesn't know is answered
//! `unsupported`. Messages on older listings topics are not answered.

mod chain;
// Envelopes and a record exactly as the client writes them.
#[cfg(test)]
mod golden;
// The chunk handler's listings hook.
#[cfg(test)]
mod hooks_tests;
mod map_size;
mod service;
mod store;

pub(crate) use chain::listings_rpc_urls;
pub use service::ListingService;
