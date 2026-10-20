//! MongoDB-backed documents and streams using the legacy collection shapes.

mod documents;
mod streams;
mod watch;

pub use documents::MongoDocuments;
pub use streams::MongoStreams;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
pub(crate) use tests::TestBlobs;
