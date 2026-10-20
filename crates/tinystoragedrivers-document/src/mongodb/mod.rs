//! MongoDB-backed documents and streams using the legacy collection shapes.

mod documents;
mod streams;

pub use documents::MongoDocuments;
pub use streams::MongoStreams;
