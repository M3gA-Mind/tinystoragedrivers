//! SQLite document and stream engines sharing the generic connection handle.
pub use tinystoragedrivers_connect::sqlite::SqliteTables;

mod body;
pub mod documents;
mod fields;
pub mod streams;

pub use documents::SqliteDocuments;
pub use streams::SqliteStreams;

pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(crate) fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
