//! Connecting the live tests to `TSD_MONGO_URL`.

#![allow(
    dead_code,
    reason = "each test binary uses a different subset of the helpers"
)]
#![allow(
    clippy::expect_used,
    reason = "a test helper reports a misconfigured server by panicking"
)]

use tinystoragedrivers_mongodb::MongoStorage;

/// The configured server URL, if any.
pub(crate) fn url() -> Option<String> {
    std::env::var("TSD_MONGO_URL")
        .ok()
        .filter(|url| !url.is_empty())
}

/// The database named by the URL path.
pub(crate) fn url_database(url: &str) -> String {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let path = rest.split_once('/').map_or("", |(_, path)| path);
    let name = path.split(['?', '/']).next().unwrap_or_default();
    if name.is_empty() {
        "tsd_test".to_owned()
    } else {
        name.to_owned()
    }
}

/// A backend on the configured server, or `None` without one.
pub(crate) async fn connect() -> Option<MongoStorage> {
    let url = url()?;
    Some(
        MongoStorage::connect(&url, &url_database(&url))
            .await
            .expect("TSD_MONGO_URL is set but the server cannot be reached"),
    )
}

/// The name of the database `connect_isolated(label)` uses.
pub(crate) fn isolated_name(label: &str) -> String {
    format!("tsd_{label}_{}", std::process::id())
}

/// A backend on its own database of the configured server, so a test shares
/// no collections with the others. Call only after `connect` found a server.
pub(crate) async fn connect_isolated(label: &str) -> MongoStorage {
    let url = url().expect("TSD_MONGO_URL is set");
    MongoStorage::connect(&url, &isolated_name(label))
        .await
        .expect("TSD_MONGO_URL is set but the server cannot be reached")
}

/// Note a skipped live test.
pub(crate) fn skip(name: &str) {
    eprintln!("{name}: TSD_MONGO_URL is not set; skipping");
}
