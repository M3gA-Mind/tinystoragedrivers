//! File-backed document and stream primitive engines.

use std::path::{Path, PathBuf};

pub(crate) mod documents;
mod encode;
mod fsio;
pub(crate) mod streams;

use crate::file::encode::dir_components;

pub(crate) fn scope_dir(root: &Path, scope: &str) -> PathBuf {
    root.join("scopes").join(dir_components(scope))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
