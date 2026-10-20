use super::*;

#[test]
fn poisoned_scope_cache_returns_a_backend_error() -> tinystoragedrivers_types::Result<()> {
    let backend = std::sync::Arc::new(MemoryBackend::new());
    let poisoner = std::sync::Arc::clone(&backend);
    let thread = std::thread::spawn(move || {
        let _guard = poisoner
            .scopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::panic::resume_unwind(Box::new("poison memory backend cache"));
    });
    let _ = thread.join();

    let scope = Scope::new("poisoned_cache")?;
    let Err(error) = backend.for_scope(&scope) else {
        return Err(StorageError::backend("poisoned scope cache was accepted"));
    };
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Backend);
    Ok(())
}
