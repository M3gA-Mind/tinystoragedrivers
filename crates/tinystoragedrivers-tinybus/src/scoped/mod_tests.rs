//! Native dispatch and guarded registration preserve the finite callback surface.
use super::*;
use serde_json::json;
use tinybus::{broker::Broker, module::ModuleHost};
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
async fn service() -> (HostCallbackBinding, ScopedService) {
    let backend = MemoryStorage::new();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    let server = Arc::new(StorageServer::new(storage));
    let host = ModuleHost::new(Broker::new());
    let endpoints = scoped_storage_endpoints("/storage").unwrap();
    assert_eq!(endpoints.len(), 3);
    let binding = host
        .prepare_host_callbacks(endpoints.clone())
        .await
        .unwrap();
    serve_scoped_storage(&binding, "/storage".parse().unwrap(), server.clone())
        .await
        .unwrap();
    assert!(
        serve_scoped_storage(&binding, "/storage".parse().unwrap(), server.clone())
            .await
            .is_err()
    );
    assert!(scoped_storage_endpoints("bad").is_err());
    assert_eq!(
        scoped_storage_endpoints("/").unwrap()[0].path.as_str(),
        "/Documents"
    );
    assert_eq!(selectors(Object::Blobs), [] as [&str; 0]);
    let endpoint = &endpoints[0];
    let service = ScopedService {
        server,
        guard: binding.guard(),
        object: Object::Documents,
        interface: endpoint.interface.clone(),
        members: endpoint.members.clone(),
    };
    (binding, service)
}
#[tokio::test]
async fn captured_server_dispatches_native_metadata_and_refuses_extensions() {
    let (binding, service) = service().await;
    let endpoint = scoped_storage_endpoints("/storage").unwrap().remove(0);
    assert_eq!(service.name(), endpoint.interface);
    assert_eq!(service.members(), endpoint.members);
    assert!(service.requires_confidential(&"Get".parse().unwrap()));
    assert!(
        service
            .call(&"Get".parse().unwrap(), Value::Null)
            .await
            .is_err()
    );
    assert!(
        service
            .call_with_context(
                &"Get".parse().unwrap(),
                Value::Null,
                &CallContext::default()
            )
            .await
            .is_err()
    );
    let caps = service
        .dispatch(&"Capabilities".parse().unwrap(), json!([null]))
        .await
        .unwrap();
    assert!(
        caps["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!("record_metadata"))
    );
    for name in ["WatchOpen", "WatchClose", "Bogus"] {
        assert!(
            service
                .dispatch(&name.parse().unwrap(), json!([null]))
                .await
                .is_err()
        );
    }
    assert!(
        service
            .dispatch(&"Get".parse().unwrap(), json!({"private":"sentinel"}))
            .await
            .is_err()
    );
    let put = json!([{"collection":"records","document_id":"key","document_json":"{}","schema":4,"precondition":{"kind":"absent"}}]);
    service
        .dispatch(&"Put".parse().unwrap(), put.clone())
        .await
        .unwrap();
    let error = service
        .dispatch(&"Put".parse().unwrap(), put)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("conflict"));
    let get = service
        .dispatch(
            &"Get".parse().unwrap(),
            json!([{"collection":"records","document_id":"key"}]),
        )
        .await
        .unwrap();
    assert_eq!(get["meta"]["schema"], 4);
    binding.revoke();
    assert!(
        serve_scoped_storage(
            &binding,
            "/storage".parse().unwrap(),
            service.server.clone()
        )
        .await
        .is_err()
    );
}
