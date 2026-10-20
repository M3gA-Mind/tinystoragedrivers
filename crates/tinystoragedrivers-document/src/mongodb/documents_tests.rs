use super::*;

#[test]
fn query_cursor_binding_includes_sort_order() {
    let scope = Scope::new("scope-a").unwrap();
    let ascending = Query::all().sort(Sort::asc("name"));
    let descending = Query::all().sort(Sort::desc("name"));
    assert_ne!(
        fingerprint("items", &scope, &ascending),
        fingerprint("items", &scope, &descending)
    );
}
