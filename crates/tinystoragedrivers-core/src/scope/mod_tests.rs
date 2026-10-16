//! Scope validation, the local scope, and the serde representation.

use super::*;
use crate::ErrorKind;

#[test]
fn accepts_printable_values() {
    let scope = Scope::new("user:42/agent-a").unwrap();
    assert_eq!(scope.as_str(), "user:42/agent-a");
    assert_eq!(scope.as_ref(), "user:42/agent-a");
    assert_eq!(scope.to_string(), "user:42/agent-a");
    assert_eq!(format!("{scope:?}"), "Scope(\"user:42/agent-a\")");
    assert!(!scope.is_local());
}

#[test]
fn rejects_empty_long_and_spaced_values() {
    for bad in [
        String::new(),
        "a".repeat(MAX_SCOPE_LEN + 1),
        "has space".to_owned(),
        "tab\there".to_owned(),
        "nul\0".to_owned(),
    ] {
        let error = Scope::new(&bad).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{bad:?}");
    }
    assert!(Scope::new("a".repeat(MAX_SCOPE_LEN)).is_ok());
}

#[test]
fn local_is_a_fixed_value() {
    let local = Scope::local();
    assert!(local.is_local());
    assert_eq!(local, Scope::new("local").unwrap());
    assert_eq!(local.as_str(), Scope::LOCAL);
}

#[test]
fn serializes_as_a_plain_string() {
    let scope = Scope::new("tenant-1").unwrap();
    assert_eq!(serde_json::to_string(&scope).unwrap(), "\"tenant-1\"");
    let back: Scope = serde_json::from_str("\"tenant-1\"").unwrap();
    assert_eq!(back, scope);
    assert!(serde_json::from_str::<Scope>("\"\"").is_err());
}

fn scope(text: &str) -> Scope {
    Scope::new(text).unwrap()
}

fn hashed_segment(raw: &str) -> String {
    let scope = Scope::tenant(raw).unwrap();
    scope.as_str().strip_prefix("u:").unwrap().to_owned()
}

#[test]
fn grammar_constants_are_the_spec_literals() {
    assert_eq!(USER_PREFIX, "u:");
    assert_eq!(AGENT_INFIX, "/agent:");
    assert_eq!(MODULE_INFIX, "/module:");
    assert_eq!(CLUSTER, "cluster");
    assert_eq!(MAX_SEGMENT_LEN, 96);
}

#[test]
fn builds_every_scope_form() {
    assert_eq!(Scope::local().as_str(), "local");
    assert_eq!(Scope::cluster().as_str(), "cluster");
    assert_eq!(Scope::tenant("alice").unwrap().as_str(), "u:alice");
    let tenant = Scope::tenant("a-b_c.d@e").unwrap();
    assert_eq!(tenant.as_str(), "u:a-b_c.d@e");
    assert_eq!(
        tenant.agent("bot").unwrap().as_str(),
        "u:a-b_c.d@e/agent:bot"
    );
    assert_eq!(
        tenant.module("mem").unwrap().as_str(),
        "u:a-b_c.d@e/module:mem"
    );
    assert_eq!(
        Scope::local().agent("bot").unwrap().as_str(),
        "local/agent:bot"
    );
    assert_eq!(
        Scope::local().module("mem").unwrap().as_str(),
        "local/module:mem"
    );
    assert!(Scope::cluster().is_cluster());
    assert!(!Scope::local().is_cluster());
}

#[test]
fn parse_round_trips_every_form() {
    let tenant = Scope::tenant("alice").unwrap();
    let forms = [
        (Scope::local(), Tenant::Local, None),
        (Scope::cluster(), Tenant::Cluster, None),
        (tenant.clone(), Tenant::User("alice".into()), None),
        (
            tenant.agent("bot").unwrap(),
            Tenant::User("alice".into()),
            Some(Principal::Agent("bot".into())),
        ),
        (
            tenant.module("mem").unwrap(),
            Tenant::User("alice".into()),
            Some(Principal::Module("mem".into())),
        ),
        (
            Scope::local().agent("bot").unwrap(),
            Tenant::Local,
            Some(Principal::Agent("bot".into())),
        ),
        (
            Scope::local().module("mem").unwrap(),
            Tenant::Local,
            Some(Principal::Module("mem".into())),
        ),
    ];
    for (built, tenant, principal) in forms {
        let parsed = Scope::parse(built.as_str()).unwrap();
        assert_eq!(parsed, ParsedScope { tenant, principal }, "{built}");
        assert_eq!(parsed.to_scope().unwrap(), built, "{built}");
        assert_eq!(parsed.to_string(), built.as_str());
        assert_eq!(built.parsed().unwrap(), parsed);
    }
}

#[test]
fn segment_of_exactly_the_limit_is_kept_and_one_more_is_hashed() {
    let at_limit = "x".repeat(MAX_SEGMENT_LEN);
    assert_eq!(hashed_segment(&at_limit), at_limit);

    let over = "x".repeat(MAX_SEGMENT_LEN + 1);
    assert_eq!(hashed_segment(&over), "~dgupa7xlp46qyc3to7rx24xmzx");
}

#[test]
fn illegal_characters_are_hashed_to_a_pinned_value() {
    assert_eq!(hashed_segment("a b"), "~zbuhucfklvxnebcdfd5gu2l2xd");
    assert_eq!(hashed_segment("é"), "~jkmvk7sagpbvhhpc5nsuoiaxzl");
    for raw in ["a/b", "a:b", "a~b", "a\0b", "a\tb", "ünï"] {
        let segment = hashed_segment(raw);
        assert_eq!(segment.len(), 27, "{raw:?}");
        assert!(segment.starts_with('~'), "{raw:?}");
        assert!(
            segment[1..]
                .bytes()
                .all(|b| matches!(b, b'a'..=b'z' | b'2'..=b'7')),
            "{raw:?}"
        );
    }
}

#[test]
fn hashed_segments_are_deterministic_and_distinct() {
    assert_eq!(hashed_segment("a b"), hashed_segment("a b"));
    assert_ne!(hashed_segment("a b"), hashed_segment("a  b"));
}

#[test]
fn rejects_an_empty_segment() {
    assert_eq!(
        Scope::tenant("").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let tenant = Scope::tenant("a").unwrap();
    assert_eq!(
        tenant.agent("").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        tenant.module("").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn a_hashed_agent_stays_under_its_tenant() {
    let tenant = Scope::tenant("a").unwrap();
    let agent = tenant.agent("not a legal segment").unwrap();
    assert!(agent.as_str().starts_with("u:a/agent:~"), "{agent}");
    assert_eq!(agent.tenant_scope().unwrap(), tenant);

    let long_tenant = Scope::tenant("t".repeat(200)).unwrap();
    let module = long_tenant.module("m").unwrap();
    assert!(
        module
            .as_str()
            .starts_with(&format!("{long_tenant}/module:"))
    );
    assert_eq!(module.tenant_scope().unwrap(), long_tenant);
}

#[test]
fn tenant_scope_strips_the_principal() {
    let tenant = Scope::tenant("a").unwrap();
    assert_eq!(tenant.tenant_scope().unwrap(), tenant);
    assert_eq!(tenant.agent("x").unwrap().tenant_scope().unwrap(), tenant);
    assert_eq!(tenant.module("x").unwrap().tenant_scope().unwrap(), tenant);
    assert_eq!(Scope::local().tenant_scope().unwrap(), Scope::local());
    assert_eq!(
        Scope::local().agent("x").unwrap().tenant_scope().unwrap(),
        Scope::local()
    );
    assert_eq!(Scope::cluster().tenant_scope(), None);
    assert_eq!(scope("profile:x").tenant_scope(), None);
}

#[test]
fn tenants_that_share_a_prefix_stay_separate() {
    let a = Scope::tenant("a").unwrap();
    let ab = Scope::tenant("ab").unwrap();
    assert_ne!(a.tenant_scope(), ab.tenant_scope());
    let agent_of_a = a.agent("ab").unwrap();
    let agent_of_ab = ab.agent("a").unwrap();
    assert_eq!(agent_of_a.tenant_scope().unwrap(), a);
    assert_eq!(agent_of_ab.tenant_scope().unwrap(), ab);
    assert_eq!(
        Scope::parse("u:a/agent:ab").unwrap().tenant,
        Tenant::User("a".into())
    );
    assert_eq!(
        Scope::parse("u:ab").unwrap().tenant,
        Tenant::User("ab".into())
    );
}

#[test]
fn a_principal_cannot_be_added_to_a_scope_that_has_one_or_to_cluster() {
    let agent = Scope::tenant("a").unwrap().agent("x").unwrap();
    assert_eq!(
        agent.agent("y").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        agent.module("y").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let cluster = Scope::cluster();
    assert_eq!(
        cluster.agent("y").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        cluster.module("y").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        scope("profile:x").agent("y").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn v1_strings_are_still_accepted_but_are_not_grammar_scopes() {
    for v1 in [
        "profile:x",
        "a~b",
        "default",
        "module:x@local",
        "agent:alice",
        "user:42/agent-a",
        "alice",
    ] {
        let scope = Scope::new(v1).unwrap();
        assert_eq!(scope.as_str(), v1);
        assert_eq!(
            scope.parsed().unwrap_err().kind(),
            ErrorKind::InvalidInput,
            "{v1}"
        );
        assert_eq!(
            Scope::parse(v1).unwrap_err().kind(),
            ErrorKind::InvalidInput,
            "{v1}"
        );
    }
}

#[test]
fn parse_rejects_malformed_grammar_strings() {
    for bad in [
        "",
        "u:",
        "u:a/",
        "u:a/agent:",
        "u:a/agent:x/agent:y",
        "u:a/agent:x/module:y",
        "u:a/other:x",
        "u:a/agent:x/",
        "local/",
        "local/agent:",
        "cluster/agent:x",
        "u:a b",
        "u:a/b",
        "u:~short",
        "u:~ZBUHUCFKLVXNEBCDFD5GU2L2XD",
        "u:~zbuhucfklvxnebcdfd5gu2l2x",
        "u:~zbuhucfklvxnebcdfd5gu2l2xde",
        "u:~zbuhucfklvxnebcdfd5gu2l2x1",
        "u:aé",
    ] {
        assert_eq!(
            Scope::parse(bad).unwrap_err().kind(),
            ErrorKind::InvalidInput,
            "{bad:?}"
        );
    }
    let too_long = format!("u:{}", "x".repeat(MAX_SEGMENT_LEN + 1));
    assert_eq!(
        Scope::parse(&too_long).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn parse_accepts_hashed_segments() {
    let parsed =
        Scope::parse("u:~zbuhucfklvxnebcdfd5gu2l2xd/agent:~jkmvk7sagpbvhhpc5nsuoiaxzl").unwrap();
    assert_eq!(
        parsed.tenant,
        Tenant::User("~zbuhucfklvxnebcdfd5gu2l2xd".into())
    );
    assert_eq!(
        parsed.principal,
        Some(Principal::Agent("~jkmvk7sagpbvhhpc5nsuoiaxzl".into()))
    );
}
