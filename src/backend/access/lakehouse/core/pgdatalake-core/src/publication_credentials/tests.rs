use std::time::{Duration, UNIX_EPOCH};

use uuid::Uuid;

use super::*;
use crate::publication_transport::{PublicationLimits, ScopedPublisher};
use crate::publication_worker::PublicationBindingIdentity;

fn target() -> PublicationTarget {
    PublicationTarget {
        binding: PublicationBindingIdentity {
            authority_id: Uuid::from_u128(1),
            binding_id: Uuid::from_u128(2),
            incarnation_id: Uuid::from_u128(3),
            binding_generation: 4,
        },
        table_uuid: Uuid::from_u128(5),
        table_root: "s3://fixture/tenant/events".into(),
        schema_id: 3,
        spec_id: 0,
        ref_name: "main".into(),
        catalog_endpoint: "http://127.0.0.1:4400".into(),
        catalog_prefix: "enrolled".into(),
        namespace: vec!["tenant".into()],
        table_name: "events".into(),
        object_endpoint: "http://127.0.0.1:9000".into(),
        s3_region: "us-east-1".into(),
    }
}

fn grant(
    target: PublicationTarget,
    purpose: PublicationPurpose,
    expires_at: SystemTime,
) -> PublicationCredentialGrant {
    let role = match purpose {
        PublicationPurpose::Send => "send",
        PublicationPurpose::Lookup => "lookup",
    };
    PublicationCredentialGrant {
        target,
        purpose,
        catalog_credential_ref: format!("fixture/catalog/{role}"),
        object_credential_ref: format!("fixture/object/{role}"),
        expires_at,
    }
}

fn material(
    catalog_expires_at: SystemTime,
    object_expires_at: SystemTime,
) -> PublicationCredentialMaterial {
    PublicationCredentialMaterial {
        catalog_bearer: "test-bearer".into(),
        catalog_expires_at,
        objects: AwsCredential {
            key_id: "fixture-key".into(),
            secret_key: "fixture-secret".into(),
            token: Some("fixture-session".into()),
        },
        object_expires_at,
    }
}

fn after_minutes(minutes: u64) -> SystemTime {
    SystemTime::now() + Duration::from_secs(minutes * 60)
}

#[test]
fn refuses_wrong_authority_generation_root_and_purpose_before_issuer() {
    let enrolled = target();
    let resolver = PublicationCredentialResolver::new([grant(
        enrolled.clone(),
        PublicationPurpose::Lookup,
        after_minutes(5),
    )])
    .unwrap();

    let mut wrong_authority = enrolled.clone();
    wrong_authority.binding.authority_id = Uuid::from_u128(42);
    let mut wrong_generation = enrolled.clone();
    wrong_generation.binding.binding_generation += 1;
    let mut wrong_incarnation = enrolled.clone();
    wrong_incarnation.binding.incarnation_id = Uuid::from_u128(43);
    let mut sibling_root = enrolled.clone();
    sibling_root.table_root.push_str("-old");
    let mut wrong_route = enrolled.clone();
    wrong_route.catalog_prefix = "other".into();
    let mut wrong_id = enrolled.clone();
    wrong_id.binding.binding_id = Uuid::from_u128(44);
    for (wrong, purpose) in [
        (wrong_authority, PublicationPurpose::Lookup),
        (wrong_generation, PublicationPurpose::Lookup),
        (wrong_incarnation, PublicationPurpose::Lookup),
        (sibling_root, PublicationPurpose::Lookup),
        (wrong_route, PublicationPurpose::Lookup),
        (wrong_id, PublicationPurpose::Lookup),
        (enrolled, PublicationPurpose::Send),
    ] {
        let refusal = resolver
            .resolve(&wrong, purpose, |_| {
                panic!("issuer called without matching grant")
            })
            .err()
            .unwrap();
        assert_eq!(refusal, PublicationCredentialError::NotApproved);
    }
}

#[test]
fn caps_bundle_at_first_expiry_and_passes_it_to_transport() {
    let enrolled = target();
    let early = after_minutes(2);
    let later = after_minutes(5);
    for (grant_expiry, catalog_expiry, object_expiry) in [
        (early, later, later),
        (later, early, later),
        (later, later, early),
    ] {
        let resolver = PublicationCredentialResolver::new([grant(
            enrolled.clone(),
            PublicationPurpose::Lookup,
            grant_expiry,
        )])
        .unwrap();
        let credentials = resolver
            .resolve(&enrolled, PublicationPurpose::Lookup, |approval| {
                assert_eq!(approval.catalog_credential_ref, "fixture/catalog/lookup");
                assert_eq!(approval.object_credential_ref, "fixture/object/lookup");
                assert!(approval.target == enrolled);
                Ok(material(catalog_expiry, object_expiry))
            })
            .unwrap();
        assert_eq!(credentials.expires_at, early);
        assert_eq!(
            credentials.objects.token.as_deref(),
            Some("fixture-session")
        );
        assert!(ScopedPublisher::new(
            enrolled.clone(),
            PublicationPurpose::Lookup,
            credentials,
            vec![("id".into(), crate::catalog::metadata::IcebergType::Long)],
            PublicationLimits {
                captured_bytes: 1024,
                captured_rows: 8,
                manifest_bytes: 64 * 1024,
                manifest_timeout: Duration::from_secs(3),
            },
        )
        .is_ok());
    }
}

#[test]
fn expiry_and_issuer_failure_refuse_the_entire_bundle() {
    let enrolled = target();
    let purpose = PublicationPurpose::Send;
    let expired =
        PublicationCredentialResolver::new([grant(enrolled.clone(), purpose, UNIX_EPOCH)]).unwrap();
    assert_eq!(
        expired
            .resolve(&enrolled, purpose, |_| panic!(
                "issuer called with expired grant"
            ))
            .err(),
        Some(PublicationCredentialError::Expired),
    );
    let live =
        PublicationCredentialResolver::new([grant(enrolled.clone(), purpose, after_minutes(2))])
            .unwrap();
    assert_eq!(
        live.resolve(&enrolled, purpose, |_| Ok(material(
            after_minutes(2),
            UNIX_EPOCH
        )))
        .err(),
        Some(PublicationCredentialError::Expired),
    );
    assert_eq!(
        live.resolve(&enrolled, purpose, |_| Err(
            PublicationCredentialError::Unavailable
        ))
        .err(),
        Some(PublicationCredentialError::Unavailable),
    );
}

#[test]
fn rejects_duplicate_grants_and_empty_references() {
    let enrolled = target();
    let purpose = PublicationPurpose::Lookup;
    assert_eq!(
        PublicationCredentialResolver::new([
            grant(enrolled.clone(), purpose, after_minutes(2)),
            grant(enrolled.clone(), purpose, after_minutes(2)),
        ])
        .err(),
        Some(PublicationCredentialError::DuplicateGrant),
    );
    let mut incomplete = grant(enrolled, purpose, after_minutes(2));
    incomplete.object_credential_ref = String::new();
    assert_eq!(
        PublicationCredentialResolver::new([incomplete]).err(),
        Some(PublicationCredentialError::InvalidGrant),
    );
}

#[test]
fn a_current_lookup_grant_can_rotate_after_send_approval_is_removed() {
    let enrolled = target();
    let expires_at = after_minutes(5);
    let mut lookup = grant(enrolled.clone(), PublicationPurpose::Lookup, expires_at);
    lookup.catalog_credential_ref = "fixture/catalog/lookup-new".into();
    let resolver = PublicationCredentialResolver::new([lookup]).unwrap();

    let send = resolver.resolve(&enrolled, PublicationPurpose::Send, |_| {
        panic!("issuer called after send approval was removed")
    });
    assert_eq!(send.err(), Some(PublicationCredentialError::NotApproved));

    let issue_count = std::cell::Cell::new(0);
    let issue = |approval: &PublicationCredentialGrant| {
        assert_eq!(
            approval.catalog_credential_ref,
            "fixture/catalog/lookup-new"
        );
        assert!(approval.purpose == PublicationPurpose::Lookup);
        issue_count.set(issue_count.get() + 1);
        let mut issued = material(expires_at, expires_at);
        issued.catalog_bearer = format!("lookup-{}", issue_count.get());
        Ok(issued)
    };
    let first = resolver
        .resolve(&enrolled, PublicationPurpose::Lookup, issue)
        .unwrap();
    let second = resolver
        .resolve(&enrolled, PublicationPurpose::Lookup, issue)
        .unwrap();
    assert_eq!(issue_count.get(), 2);
    assert!(first.target == enrolled && second.target == enrolled);
    assert_eq!(first.catalog_bearer, "lookup-1");
    assert_eq!(second.catalog_bearer, "lookup-2");
}
