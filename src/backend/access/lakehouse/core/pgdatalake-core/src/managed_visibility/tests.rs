use super::*;

const BASE: i64 = i64::MAX;
const HEAD: i64 = 9_007_199_254_740_993;
const FIRST: i64 = 9_007_199_254_740_992;
const SECOND: i64 = FIRST + 1;

fn lineage() -> ManagedReadScope {
    ManagedReadScope {
        authority_id: Uuid::from_u128(1),
        table_uuid: Uuid::from_u128(2),
        incarnation_id: Uuid::from_u128(3),
        ref_name: "main".into(),
        storage_root: "s3://warehouse/events".into(),
    }
}

fn retained(segment_id: i64) -> VisibleSegment {
    VisibleSegment {
        segment_id,
        payload_retained: true,
    }
}

#[test]
fn a_mixed_batch_can_only_read_its_older_compatible_pin() {
    let scope = lineage();
    // The catalog certifies ancestry, independent of numeric pin ordering.
    let candidates = [
        ManagedPinCandidate {
            scope: &scope,
            pin: HEAD,
            membership: PinMembership::Complete(&[FIRST, SECOND]),
        },
        ManagedPinCandidate {
            scope: &scope,
            pin: BASE,
            membership: PinMembership::Complete(&[]),
        },
    ];

    assert_eq!(
        select_compatible_pin(&scope, &candidates, &[]),
        Ok(ManagedReadCut {
            pin: BASE,
            overlay_segment_ids: vec![],
        })
    );
    assert_eq!(
        select_compatible_pin(&scope, &candidates, &[retained(FIRST)]),
        Ok(ManagedReadCut {
            pin: BASE,
            overlay_segment_ids: vec![FIRST],
        })
    );
    assert_eq!(
        select_compatible_pin(&scope, &candidates, &[retained(SECOND), retained(FIRST)]),
        Ok(ManagedReadCut {
            pin: HEAD,
            overlay_segment_ids: vec![]
        })
    );
    assert_eq!(
        select_compatible_pin(
            &scope,
            &candidates[1..],
            &[retained(SECOND), retained(FIRST)]
        ),
        Ok(ManagedReadCut {
            pin: BASE,
            overlay_segment_ids: vec![FIRST, SECOND]
        })
    );
}

#[test]
fn unknown_and_expired_pins_cannot_prove_an_empty_base() {
    let scope = lineage();
    let unavailable = [
        ManagedPinCandidate {
            scope: &scope,
            pin: HEAD,
            membership: PinMembership::Unknown,
        },
        ManagedPinCandidate {
            scope: &scope,
            pin: BASE,
            membership: PinMembership::Expired,
        },
    ];
    assert_eq!(
        select_compatible_pin(&scope, &unavailable, &[]),
        Err(ManagedReadError::NoCompatiblePin)
    );

    let fallback = [
        unavailable[0],
        ManagedPinCandidate {
            scope: &scope,
            pin: BASE,
            membership: PinMembership::Complete(&[]),
        },
    ];
    assert_eq!(
        select_compatible_pin(&scope, &fallback, &[retained(FIRST)]),
        Ok(ManagedReadCut {
            pin: BASE,
            overlay_segment_ids: vec![FIRST]
        })
    );
}

#[test]
fn invisible_published_segment_with_expired_parent_is_blocked() {
    let scope = lineage();
    let candidates = [
        ManagedPinCandidate {
            scope: &scope,
            pin: HEAD,
            membership: PinMembership::Complete(&[FIRST, SECOND]),
        },
        ManagedPinCandidate {
            scope: &scope,
            pin: BASE,
            membership: PinMembership::Expired,
        },
    ];
    assert_eq!(
        select_compatible_pin(&scope, &candidates, &[retained(FIRST)]),
        Err(ManagedReadError::NoCompatiblePin)
    );
}

#[test]
fn reclaimed_payload_cannot_be_replaced_by_an_empty_overlay() {
    let scope = lineage();
    let candidates = [
        ManagedPinCandidate {
            scope: &scope,
            pin: HEAD,
            membership: PinMembership::Complete(&[FIRST]),
        },
        ManagedPinCandidate {
            scope: &scope,
            pin: BASE,
            membership: PinMembership::Complete(&[]),
        },
    ];
    let visible = [VisibleSegment {
        segment_id: FIRST,
        payload_retained: false,
    }];
    assert_eq!(
        select_compatible_pin(&scope, &candidates, &visible),
        Ok(ManagedReadCut {
            pin: HEAD,
            overlay_segment_ids: vec![]
        })
    );
    assert_eq!(
        select_compatible_pin(&scope, &candidates[1..], &visible),
        Err(ManagedReadError::NoCompatiblePin)
    );
}

#[test]
fn changed_lineage_invalidates_the_whole_proof() {
    let scope = lineage();
    let valid = ManagedPinCandidate {
        scope: &scope,
        pin: HEAD,
        membership: PinMembership::Complete(&[]),
    };
    for other in [
        ManagedReadScope {
            incarnation_id: Uuid::from_u128(4),
            ..scope.clone()
        },
        ManagedReadScope {
            authority_id: Uuid::from_u128(4),
            ..scope.clone()
        },
        ManagedReadScope {
            table_uuid: Uuid::from_u128(4),
            ..scope.clone()
        },
        ManagedReadScope {
            ref_name: "branch".into(),
            ..scope.clone()
        },
        ManagedReadScope {
            storage_root: "s3://warehouse/rebound".into(),
            ..scope.clone()
        },
    ] {
        let changed = ManagedPinCandidate {
            scope: &other,
            pin: BASE,
            membership: PinMembership::Unknown,
        };
        assert_eq!(
            select_compatible_pin(&scope, &[valid, changed], &[]),
            Err(ManagedReadError::ScopeMismatch)
        );
    }
}

#[test]
fn malformed_or_contradictory_ids_fail_closed() {
    let scope = lineage();
    let valid = ManagedPinCandidate {
        scope: &scope,
        pin: HEAD,
        membership: PinMembership::Complete(&[FIRST]),
    };
    for invalid in [
        valid,
        ManagedPinCandidate { pin: 0, ..valid },
        ManagedPinCandidate {
            pin: BASE,
            membership: PinMembership::Complete(&[FIRST, FIRST]),
            ..valid
        },
        ManagedPinCandidate {
            pin: BASE,
            membership: PinMembership::Complete(&[SECOND]),
            ..valid
        },
        ManagedPinCandidate {
            pin: BASE,
            membership: PinMembership::Complete(&[-1]),
            ..valid
        },
    ] {
        assert_eq!(
            select_compatible_pin(
                &scope,
                &[valid, invalid],
                &[retained(FIRST), retained(SECOND)]
            ),
            Err(ManagedReadError::InvalidProof)
        );
    }
    assert_eq!(
        select_compatible_pin(&scope, &[valid], &[retained(FIRST), retained(FIRST)]),
        Err(ManagedReadError::InvalidProof)
    );
}

mod provider_ancestry {
    use super::*;
    use crate::catalog::metadata::{Snapshot, TableMetadata};
    use crate::managed_visibility::ancestry::{MAX_MANAGED_ANCESTRY_PINS, MAX_MANAGED_SNAPSHOTS};

    const SIBLING: i64 = HEAD + 1;

    fn metadata(scope: &ManagedReadScope) -> TableMetadata {
        serde_json::from_value(serde_json::json!({
            "format-version": 2,
            "table-uuid": scope.table_uuid,
            "location": scope.storage_root,
            "schemas": [],
            "current-schema-id": 0,
            "current-snapshot-id": HEAD,
            "refs": {"main": {"snapshot-id": HEAD, "type": "branch"}},
            "snapshots": [
                snapshot(HEAD, Some(BASE), 3),
                snapshot(SIBLING, Some(BASE), 2),
                snapshot(BASE, None, 1)
            ]
        }))
        .unwrap()
    }

    fn snapshot(id: i64, parent: Option<i64>, sequence: i64) -> Snapshot {
        serde_json::from_value(serde_json::json!({
            "snapshot-id": id,
            "parent-snapshot-id": parent,
            "sequence-number": sequence,
            "timestamp-ms": sequence,
            "manifest-list": format!("s3://warehouse/events/metadata/{id}.avro"),
            "summary": {"operation": "append"}
        }))
        .unwrap()
    }

    fn observe<'a>(
        scope: &'a ManagedReadScope,
        metadata: &'a TableMetadata,
    ) -> ManagedCatalogObservation<'a> {
        ManagedCatalogObservation {
            authority_id: scope.authority_id,
            incarnation_id: scope.incarnation_id,
            ref_name: &scope.ref_name,
            metadata,
        }
    }

    #[test]
    fn follows_exact_provider_parent_links_and_rejects_sibling_or_reversed_candidates() {
        let scope = lineage();
        let metadata = metadata(&scope);
        let ancestry = ManagedAncestry::certify(&scope, BASE, observe(&scope, &metadata)).unwrap();
        assert_eq!(ancestry.pins(), &[HEAD, BASE]);
        let head = ManagedPinCandidate {
            scope: &scope,
            pin: HEAD,
            membership: PinMembership::Complete(&[FIRST, SECOND]),
        };
        let base = ManagedPinCandidate {
            scope: &scope,
            pin: BASE,
            membership: PinMembership::Complete(&[]),
        };
        let visible = [retained(FIRST)];
        assert_eq!(
            ancestry.select(&[head, base], &visible),
            Ok(ManagedReadCut {
                pin: BASE,
                overlay_segment_ids: vec![FIRST],
            })
        );
        assert_eq!(
            ancestry.select(&[base, head], &visible),
            Err(ManagedReadError::InvalidProof)
        );
        assert_eq!(
            ancestry.select(
                &[ManagedPinCandidate {
                    pin: SIBLING,
                    ..base
                }],
                &visible
            ),
            Err(ManagedReadError::InvalidProof)
        );
    }

    #[test]
    fn checks_all_scope_facts_and_exact_provider_branch() {
        let scope = lineage();
        let metadata = metadata(&scope);
        for observation in [
            ManagedCatalogObservation {
                authority_id: Uuid::from_u128(9),
                ..observe(&scope, &metadata)
            },
            ManagedCatalogObservation {
                incarnation_id: Uuid::from_u128(9),
                ..observe(&scope, &metadata)
            },
            ManagedCatalogObservation {
                ref_name: "audit",
                ..observe(&scope, &metadata)
            },
        ] {
            assert_eq!(
                ManagedAncestry::certify(&scope, BASE, observation).unwrap_err(),
                ManagedReadError::ScopeMismatch
            );
        }
        for metadata in [
            TableMetadata {
                table_uuid: Uuid::from_u128(9),
                ..metadata.clone()
            },
            TableMetadata {
                location: "s3://warehouse/events-sibling".into(),
                ..metadata.clone()
            },
        ] {
            assert_eq!(
                ManagedAncestry::certify(&scope, BASE, observe(&scope, &metadata)).unwrap_err(),
                ManagedReadError::ScopeMismatch
            );
        }
        for metadata in [
            TableMetadata {
                current_snapshot_id: Some(SIBLING),
                ..metadata.clone()
            },
            {
                let mut metadata = metadata.clone();
                metadata.refs.get_mut("main").unwrap().kind = "tag".into();
                metadata
            },
        ] {
            assert_eq!(
                ManagedAncestry::certify(&scope, BASE, observe(&scope, &metadata)).unwrap_err(),
                ManagedReadError::InvalidProof
            );
        }
    }

    #[test]
    fn fails_closed_for_missing_anchor_detours_cycles_and_inconsistent_sequences() {
        let scope = lineage();
        let valid = metadata(&scope);
        for invalid_metadata in [
            TableMetadata {
                snapshots: vec![snapshot(HEAD, Some(HEAD), 3), snapshot(BASE, None, 1)],
                ..valid.clone()
            },
            TableMetadata {
                snapshots: vec![snapshot(HEAD, Some(BASE), 3), snapshot(BASE, None, 3)],
                ..valid.clone()
            },
            TableMetadata {
                snapshots: vec![
                    snapshot(HEAD, Some(BASE), 3),
                    snapshot(BASE, None, 1),
                    snapshot(BASE, None, 2),
                ],
                ..valid.clone()
            },
            {
                let mut metadata = valid.clone();
                metadata.snapshots[0].manifest_list = "s3://warehouse/other/metadata/1.avro".into();
                metadata
            },
            {
                let mut metadata = valid.clone();
                metadata.snapshots[0]
                    .summary
                    .insert("operation".into(), "overwrite".into());
                metadata
            },
        ] {
            assert_eq!(
                ManagedAncestry::certify(&scope, BASE, observe(&scope, &invalid_metadata))
                    .unwrap_err(),
                ManagedReadError::InvalidProof
            );
        }
        for missing in [
            TableMetadata {
                snapshots: vec![snapshot(HEAD, Some(SIBLING), 3), snapshot(BASE, None, 1)],
                ..valid.clone()
            },
            TableMetadata {
                snapshots: vec![snapshot(HEAD, None, 3), snapshot(BASE, None, 1)],
                ..valid
            },
        ] {
            assert_eq!(
                ManagedAncestry::certify(&scope, BASE, observe(&scope, &missing)).unwrap_err(),
                ManagedReadError::AncestryUnavailable
            );
        }
    }

    #[test]
    fn caps_snapshot_inventory_and_walk_before_accepting_a_pin() {
        let scope = lineage();
        let mut metadata = metadata(&scope);
        metadata.snapshots = vec![snapshot(BASE, None, 1); MAX_MANAGED_SNAPSHOTS + 1];
        assert_eq!(
            ManagedAncestry::certify(&scope, BASE, observe(&scope, &metadata)).unwrap_err(),
            ManagedReadError::AncestryLimitExceeded
        );

        let head = i64::try_from(MAX_MANAGED_ANCESTRY_PINS).unwrap() + 1;
        metadata.current_snapshot_id = Some(head);
        metadata.refs.get_mut("main").unwrap().snapshot_id = head;
        metadata.snapshots = (1..=head)
            .map(|id| snapshot(id, (id > 1).then_some(id - 1), id))
            .collect();
        assert_eq!(
            ManagedAncestry::certify(&scope, 1, observe(&scope, &metadata)).unwrap_err(),
            ManagedReadError::AncestryLimitExceeded
        );
        let bounded = ManagedAncestry::certify(&scope, 2, observe(&scope, &metadata)).unwrap();
        assert_eq!(bounded.pins().len(), MAX_MANAGED_ANCESTRY_PINS);
        assert_eq!(bounded.pins().last(), Some(&2));
    }
}
