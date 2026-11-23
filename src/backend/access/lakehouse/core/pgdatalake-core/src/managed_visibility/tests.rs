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
