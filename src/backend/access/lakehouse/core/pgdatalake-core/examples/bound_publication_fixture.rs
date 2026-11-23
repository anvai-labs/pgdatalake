//! Disposable M2 test worker. The hosting fixture supplies a pre-enrolled
//! Lakekeeper target and explicitly captured ledger rows. This example is
//! not a SQL API or the production publisher.

use std::{
    cell::OnceCell,
    io::{self, Write},
    process::{Command, Stdio},
    time::Duration,
};

use pgdatalake_core::{
    catalog::{
        commit::PreparedPublication,
        metadata::{IcebergType, TableMetadata},
        publication::{
            AuthorizedPublication, PublicationIdentity, PublicationLookup, PublicationStorage,
        },
        rest::{RestCatalog, RestError, StaticToken},
    },
    managed_visibility::{
        ManagedAncestry, ManagedCatalogObservation, ManagedPinCandidate, ManagedReadError,
        ManagedReadScope, PinMembership, VisibleSegment,
    },
    parquet::CellValue,
    publication_credentials::{
        PublicationCredentialError, PublicationCredentialGrant, PublicationCredentialMaterial,
        PublicationCredentialResolver,
    },
    publication_transport::{
        PublicationLimits, PublicationPurpose, PublicationTarget, ScopedPublisher,
    },
    publication_worker::{
        dispatch_publication_lookup, dispatch_publication_send, drive_publication,
        reconcile_publication, CapturedMember, ClaimState, LedgerResult, PendingReason,
        PublicationBindingIdentity, PublicationCatalog, PublicationClaim, PublicationDispatchHint,
        PublicationDispatchMode, PublicationLedger, RetainedAttempt, WorkerOutcome,
    },
    session::{CatalogClient, S3ClientOptions},
    snapshot_write::NewDataFile,
    storage::{parse_location, Location, S3Options, StorageError, StorageIo},
    writer::{BatchWriter, WriteColumn},
};
use serde::Deserialize;
use uuid::Uuid;

type FixtureResult<T> = Result<T, Box<dyn std::error::Error>>;
const MANIFEST_BUDGET: (u64, Duration) = (1024 * 1024, Duration::from_secs(4));
const CAPTURED_ROWS: usize = 1024;
const CAPTURED_BYTES: usize = 1024 * 1024;

#[derive(Deserialize)]
struct Fixture {
    database: String,
    operation_id: Uuid,
    #[serde(default)]
    admitted_transport: bool,
    #[serde(default)]
    lookup_only: bool,
    approved_target: Option<BindingDescriptor>,
    dispatch_hint: Option<PublicationDispatchHint>,
    member_byte_budget: Option<usize>,
    #[serde(flatten)]
    legacy: Option<LegacyTarget>,
}

#[derive(Deserialize)]
struct LegacyTarget {
    endpoint: String,
    namespace: String,
    table_uuid: Uuid,
    authority_id: Uuid,
    root: String,
}

#[derive(Deserialize)]
struct BindingIdentity {
    authority_id: Uuid,
    binding_id: Uuid,
    incarnation_id: Uuid,
    binding_generation: i64,
}

impl From<BindingIdentity> for PublicationBindingIdentity {
    fn from(binding: BindingIdentity) -> Self {
        Self {
            authority_id: binding.authority_id,
            binding_id: binding.binding_id,
            incarnation_id: binding.incarnation_id,
            binding_generation: binding.binding_generation,
        }
    }
}

#[derive(Deserialize)]
struct BindingDescriptor {
    #[serde(flatten)]
    identity: BindingIdentity,
    table_uuid: Uuid,
    storage_root: String,
    schema_id: i64,
    spec_id: i32,
    ref_name: String,
    catalog_endpoint: String,
    catalog_prefix: String,
    namespace_components: Vec<String>,
    table_name: String,
    object_endpoint: String,
    s3_region: String,
}

impl From<BindingDescriptor> for PublicationTarget {
    fn from(descriptor: BindingDescriptor) -> Self {
        Self {
            binding: descriptor.identity.into(),
            table_uuid: descriptor.table_uuid,
            table_root: descriptor.storage_root,
            schema_id: descriptor.schema_id,
            spec_id: descriptor.spec_id,
            ref_name: descriptor.ref_name,
            catalog_endpoint: descriptor.catalog_endpoint,
            catalog_prefix: descriptor.catalog_prefix,
            namespace: descriptor.namespace_components,
            table_name: descriptor.table_name,
            object_endpoint: descriptor.object_endpoint,
            s3_region: descriptor.s3_region,
        }
    }
}

#[derive(Deserialize)]
struct Claim {
    input_digest: String,
    authority_id: Uuid,
    table_uuid: Uuid,
    owner_generation: i64,
    binding: Option<BindingIdentity>,
    verified_snapshot_id: Option<i64>,
    attempt: Option<Attempt>,
}

#[derive(Deserialize)]
struct Attempt {
    expected_parent: Option<i64>,
    proposed_snapshot_id: i64,
    proposal_digest: String,
    proposal_body: Option<String>,
}

#[derive(Deserialize)]
struct LedgerMember {
    segment_id: i64,
    row_count: i32,
    payload: String,
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("writing into a String");
    }
    output
}

fn unhex(value: &str) -> FixtureResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid fixture hex value".into());
    }
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect()
}

fn digest(value: &str) -> FixtureResult<[u8; 32]> {
    unhex(value)?
        .try_into()
        .map_err(|_| "expected SHA-256 bytes in fixture ledger".into())
}

fn s3_options() -> S3Options {
    S3Options {
        endpoint: Some("http://127.0.0.1:9000".into()),
        region: Some("us-east-1".into()),
        access_key_id: Some("lakehouse-test".into()),
        secret_access_key: Some("lakehouse-test-password".into()),
        path_style: true,
    }
}

impl Fixture {
    fn captured_bytes(&self) -> usize {
        self.member_byte_budget.unwrap_or(CAPTURED_BYTES)
    }

    fn legacy_target(&self) -> FixtureResult<&LegacyTarget> {
        self.legacy
            .as_ref()
            .ok_or("legacy fixture target is missing".into())
    }

    fn sql(&self, statement: &str) -> FixtureResult<String> {
        let mut child = Command::new(std::env::var_os("PSQL").unwrap_or_else(|| "psql".into()))
            .args(["-XqAt", "-v", "ON_ERROR_STOP=1", "-d", &self.database])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or("fixture psql input is missing")?
            .write_all(statement.as_bytes())?;
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Err(format!("fixture psql exited {}", output.status).into());
        }
        Ok(String::from_utf8(output.stdout)?.trim().into())
    }
}

impl PublicationLedger for Fixture {
    fn load(&self, operation: Uuid) -> LedgerResult<PublicationClaim> {
        let source = if self.admitted_transport {
            format!(
                "SELECT j.*, json_build_object('authority_id', j.authority_id,
                         'binding_id', j.binding_id, 'incarnation_id', j.incarnation_id,
                         'binding_generation', j.binding_generation) AS binding
                 FROM lakehouse.load_managed_publication_job('{operation}') j"
            )
        } else {
            format!(
                "SELECT c.input_digest, c.authority_id, c.table_uuid, c.owner_generation,
                        c.verified_snapshot_id, NULL::json AS binding, a.expected_parent,
                        a.proposed_snapshot_id, a.proposal_digest, a.proposal_body
                 FROM lakehouse.publication_claims c
                 LEFT JOIN lakehouse.publication_attempts a USING (operation_id)
                 WHERE c.operation_id = '{operation}'"
            )
        };
        let claim: Claim = serde_json::from_str(&self.sql(&format!(
            "SELECT json_build_object('input_digest', encode(j.input_digest, 'hex'),
                        'authority_id', j.authority_id, 'table_uuid', j.table_uuid,
                        'owner_generation', j.owner_generation, 'binding', j.binding,
                        'verified_snapshot_id', j.verified_snapshot_id,
                        'attempt', CASE WHEN j.proposed_snapshot_id IS NOT NULL THEN
                            json_build_object('expected_parent', j.expected_parent,
                                'proposed_snapshot_id', j.proposed_snapshot_id,
                                'proposal_digest', encode(j.proposal_digest, 'hex'),
                                'proposal_body', encode(j.proposal_body, 'hex')) END)
             FROM ({source}) j;"
        ))?)?;
        if self.admitted_transport {
            let scope = claim
                .binding
                .as_ref()
                .ok_or("admitted claim has no capture binding")?;
            if scope.authority_id != claim.authority_id {
                return Err("claim and capture binding authorities differ".into());
            }
        } else {
            let legacy = self.legacy_target()?;
            if claim.authority_id != legacy.authority_id || claim.table_uuid != legacy.table_uuid {
                return Err("claim differs from the legacy fixture target".into());
            }
        }
        let state = if let Some(snapshot_id) = claim.verified_snapshot_id {
            ClaimState::Complete { snapshot_id }
        } else if let Some(attempt) = claim.attempt {
            ClaimState::Reserved(RetainedAttempt {
                expected_parent: attempt.expected_parent,
                snapshot_id: attempt.proposed_snapshot_id,
                request_digest: digest(&attempt.proposal_digest)?,
                request_body: attempt.proposal_body.as_deref().map(unhex).transpose()?,
            })
        } else {
            ClaimState::Unsent
        };
        Ok(PublicationClaim {
            identity: PublicationIdentity {
                operation_id: operation,
                table_uuid: claim.table_uuid,
                input_digest: digest(&claim.input_digest)?,
            },
            binding: claim.binding.map(Into::into),
            generation: claim.owner_generation,
            state,
        })
    }

    fn acquire(&self, claim: &PublicationClaim) -> LedgerResult<i64> {
        Ok(self
            .sql(&format!(
                "SELECT lakehouse.advance_publication_generation('{}', {});",
                claim.identity.operation_id, claim.generation,
            ))?
            .parse()?)
    }

    fn members(&self, claim: &PublicationClaim) -> LedgerResult<Vec<CapturedMember>> {
        let op = claim.identity.operation_id;
        let source = if self.admitted_transport {
            format!(
                "SELECT * FROM lakehouse.load_managed_publication_members(
                                '{op}', {}, {CAPTURED_ROWS}, {})",
                claim.generation,
                self.captured_bytes()
            )
        } else {
            format!(
                "SELECT s.seg_id AS segment_id, s.row_count, s.payload
                 FROM lakehouse.publication_members m
                 JOIN lakehouse.lakehouse_delta_segments s USING (seg_id)
                 WHERE m.operation_id = '{op}'"
            )
        };
        let members: Vec<LedgerMember> = serde_json::from_str(&self.sql(&format!(
            "SELECT json_agg(json_build_object('segment_id', s.segment_id,
                        'row_count', s.row_count,
                        'payload', encode(s.payload, 'hex')) ORDER BY s.segment_id)
             FROM ({source}) s;"
        ))?)?;
        members
            .into_iter()
            .map(|member| {
                Ok(CapturedMember {
                    segment_id: member.segment_id,
                    row_count: member.row_count,
                    payload: unhex(&member.payload)?,
                })
            })
            .collect()
    }

    fn authorize(
        &self,
        claim: &PublicationClaim,
        intent: AuthorizedPublication<'_>,
    ) -> LedgerResult<()> {
        let parent = intent
            .expected_parent
            .map_or_else(|| "NULL".into(), |value| value.to_string());
        self.sql(&format!(
            "SET synchronous_commit = off;
             CALL lakehouse.authorize_publication_send(
                '{}', {}, decode('{}', 'hex'), {parent}, {},
                decode('{}', 'hex'), decode('{}', 'hex'), NULL);",
            claim.identity.operation_id,
            claim.generation,
            hex(&claim.identity.input_digest),
            intent.snapshot_id,
            hex(&intent.request_digest),
            hex(intent.request_body),
        ))?;
        Ok(())
    }

    fn finalize(
        &self,
        claim: &PublicationClaim,
        intent: AuthorizedPublication<'_>,
    ) -> LedgerResult<usize> {
        let authority = match claim.binding {
            Some(binding) => binding.authority_id,
            None => self.legacy_target()?.authority_id,
        };
        Ok(self
            .sql(&format!(
                "SET synchronous_commit = local;
             SELECT lakehouse.finalize_publication(
                '{}', {}, '{}', '{}', decode('{}', 'hex'), {}, decode('{}', 'hex'));",
                claim.identity.operation_id,
                claim.generation,
                authority,
                claim.identity.table_uuid,
                hex(&claim.identity.input_digest),
                intent.snapshot_id,
                hex(&intent.request_digest),
            ))?
            .parse()?)
    }
}

struct FixtureCatalog<'a> {
    target: &'a Fixture,
    writer: Option<CatalogClient>,
    scoped: OnceCell<ScopedPublisher>,
    resolver: Option<PublicationCredentialResolver>,
}

impl FixtureCatalog<'_> {
    fn admitted_publisher(
        &self,
        query: &str,
        purpose: PublicationPurpose,
    ) -> FixtureResult<ScopedPublisher> {
        let descriptor: BindingDescriptor = serde_json::from_str(&self.target.sql(query)?)?;
        let target = descriptor.into();
        let credentials = self
            .resolver
            .as_ref()
            .ok_or("fixture needs independent credential approval")?
            .resolve(&target, purpose, issue_fixture_credentials)?;
        Ok(ScopedPublisher::new(
            target,
            purpose,
            credentials,
            vec![("id".into(), IcebergType::Long)],
            PublicationLimits {
                captured_bytes: self.target.captured_bytes(),
                captured_rows: CAPTURED_ROWS,
                manifest_bytes: MANIFEST_BUDGET.0,
                manifest_timeout: MANIFEST_BUDGET.1,
            },
        )?)
    }
}

impl PublicationCatalog for FixtureCatalog<'_> {
    fn prepare(
        &self,
        claim: &PublicationClaim,
        members: &[CapturedMember],
    ) -> Result<PreparedPublication, StorageError> {
        if self.target.admitted_transport {
            let catalog = self
                .admitted_publisher(
                    &format!(
                        "SELECT row_to_json(d) FROM lakehouse.get_publication_send_binding('{}', {}) AS d;",
                        claim.identity.operation_id, claim.generation,
                    ),
                    PublicationPurpose::Send,
                )
                .map_err(|error| {
                    StorageError::Config(format!("fixture send admission: {error}"))
                })?;
            self.scoped.set(catalog).map_err(|_| {
                StorageError::Config("fixture prepared twice in one worker invocation".into())
            })?;
            return self.scoped.get().expect("just set").prepare(claim, members);
        }
        let writer_client = self.writer.as_ref().expect("fixture writer");
        let target = self.target.legacy_target().expect("fixture target");
        let namespace = &[target.namespace.as_str()];
        let mut rows = Vec::new();
        for member in members {
            let payload: Vec<[i64; 1]> = serde_json::from_slice(&member.payload)
                .map_err(|error| StorageError::Decode(error.to_string()))?;
            if payload.len() != member.row_count as usize {
                return Err(StorageError::Decode("fixture row count mismatch".into()));
            }
            rows.extend(payload);
        }
        let pin = writer_client.write_table_open(
            namespace,
            "events",
            &[("id".into(), IcebergType::Long)],
        )?;
        if pin.table_uuid != claim.identity.table_uuid
            || pin.location.trim_end_matches('/') != target.root
        {
            return Err(StorageError::Config(
                "writer pin differs from fixture enrollment".into(),
            ));
        }
        let location = parse_location(&target.root)?;
        let Location::S3 { bucket, key } = &location else {
            return Err(StorageError::Config(
                "the fixture requires S3 storage".into(),
            ));
        };
        let store = StorageIo::open(&location, Some(&pin.s3))?;
        let mut writer = BatchWriter::new(vec![WriteColumn {
            name: "id".into(),
            ty: IcebergType::Long,
            field_id: Some(
                *pin.field_ids
                    .get("id")
                    .ok_or_else(|| StorageError::Config("fixture column not found".into()))?,
            ),
        }])?;
        for [value] in rows {
            writer.push(&[CellValue::Int64(value)])?;
        }
        let file = writer.write_file(&store, &format!("{key}/data"), 0)?;
        writer_client.prepare_staged_append(
            &pin,
            namespace,
            "events",
            &[NewDataFile {
                file_path: format!("s3://{bucket}/{}", file.key),
                record_count: file.record_count,
                file_size_in_bytes: file.file_size_in_bytes,
                ..NewDataFile::default()
            }],
            claim.identity,
            MANIFEST_BUDGET,
        )
    }

    fn lookup(
        &self,
        claim: &PublicationClaim,
        intent: AuthorizedPublication<'_>,
    ) -> Result<PublicationLookup, RestError> {
        if self.target.admitted_transport {
            return self
                .admitted_publisher(
                    &format!(
                        "SELECT row_to_json(d) FROM lakehouse.get_publication_lookup_binding('{}') AS d;",
                        claim.identity.operation_id,
                    ),
                    PublicationPurpose::Lookup,
                )
                .map_err(|error| RestError::Http(format!("fixture lookup admission: {error}")))?
                .lookup(claim, intent);
        }
        let target = self.target.legacy_target().expect("fixture target");
        let catalog = RestCatalog::new(&target.endpoint, Box::new(StaticToken(String::new())))?;
        catalog.fetch_config(Some("smoke"))?;
        catalog.lookup_staged_append_on_storage(
            &[&target.namespace],
            "events",
            intent,
            &PublicationStorage {
                table_root: &target.root,
                s3: &s3_options(),
                max_bytes: MANIFEST_BUDGET.0,
                read_timeout: MANIFEST_BUDGET.1,
            },
        )
    }
}

fn issue_fixture_credentials(
    grant: &PublicationCredentialGrant,
) -> Result<PublicationCredentialMaterial, PublicationCredentialError> {
    let (bearer, key, secret) = match (
        grant.catalog_credential_ref.as_str(),
        grant.object_credential_ref.as_str(),
    ) {
        ("fixture-send", "lakehouse-test") => {
            ("fixture-send", "lakehouse-test", "lakehouse-test-password")
        }
        ("fixture-lookup", "lakehouse-reader") => (
            "fixture-lookup",
            "lakehouse-reader",
            "lakehouse-reader-password",
        ),
        _ => return Err(PublicationCredentialError::Unavailable),
    };
    let expires_at = std::time::SystemTime::now() + Duration::from_secs(60);
    Ok(PublicationCredentialMaterial {
        catalog_bearer: bearer.into(),
        catalog_expires_at: expires_at,
        objects: object_store::aws::AwsCredential {
            key_id: key.into(),
            secret_key: secret.into(),
            token: None,
        },
        object_expires_at: expires_at,
    })
}

#[derive(Deserialize)]
struct ManagedCutFixture {
    scope: ManagedReadScope,
    catalog: CutCatalogObservation,
    candidates: Vec<CutCandidate>,
    visible: Vec<CutSegment>,
}

#[derive(Deserialize)]
struct CutCatalogObservation {
    authority_id: Uuid,
    incarnation_id: Uuid,
    ref_name: String,
    #[serde(deserialize_with = "exact_bigint")]
    anchor: i64,
    metadata: TableMetadata,
}

#[derive(Deserialize)]
struct CutCandidate {
    scope: ManagedReadScope,
    #[serde(deserialize_with = "exact_bigint")]
    pin: i64,
    membership: CutMembership,
}

#[derive(Deserialize)]
#[serde(tag = "proof", rename_all = "snake_case")]
enum CutMembership {
    Complete {
        #[serde(deserialize_with = "exact_bigints")]
        included: Vec<i64>,
    },
    Unknown,
    Expired,
}

#[derive(Deserialize)]
struct CutSegment {
    #[serde(deserialize_with = "exact_bigint")]
    segment_id: i64,
    payload_retained: bool,
}

fn exact_bigint<'de, D: serde::Deserializer<'de>>(input: D) -> Result<i64, D::Error> {
    String::deserialize(input)?
        .parse()
        .map_err(serde::de::Error::custom)
}

fn exact_bigints<'de, D: serde::Deserializer<'de>>(input: D) -> Result<Vec<i64>, D::Error> {
    Vec::<String>::deserialize(input)?
        .into_iter()
        .map(|value| value.parse().map_err(serde::de::Error::custom))
        .collect()
}

fn run_managed_cut() -> FixtureResult<()> {
    let fixture: ManagedCutFixture = serde_json::from_reader(io::stdin().lock())?;
    let candidates = fixture
        .candidates
        .iter()
        .map(|candidate| ManagedPinCandidate {
            scope: &candidate.scope,
            pin: candidate.pin,
            membership: match &candidate.membership {
                CutMembership::Complete { included } => PinMembership::Complete(included),
                CutMembership::Unknown => PinMembership::Unknown,
                CutMembership::Expired => PinMembership::Expired,
            },
        })
        .collect::<Vec<_>>();
    let visible = fixture
        .visible
        .iter()
        .map(|segment| VisibleSegment {
            segment_id: segment.segment_id,
            payload_retained: segment.payload_retained,
        })
        .collect::<Vec<_>>();
    let catalog = &fixture.catalog;
    let observation = ManagedCatalogObservation {
        authority_id: catalog.authority_id,
        incarnation_id: catalog.incarnation_id,
        ref_name: &catalog.ref_name,
        metadata: &catalog.metadata,
    };
    let cut = ManagedAncestry::certify(&fixture.scope, catalog.anchor, observation)
        .and_then(|ancestry| ancestry.select(&candidates, &visible));
    let output = match cut {
        Ok(cut) => serde_json::json!({
            "pin": cut.pin.to_string(),
            "overlay_segment_ids": cut.overlay_segment_ids.iter().map(i64::to_string).collect::<Vec<_>>(),
        }),
        Err(err) => serde_json::json!({ "error": match err {
            ManagedReadError::ScopeMismatch => "binding_changed",
            ManagedReadError::InvalidProof => "invalid_proof",
            ManagedReadError::AncestryUnavailable => "ancestry_unavailable",
            ManagedReadError::AncestryLimitExceeded => "ancestry_limit_exceeded",
            ManagedReadError::NoCompatiblePin => "no_compatible_pin",
        }}),
    };
    println!("{output}");
    Ok(())
}

fn main() -> FixtureResult<()> {
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "--select-managed-cut")
    {
        return run_managed_cut();
    }
    let mut fixture: Fixture = serde_json::from_reader(io::stdin().lock())?;
    let s3 = s3_options();
    let resolver = if fixture.admitted_transport {
        let target: PublicationTarget = fixture
            .approved_target
            .take()
            .ok_or("admitted test worker requires an independent approved target")?
            .into();
        let expires_at = std::time::SystemTime::now() + Duration::from_secs(90);
        Some(PublicationCredentialResolver::new(
            [
                (PublicationPurpose::Send, "fixture-send", "lakehouse-test"),
                (
                    PublicationPurpose::Lookup,
                    "fixture-lookup",
                    "lakehouse-reader",
                ),
            ]
            .map(|(purpose, catalog, objects)| PublicationCredentialGrant {
                target: target.clone(),
                purpose,
                catalog_credential_ref: catalog.into(),
                object_credential_ref: objects.into(),
                expires_at,
            }),
        )?)
    } else {
        None
    };
    let catalog = FixtureCatalog {
        target: &fixture,
        resolver,
        scoped: OnceCell::new(),
        writer: if fixture.admitted_transport || fixture.lookup_only {
            None
        } else {
            Some(CatalogClient::new(
                &fixture.legacy_target()?.endpoint,
                "smoke",
                "",
                &S3ClientOptions {
                    endpoint: s3.endpoint,
                    region: s3.region,
                    access_key_id: s3.access_key_id,
                    secret_access_key: s3.secret_access_key,
                    path_style: s3.path_style,
                },
            )?)
        },
    };
    let outcome = if let Some(hint) = fixture.dispatch_hint {
        if !fixture.admitted_transport
            || fixture.lookup_only
            || hint.operation_id != fixture.operation_id
        {
            return Err("invalid scheduled publication fixture request".into());
        }
        let outcome = match hint.mode {
            PublicationDispatchMode::Send => dispatch_publication_send(&fixture, &catalog, hint)?,
            PublicationDispatchMode::Lookup => {
                dispatch_publication_lookup(&fixture, hint, |claim, intent| {
                    catalog.lookup(claim, intent)
                })?
            }
            PublicationDispatchMode::Hold => None,
        };
        let Some(outcome) = outcome else {
            println!("rediscover");
            return Ok(());
        };
        outcome
    } else if fixture.lookup_only {
        let Some(outcome) =
            reconcile_publication(&fixture, fixture.operation_id, |claim, intent| {
                catalog.lookup(claim, intent)
            })?
        else {
            println!("no_retained_intent");
            return Ok(());
        };
        outcome
    } else {
        drive_publication(&fixture, &catalog, fixture.operation_id)?
    };
    match outcome {
        WorkerOutcome::Complete { snapshot_id } => println!("complete {snapshot_id}"),
        WorkerOutcome::Verified {
            snapshot_id,
            members,
        } => println!("verified {snapshot_id} {members}"),
        WorkerOutcome::Pending(reason) => println!(
            "hold {}",
            match reason {
                PendingReason::RetainedBodyMissing => "retained_body_missing",
                PendingReason::AcceptanceUnproved => "acceptance_unproved",
                PendingReason::LookupUnavailable(_) => "lookup_unavailable",
                PendingReason::DispatchUnknown(_) => "dispatch_unknown",
                PendingReason::DispatchConflict(_) => "dispatch_conflict",
            }
        ),
    }
    Ok(())
}
