//! Disposable M2 test worker. The hosting fixture supplies a pre-enrolled
//! Lakekeeper target and explicitly captured ledger rows. This example is
//! not a SQL API or the production publisher.

use std::{
    io::{self, Write},
    process::{Command, Stdio},
    time::Duration,
};

use pgdatalake_core::{
    catalog::{
        commit::PreparedPublication,
        metadata::IcebergType,
        publication::{
            AuthorizedPublication, PublicationIdentity, PublicationLookup, PublicationStorage,
        },
        rest::{RestCatalog, RestError, StaticToken},
    },
    parquet::CellValue,
    publication_worker::{
        drive_publication, CapturedMember, ClaimState, LedgerResult, PendingReason,
        PublicationCatalog, PublicationClaim, PublicationLedger, RetainedAttempt, WorkerOutcome,
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

#[derive(Deserialize)]
struct Fixture {
    database: String,
    endpoint: String,
    namespace: String,
    table_uuid: Uuid,
    authority_id: Uuid,
    operation_id: Uuid,
    root: String,
}

#[derive(Deserialize)]
struct Claim {
    input_digest: String,
    owner_generation: i64,
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
        let claim: Claim = serde_json::from_str(&self.sql(&format!(
            "SELECT json_build_object('input_digest', encode(c.input_digest, 'hex'),
                        'owner_generation', c.owner_generation,
                        'verified_snapshot_id', c.verified_snapshot_id,
                        'attempt', CASE WHEN a.operation_id IS NOT NULL THEN
                            json_build_object('expected_parent', a.expected_parent,
                                'proposed_snapshot_id', a.proposed_snapshot_id,
                                'proposal_digest', encode(a.proposal_digest, 'hex'),
                                'proposal_body', encode(a.proposal_body, 'hex')) END)
             FROM lakehouse.publication_claims AS c
             LEFT JOIN lakehouse.publication_attempts AS a USING (operation_id)
             WHERE c.operation_id = '{operation}' AND c.authority_id = '{}' AND c.table_uuid = '{}';",
            self.authority_id, self.table_uuid,
        ))?)?;
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
                table_uuid: self.table_uuid,
                input_digest: digest(&claim.input_digest)?,
            },
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
        let members: Vec<LedgerMember> = serde_json::from_str(&self.sql(&format!(
            "SELECT json_agg(json_build_object('segment_id', s.seg_id,
                        'row_count', s.row_count,
                        'payload', encode(s.payload, 'hex')) ORDER BY s.seg_id)
             FROM lakehouse.publication_members AS m
             JOIN lakehouse.lakehouse_delta_segments AS s USING (seg_id)
             WHERE m.operation_id = '{}';",
            claim.identity.operation_id,
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
        Ok(self
            .sql(&format!(
                "SET synchronous_commit = local;
             SELECT lakehouse.finalize_publication(
                '{}', {}, '{}', '{}', decode('{}', 'hex'), {}, decode('{}', 'hex'));",
                claim.identity.operation_id,
                claim.generation,
                self.authority_id,
                self.table_uuid,
                hex(&claim.identity.input_digest),
                intent.snapshot_id,
                hex(&intent.request_digest),
            ))?
            .parse()?)
    }
}

struct FixtureCatalog<'a> {
    target: &'a Fixture,
    writer: CatalogClient,
}

impl PublicationCatalog for FixtureCatalog<'_> {
    fn prepare(
        &self,
        claim: &PublicationClaim,
        members: &[CapturedMember],
    ) -> Result<PreparedPublication<'_>, StorageError> {
        let namespace = &[self.target.namespace.as_str()];
        let mut rows = Vec::new();
        for member in members {
            let payload: Vec<[i64; 1]> = serde_json::from_slice(&member.payload)
                .map_err(|error| StorageError::Decode(error.to_string()))?;
            if payload.len() != member.row_count as usize {
                return Err(StorageError::Decode("fixture row count mismatch".into()));
            }
            rows.extend(payload);
        }
        let pin = self.writer.write_table_open(
            namespace,
            "events",
            &[("id".into(), IcebergType::Long)],
        )?;
        if pin.table_uuid != claim.identity.table_uuid
            || pin.location.trim_end_matches('/') != self.target.root
        {
            return Err(StorageError::Config(
                "writer pin differs from fixture enrollment".into(),
            ));
        }
        let location = parse_location(&self.target.root)?;
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
        self.writer.prepare_staged_append(
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

    fn lookup(&self, intent: AuthorizedPublication<'_>) -> Result<PublicationLookup, RestError> {
        let catalog =
            RestCatalog::new(&self.target.endpoint, Box::new(StaticToken(String::new())))?;
        catalog.fetch_config(Some("smoke"))?;
        catalog.lookup_staged_append_on_storage(
            &[&self.target.namespace],
            "events",
            intent,
            &PublicationStorage {
                table_root: &self.target.root,
                s3: &s3_options(),
                max_bytes: MANIFEST_BUDGET.0,
                read_timeout: MANIFEST_BUDGET.1,
            },
        )
    }
}

fn main() -> FixtureResult<()> {
    let fixture: Fixture = serde_json::from_reader(io::stdin().lock())?;
    let s3 = s3_options();
    let catalog = FixtureCatalog {
        target: &fixture,
        writer: CatalogClient::new(
            &fixture.endpoint,
            "smoke",
            "",
            &S3ClientOptions {
                endpoint: s3.endpoint,
                region: s3.region,
                access_key_id: s3.access_key_id,
                secret_access_key: s3.secret_access_key,
                path_style: s3.path_style,
            },
        )?,
    };
    match drive_publication(&fixture, &catalog, fixture.operation_id)? {
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
