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
        metadata::IcebergType,
        publication::{
            AuthorizedPublication, PublicationIdentity, PublicationLookup, PublicationStorage,
        },
        rest::{RestCatalog, RestError, StaticToken},
    },
    parquet::CellValue,
    session::{CatalogClient, S3ClientOptions},
    snapshot_write::NewDataFile,
    storage::{parse_location, Location, S3Options, StorageIo},
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
}

#[derive(Deserialize)]
struct Attempt {
    expected_parent: Option<i64>,
    proposed_snapshot_id: i64,
    proposal_digest: String,
    proposal_body: String,
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

    fn claim(&self) -> FixtureResult<Claim> {
        Ok(serde_json::from_str(&self.sql(&format!(
            "SELECT json_build_object('input_digest', encode(input_digest, 'hex'),
                        'owner_generation', owner_generation,
                        'verified_snapshot_id', verified_snapshot_id)
             FROM lakehouse.publication_claims
             WHERE operation_id = '{}' AND authority_id = '{}' AND table_uuid = '{}';",
            self.operation_id, self.authority_id, self.table_uuid,
        ))?)?)
    }

    fn acquire(&self, claim: &Claim) -> FixtureResult<i64> {
        Ok(self
            .sql(&format!(
                "SELECT lakehouse.advance_publication_generation('{}', {});",
                self.operation_id, claim.owner_generation,
            ))?
            .parse()?)
    }

    fn identity(&self, claim: &Claim) -> FixtureResult<PublicationIdentity> {
        Ok(PublicationIdentity {
            operation_id: self.operation_id,
            table_uuid: self.table_uuid,
            input_digest: digest(&claim.input_digest)?,
        })
    }

    fn send(&self) -> FixtureResult<()> {
        let claim = self.claim()?;
        if claim.owner_generation != 0 || claim.verified_snapshot_id.is_some() {
            return Err("the fixture submitter only accepts a fresh claim".into());
        }
        let generation = self.acquire(&claim)?;
        let rows: Vec<Vec<[i64; 1]>> = serde_json::from_str(&self.sql(&format!(
            "SELECT json_agg(convert_from(s.payload, 'UTF8')::json ORDER BY s.seg_id)
             FROM lakehouse.publication_members AS m
             JOIN lakehouse.lakehouse_delta_segments AS s USING (seg_id)
             WHERE m.operation_id = '{}';",
            self.operation_id,
        ))?)?;

        let s3 = s3_options();
        let client = CatalogClient::new(
            &self.endpoint,
            "smoke",
            "",
            &S3ClientOptions {
                endpoint: s3.endpoint,
                region: s3.region,
                access_key_id: s3.access_key_id,
                secret_access_key: s3.secret_access_key,
                path_style: s3.path_style,
            },
        )?;
        let namespace = &[self.namespace.as_str()];
        let pin =
            client.write_table_open(namespace, "events", &[("id".into(), IcebergType::Long)])?;
        if pin.table_uuid != self.table_uuid || pin.location.trim_end_matches('/') != self.root {
            return Err("writer pin differs from the fixture enrollment".into());
        }
        let location = parse_location(&self.root)?;
        let Location::S3 { bucket, key } = &location else {
            return Err("the fixture requires S3 storage".into());
        };
        let store = StorageIo::open(&location, Some(&pin.s3))?;
        let mut writer = BatchWriter::new(vec![WriteColumn {
            name: "id".into(),
            ty: IcebergType::Long,
            field_id: Some(*pin.field_ids.get("id").ok_or("fixture column not found")?),
        }])?;
        for [value] in rows.into_iter().flatten() {
            writer.push(&[CellValue::Int64(value)])?;
        }
        let file = writer.write_file(&store, &format!("{key}/data"), 0)?;
        let prepared = client.prepare_staged_append(
            &pin,
            namespace,
            "events",
            &[NewDataFile {
                file_path: format!("s3://{bucket}/{}", file.key),
                record_count: file.record_count,
                file_size_in_bytes: file.file_size_in_bytes,
                ..NewDataFile::default()
            }],
            self.identity(&claim)?,
            MANIFEST_BUDGET,
        )?;
        let parent = prepared
            .expected_parent()
            .map_or_else(|| "NULL".into(), |value| value.to_string());
        let snapshot = prepared.snapshot_id();
        let request_digest = hex(&prepared.request_digest());
        let body = hex(prepared.request_body());
        self.sql(&format!(
            "SET synchronous_commit = off;
             CALL lakehouse.authorize_publication_send(
                '{}', {generation}, decode('{}', 'hex'), {parent}, {snapshot},
                decode('{request_digest}', 'hex'), decode('{body}', 'hex'), NULL);",
            self.operation_id, claim.input_digest,
        ))?;
        match prepared.submit() {
            Err(RestError::CommitStateUnknown(_)) => println!("unknown"),
            result => return Err(format!("expected a lost catalog reply, got {result:?}").into()),
        }
        Ok(())
    }

    fn recover(&self) -> FixtureResult<()> {
        let claim = self.claim()?;
        if let Some(snapshot) = claim.verified_snapshot_id {
            println!("complete {snapshot}");
            return Ok(());
        }
        let generation = self.acquire(&claim)?;
        let attempt: Attempt = serde_json::from_str(&self.sql(&format!(
            "SELECT json_build_object('expected_parent', expected_parent,
                        'proposed_snapshot_id', proposed_snapshot_id,
                        'proposal_digest', encode(proposal_digest, 'hex'),
                        'proposal_body', encode(proposal_body, 'hex'))
             FROM lakehouse.publication_attempts WHERE operation_id = '{}';",
            self.operation_id,
        ))?)?;
        let catalog = RestCatalog::new(&self.endpoint, Box::new(StaticToken(String::new())))?;
        catalog.fetch_config(Some("smoke"))?;
        let result = catalog.lookup_staged_append_on_storage(
            &[&self.namespace],
            "events",
            AuthorizedPublication {
                identity: self.identity(&claim)?,
                expected_parent: attempt.expected_parent,
                snapshot_id: attempt.proposed_snapshot_id,
                request_digest: digest(&attempt.proposal_digest)?,
                request_body: &unhex(&attempt.proposal_body)?,
            },
            &PublicationStorage {
                table_root: &self.root,
                s3: &s3_options(),
                max_bytes: MANIFEST_BUDGET.0,
                read_timeout: MANIFEST_BUDGET.1,
            },
        )?;
        match result {
            PublicationLookup::Unresolved => println!("hold"),
            PublicationLookup::Accepted { snapshot_id } => {
                let members = self.sql(&format!(
                    "SET synchronous_commit = local;
                     SELECT lakehouse.finalize_publication(
                        '{}', {generation}, '{}', '{}', decode('{}', 'hex'), {snapshot_id},
                        decode('{}', 'hex'));",
                    self.operation_id,
                    self.authority_id,
                    self.table_uuid,
                    claim.input_digest,
                    attempt.proposal_digest,
                ))?;
                println!("verified {snapshot_id} {members}");
            }
        }
        Ok(())
    }
}

fn main() -> FixtureResult<()> {
    let fixture: Fixture = serde_json::from_reader(io::stdin().lock())?;
    match std::env::args().nth(1).as_deref() {
        Some("send") => fixture.send(),
        Some("recover") => fixture.recover(),
        _ => Err("expected a fixture mode of send or recover".into()),
    }
}
