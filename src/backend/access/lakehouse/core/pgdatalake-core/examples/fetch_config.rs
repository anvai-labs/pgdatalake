// Live fixture verification (M1): config -> loadTable against Lakekeeper.
// Requires `make lakehouse-up` + the `smoke` warehouse + analytics.events
// table (created by lakekeeper-setup.sh / slice 3 verification).
fn main() {
    use pgdatalake_core::catalog::rest::{RestCatalog, StaticToken};

    let cat = RestCatalog::new(
        "http://localhost:8181/catalog",
        Box::new(StaticToken(String::new())),
    )
    .unwrap();
    let cfg = cat.fetch_config(Some("smoke")).expect("GET /v1/config");
    println!(
        "config OK: defaults={} overrides={}",
        cfg.defaults.len(),
        cfg.overrides.len()
    );
    let lt = cat
        .load_table(&["analytics"], "events")
        .expect("loadTable analytics.events");
    println!("loadTable OK: {}", lt.metadata_location);
    // Slice 4: fetch metadata.json from object storage via metadata_location
    // (SeaweedFS S3 fixture creds — fake, RULES §6).
    let loc = pgdatalake_core::storage::parse_location(&lt.metadata_location)
        .expect("parse metadata_location");
    let s3 = pgdatalake_core::storage::S3Options {
        endpoint: Some("http://localhost:9000".into()),
        region: Some("us-east-1".into()),
        access_key_id: Some("lakehouse-test".into()),
        secret_access_key: Some("lakehouse-test-password".into()),
        path_style: true,
    };
    let io = pgdatalake_core::storage::StorageIo::open(&loc, Some(&s3)).expect("open S3 store");
    let key = match &loc {
        pgdatalake_core::storage::Location::S3 { key, .. } => key.clone(),
        other => panic!("expected S3 location, got {other:?}"),
    };
    let md =
        pgdatalake_core::storage::fetch_metadata(&io, &key).expect("fetch metadata.json from S3");
    println!(
        "metadata.json OK: format {:?}, uuid {}, fields={}",
        md.format_version,
        md.table_uuid,
        md.current_schema().map(|s| s.fields.len()).unwrap_or(0)
    );
}
