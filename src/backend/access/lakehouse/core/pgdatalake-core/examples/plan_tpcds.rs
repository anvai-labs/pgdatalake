fn main() -> Result<(), Box<dyn std::error::Error>> {
    use pgdatalake_core::catalog::rest::{RestCatalog, StaticToken};
    let cat = RestCatalog::new(
        "http://localhost:8181/catalog",
        Box::new(StaticToken(String::new())),
    )
    .unwrap();
    cat.fetch_config(Some("smoke")).unwrap();
    let lt = cat.load_table(&["tpcds"], "store_sales").unwrap();
    let loc = pgdatalake_core::storage::parse_location(&lt.metadata_location).unwrap();
    let s3 = pgdatalake_core::storage::S3Options {
        endpoint: Some("http://localhost:9000".into()),
        region: Some("us-east-1".into()),
        access_key_id: Some("lakehouse-test".into()),
        secret_access_key: Some("lakehouse-test-password".into()),
        path_style: true,
    };
    let io = pgdatalake_core::storage::StorageIo::open(&loc, Some(&s3)).unwrap();
    let key = match &loc {
        pgdatalake_core::storage::Location::S3 { key, .. } => key.clone(),
        _ => panic!(),
    };
    let md: pgdatalake_core::catalog::metadata::TableMetadata = serde_json::from_slice(
        &pgdatalake_core::storage::maybe_gunzip_pub(io.get_bytes(&key)?)?,
    )?;
    let schema = md.current_schema().unwrap();
    let f = schema
        .fields
        .iter()
        .find(|f| f.name == "ss_sold_date_sk")
        .unwrap();
    println!("field id={} type={:?}", f.id, f.r#type);
    // walk one manifest like plan_scan does
    let snap = md.current_snapshot().unwrap();
    let (list_io, list_key) = pgdatalake_core::scan::open_for(&snap.manifest_list, Some(&s3))?;
    let entries = pgdatalake_core::manifest::parse_manifest_list(&list_io.get_bytes(&list_key)?)?;
    let mle = &entries[0];
    let (mn_io, mn_key) = pgdatalake_core::scan::open_for(&mle.manifest_path, Some(&s3))?;
    let ents = pgdatalake_core::manifest::parse_manifest(&mn_io.get_bytes(&mn_key)?)?;
    let e = ents
        .iter()
        .find(|e| e.status == pgdatalake_core::manifest::ManifestEntryStatus::Added)
        .unwrap();
    let lb = e.data_file.lower_bounds.get(&f.id);
    let ub = e.data_file.upper_bounds.get(&f.id);
    println!(
        "bounds present: lower={:?} upper={:?}",
        lb.map(std::vec::Vec::len),
        ub.map(std::vec::Vec::len)
    );
    Ok(())
}
