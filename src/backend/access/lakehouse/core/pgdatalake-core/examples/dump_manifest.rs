fn main() -> Result<(), Box<dyn std::error::Error>> {
    // fetch the first live manifest and dump raw avro values
    use pgdatalake_core::catalog::rest::{RestCatalog, StaticToken};
    let cat = RestCatalog::new(
        "http://localhost:8181/catalog",
        Box::new(StaticToken(String::new())),
    )
    .unwrap();
    let cfg = cat.fetch_config(Some("smoke")).unwrap();
    let _ = cfg;
    let lt = cat.load_table(&["analytics"], "events").unwrap();
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
    )
    .unwrap();
    let snap = md.current_snapshot().unwrap();
    let list_loc = pgdatalake_core::storage::parse_location(&snap.manifest_list).unwrap();
    let list_io = pgdatalake_core::storage::StorageIo::open(&list_loc, Some(&s3)).unwrap();
    let list_key = match &list_loc {
        pgdatalake_core::storage::Location::S3 { key, .. } => key.clone(),
        _ => panic!(),
    };
    let list_bytes = list_io.get_bytes(&list_key).unwrap();
    let entries = pgdatalake_core::manifest::parse_manifest_list(&list_bytes).unwrap();
    let mn_loc = pgdatalake_core::storage::parse_location(&entries[0].manifest_path).unwrap();
    let mn_io = pgdatalake_core::storage::StorageIo::open(&mn_loc, Some(&s3)).unwrap();
    let mn_key = match &mn_loc {
        pgdatalake_core::storage::Location::S3 { key, .. } => key.clone(),
        _ => panic!(),
    };
    let bytes = mn_io.get_bytes(&mn_key).unwrap();
    // raw apache-avro dump
    let reader = apache_avro::Reader::new(&bytes[..]).unwrap();
    for v in reader {
        let v = v.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        if let apache_avro::types::Value::Record(fields) = &v {
            for (k, val) in fields {
                if true {
                    println!("{k} = {val:?}");
                }
            }
        }
    }
    Ok(())
}
