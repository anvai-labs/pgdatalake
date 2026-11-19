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
    if let Some(md) = &lt.metadata {
        println!(
            "in-band metadata: format-version {:?}, schema fields={}",
            md.format_version,
            md.current_schema().map(|s| s.fields.len()).unwrap_or(0)
        );
    } else {
        println!("no in-band metadata (fetch metadata_location via file IO)");
    }
}
