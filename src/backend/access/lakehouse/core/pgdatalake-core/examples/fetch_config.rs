// One-off integration check against the live Lakekeeper fixture (M1 slice 1).
fn main() {
    use pgdatalake_core::catalog::rest::{RestCatalog, StaticToken};
    use pgdatalake_core::catalog::types::CatalogConfig;
    let cat = RestCatalog::new(
        "http://localhost:8181/catalog",
        Box::new(StaticToken(String::new())),
    )
    .unwrap();
    let cfg: CatalogConfig = cat
        .fetch_config(Some("smoke"))
        .expect("GET /v1/config from Lakekeeper");
    println!(
        "Lakekeeper /v1/config OK: defaults={} overrides={}",
        cfg.defaults.len(),
        cfg.overrides.len()
    );
}
