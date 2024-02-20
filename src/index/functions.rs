#[pgrx::pg_extern(volatile, strict)]
fn _vectors_pgvectors_upgrade() {
    let _ = std::fs::remove_dir_all("pg_vectors");
}
