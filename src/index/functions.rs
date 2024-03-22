#[pgrx::pg_extern(volatile, strict, parallel_safe)]
fn _vectors_pgvectors_upgrade() {
    if crate::bgworker::is_started() {
        return;
    }
    let _ = std::fs::remove_dir_all("pg_vectors");
}
