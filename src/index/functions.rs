use crate::ipc::client;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_pgvectors_upgrade() {
    let mut client = client::borrow_mut();
    client.upgrade();
    pgrx::warning!("pgvecto.rs is upgraded. Restart PostgreSQL to take effects.");
}
