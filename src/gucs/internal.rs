use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::PostgresGucEnum;

#[derive(Debug, Clone, Copy, PostgresGucEnum)]
#[allow(non_camel_case_types)]
pub enum Transport {
    unix,
    mmap,
}

pub static TRANSPORT: GucSetting<Transport> = GucSetting::<Transport>::new(Transport::mmap);

pub unsafe fn init() {
    GucRegistry::define_enum_guc(
        "vectors.internal_transport",
        "Transport for communicating with background worker.",
        "https://docs.pgvecto.rs/usage/search.html",
        &TRANSPORT,
        GucContext::Userset,
        GucFlags::default(),
    )
}
