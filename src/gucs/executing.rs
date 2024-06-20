use base::index::*;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

static IVF_NPROBE: GucSetting<i32> = GucSetting::<i32>::new(10);

static HNSW_EF_SEARCH: GucSetting<i32> = GucSetting::<i32>::new(100);

pub unsafe fn init() {
    GucRegistry::define_int_guc(
        "vectors.ivf_nprobe",
        "`nprobe` argument of IVF algorithm.",
        "https://docs.pgvecto.rs/usage/search.html",
        &IVF_NPROBE,
        1,
        1_000_000,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.hnsw_ef_search",
        "`ef_search` argument of HNSW algorithm.",
        "https://docs.pgvecto.rs/usage/search.html",
        &HNSW_EF_SEARCH,
        1,
        u16::MAX as _,
        GucContext::Userset,
        GucFlags::default(),
    );
}

pub fn search_options() -> SearchOptions {
    SearchOptions {
        hnsw_ef_search: HNSW_EF_SEARCH.get() as u32,
        ivf_nprobe: IVF_NPROBE.get() as u32,
    }
}
