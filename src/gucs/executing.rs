use base::index::*;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

static SQ_RERANK_SIZE: GucSetting<i32> =
    GucSetting::<i32>::new(SearchOptions::default_sq_rerank_size() as i32);

static SQ_FAST_SCAN: GucSetting<bool> =
    GucSetting::<bool>::new(SearchOptions::default_sq_fast_scan());

static PQ_RERANK_SIZE: GucSetting<i32> =
    GucSetting::<i32>::new(SearchOptions::default_pq_rerank_size() as i32);

static PQ_FAST_SCAN: GucSetting<bool> =
    GucSetting::<bool>::new(SearchOptions::default_pq_fast_scan());

static RQ_FAST_SCAN: GucSetting<bool> =
    GucSetting::<bool>::new(SearchOptions::default_rq_fast_scan());

static IVF_NPROBE: GucSetting<i32> =
    GucSetting::<i32>::new(SearchOptions::default_ivf_nprobe() as i32);

static HNSW_EF_SEARCH: GucSetting<i32> =
    GucSetting::<i32>::new(SearchOptions::default_hnsw_ef_search() as i32);

pub unsafe fn init() {
    GucRegistry::define_int_guc(
        "vectors.sq_rerank_size",
        "Scalar quantization reranker size.",
        "https://docs.pgvecto.rs/usage/search.html",
        &SQ_RERANK_SIZE,
        0,
        65535,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.sq_fast_scan",
        "Enables fast scan or not.",
        "https://docs.pgvecto.rs/usage/search.html",
        &SQ_FAST_SCAN,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.pq_rerank_size",
        "Product quantization reranker size.",
        "https://docs.pgvecto.rs/usage/search.html",
        &PQ_RERANK_SIZE,
        0,
        65535,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.pq_fast_scan",
        "Enables fast scan or not.",
        "https://docs.pgvecto.rs/usage/search.html",
        &PQ_FAST_SCAN,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.rq_fast_scan",
        "Enables fast scan or not.",
        "https://docs.pgvecto.rs/usage/search.html",
        &PQ_FAST_SCAN,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.ivf_nprobe",
        "`nprobe` argument of IVF algorithm.",
        "https://docs.pgvecto.rs/usage/search.html",
        &IVF_NPROBE,
        1,
        u16::MAX as _,
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
        sq_rerank_size: SQ_RERANK_SIZE.get() as u32,
        sq_fast_scan: SQ_FAST_SCAN.get(),
        pq_rerank_size: PQ_RERANK_SIZE.get() as u32,
        pq_fast_scan: PQ_FAST_SCAN.get(),
        rq_fast_scan: RQ_FAST_SCAN.get(),
        ivf_nprobe: IVF_NPROBE.get() as u32,
        hnsw_ef_search: HNSW_EF_SEARCH.get() as u32,
    }
}
