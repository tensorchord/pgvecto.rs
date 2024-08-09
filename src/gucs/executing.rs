use base::index::*;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

static FLAT_SQ_RERANK_SIZE: GucSetting<i32> = GucSetting::<i32>::new(0);

static FLAT_SQ_FAST_SCAN: GucSetting<bool> = GucSetting::<bool>::new(false);

static FLAT_PQ_RERANK_SIZE: GucSetting<i32> = GucSetting::<i32>::new(0);

static FLAT_PQ_FAST_SCAN: GucSetting<bool> = GucSetting::<bool>::new(false);

static FLAT_RQ_FAST_SCAN: GucSetting<bool> = GucSetting::<bool>::new(true);

static IVF_SQ_RERANK_SIZE: GucSetting<i32> = GucSetting::<i32>::new(0);

static IVF_SQ_FAST_SCAN: GucSetting<bool> = GucSetting::<bool>::new(false);

static IVF_PQ_RERANK_SIZE: GucSetting<i32> = GucSetting::<i32>::new(0);

static IVF_PQ_FAST_SCAN: GucSetting<bool> = GucSetting::<bool>::new(false);

static IVF_RQ_FAST_SCAN: GucSetting<bool> = GucSetting::<bool>::new(true);

static IVF_NPROBE: GucSetting<i32> = GucSetting::<i32>::new(10);

static HNSW_EF_SEARCH: GucSetting<i32> = GucSetting::<i32>::new(100);

static DISKANN_EF_SEARCH: GucSetting<i32> = GucSetting::<i32>::new(100);

static SEISMIC_Q_CUT: GucSetting<i32> = GucSetting::<i32>::new(10);

static SEISMIC_HEAP_FACTOR: GucSetting<f64> = GucSetting::<f64>::new(1.0);

pub unsafe fn init() {
    GucRegistry::define_int_guc(
        "vectors.flat_sq_rerank_size",
        "Scalar quantization reranker size.",
        "https://docs.pgvecto.rs/usage/search.html",
        &FLAT_SQ_RERANK_SIZE,
        0,
        65535,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.flat_sq_fast_scan",
        "Enables fast scan or not.",
        "https://docs.pgvecto.rs/usage/search.html",
        &FLAT_SQ_FAST_SCAN,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.flat_pq_rerank_size",
        "Product quantization reranker size.",
        "https://docs.pgvecto.rs/usage/search.html",
        &FLAT_PQ_RERANK_SIZE,
        0,
        65535,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.flat_pq_fast_scan",
        "Enables fast scan or not.",
        "https://docs.pgvecto.rs/usage/search.html",
        &FLAT_PQ_FAST_SCAN,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.flat_rq_fast_scan",
        "Enables fast scan or not.",
        "https://docs.pgvecto.rs/usage/search.html",
        &FLAT_RQ_FAST_SCAN,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.ivf_sq_rerank_size",
        "Scalar quantization reranker size.",
        "https://docs.pgvecto.rs/usage/search.html",
        &IVF_SQ_RERANK_SIZE,
        0,
        65535,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.ivf_sq_fast_scan",
        "Enables fast scan or not.",
        "https://docs.pgvecto.rs/usage/search.html",
        &IVF_SQ_FAST_SCAN,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.ivf_pq_rerank_size",
        "Product quantization reranker size.",
        "https://docs.pgvecto.rs/usage/search.html",
        &IVF_PQ_RERANK_SIZE,
        0,
        65535,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.ivf_pq_fast_scan",
        "Enables fast scan or not.",
        "https://docs.pgvecto.rs/usage/search.html",
        &IVF_PQ_FAST_SCAN,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.ivf_rq_fast_scan",
        "Enables fast scan or not.",
        "https://docs.pgvecto.rs/usage/search.html",
        &IVF_RQ_FAST_SCAN,
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
    GucRegistry::define_int_guc(
        "vectors.diskann_ef_search",
        "`ef_search` argument of DiskANN algorithm.",
        "https://docs.pgvecto.rs/usage/search.html",
        &DISKANN_EF_SEARCH,
        1,
        u16::MAX as _,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.seismic_q_cut",
        "The number of elements to keep in the heap.",
        "https://docs.pgvecto.rs/usage/search.html",
        &SEISMIC_Q_CUT,
        1,
        100_000,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_float_guc(
        "vectors.seismic_heap_factor",
        "The factor to multiply the number of elements to keep in the heap.",
        "https://docs.pgvecto.rs/usage/search.html",
        &SEISMIC_HEAP_FACTOR,
        0.01,
        1.0,
        GucContext::Userset,
        GucFlags::default(),
    );
}

pub fn search_options() -> SearchOptions {
    SearchOptions {
        flat_sq_rerank_size: FLAT_SQ_RERANK_SIZE.get() as u32,
        flat_sq_fast_scan: FLAT_SQ_FAST_SCAN.get(),
        flat_pq_rerank_size: FLAT_PQ_RERANK_SIZE.get() as u32,
        flat_pq_fast_scan: FLAT_PQ_FAST_SCAN.get(),
        ivf_sq_rerank_size: IVF_SQ_RERANK_SIZE.get() as u32,
        ivf_sq_fast_scan: IVF_SQ_FAST_SCAN.get(),
        ivf_pq_rerank_size: IVF_PQ_RERANK_SIZE.get() as u32,
        ivf_pq_fast_scan: IVF_PQ_FAST_SCAN.get(),
        ivf_nprobe: IVF_NPROBE.get() as u32,
        hnsw_ef_search: HNSW_EF_SEARCH.get() as u32,
        diskann_ef_search: DISKANN_EF_SEARCH.get() as u32,
        seismic_q_cut: SEISMIC_Q_CUT.get() as u32,
        seismic_heap_factor: SEISMIC_HEAP_FACTOR.get() as f32,
    }
}
