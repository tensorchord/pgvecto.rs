use base::index::*;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

static FLAT_SQ_RERANK_SIZE: GucSetting<i32> = GucSetting::<i32>::new(0);

static FLAT_PQ_RERANK_SIZE: GucSetting<i32> = GucSetting::<i32>::new(0);

static IVF_SQ_RERANK_SIZE: GucSetting<i32> = GucSetting::<i32>::new(0);

static IVF_PQ_RERANK_SIZE: GucSetting<i32> = GucSetting::<i32>::new(0);

static IVF_NPROBE: GucSetting<i32> = GucSetting::<i32>::new(10);

static HNSW_EF_SEARCH: GucSetting<i32> = GucSetting::<i32>::new(100);

static DISKANN_EF_SEARCH: GucSetting<i32> = GucSetting::<i32>::new(100);

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
}

pub fn search_options() -> SearchOptions {
    SearchOptions {
        flat_sq_rerank_size: FLAT_SQ_RERANK_SIZE.get() as u32,
        flat_pq_rerank_size: FLAT_PQ_RERANK_SIZE.get() as u32,
        ivf_sq_rerank_size: IVF_SQ_RERANK_SIZE.get() as u32,
        ivf_pq_rerank_size: IVF_PQ_RERANK_SIZE.get() as u32,
        ivf_nprobe: IVF_NPROBE.get() as u32,
        hnsw_ef_search: HNSW_EF_SEARCH.get() as u32,
        diskann_ef_search: DISKANN_EF_SEARCH.get() as u32,
    }
}
