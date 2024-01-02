use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;

#[derive(Debug, Clone, Copy, pgrx::PostgresGucEnum)]
#[allow(non_camel_case_types)]
pub enum Transport {
    unix,
    mmap,
}

impl Transport {
    pub const fn default() -> Transport {
        Transport::mmap
    }
}

pub static OPENAI_API_KEY_GUC: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static K: GucSetting<i32> = GucSetting::<i32>::new(64);

pub static IVF_NPROBE: GucSetting<i32> = GucSetting::<i32>::new(10);

pub static ENABLE_VECTOR_INDEX: GucSetting<bool> = GucSetting::<bool>::new(true);

pub static ENABLE_PREFILTER: GucSetting<bool> = GucSetting::<bool>::new(false);

pub static ENABLE_VBASE: GucSetting<bool> = GucSetting::<bool>::new(false);

pub static VBASE_RANGE: GucSetting<i32> = GucSetting::<i32>::new(100);

pub static TRANSPORT: GucSetting<Transport> = GucSetting::<Transport>::new(Transport::default());

pub static OPTIMIZING_THREADS_LIMIT: GucSetting<i32> = GucSetting::<i32>::new(0);

pub unsafe fn init() {
    GucRegistry::define_string_guc(
        "vectors.openai_api_key",
        "The API key of OpenAI.",
        "The OpenAI API key is required to use OpenAI embedding.",
        &OPENAI_API_KEY_GUC,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.k",
        "The number of nearest neighbors to return for searching.",
        "The number of nearest neighbors to return for searching.",
        &K,
        1,
        u16::MAX as _,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.ivf_nporbe",
        "The number of probes at ivf index.",
        "The number of probes at ivf index.",
        &IVF_NPROBE,
        1,
        1_000_000,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.enable_vector_index",
        "Whether to enable vector index.",
        "When enabled, it will use existing vector index to speed up the search.",
        &ENABLE_VECTOR_INDEX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.enable_prefilter",
        "Whether to enable prefilter.",
        "When enabled, it will use prefilter to reduce the number of vectors to search.",
        &ENABLE_PREFILTER,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.enable_vbase",
        "Whether to enable vbase.",
        "When enabled, it will use vbase for filtering.",
        &ENABLE_VBASE,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.vbase_range",
        "The range of vbase.",
        "The range size of vabse optimization.",
        &VBASE_RANGE,
        1,
        u16::MAX as _,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_enum_guc(
        "vectors.transport",
        "Transport for communicating with background worker.",
        "Transport for communicating with background worker.",
        &TRANSPORT,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.optimizing_threads_limit",
        "Maximum threads for index optimizing.",
        "Maximum threads for optimizer of each index, 0 for no limit.",
        &OPTIMIZING_THREADS_LIMIT,
        0,
        65535,
        GucContext::Userset,
        GucFlags::default(),
    );
}
