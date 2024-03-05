use crate::ipc::{ClientRpc, ConnectionError};
use pgrx::error;
use std::num::{NonZeroU16, NonZeroU32};

pub fn bad_init() -> ! {
    error!("\
pgvecto.rs: pgvecto.rs must be loaded via shared_preload_libraries.
ADVICE: If you encounter this error for your first use of pgvecto.rs, \
please read `https://docs.pgvecto.rs/getting-started/installation.html`. \
You should edit `shared_preload_libraries` in `postgresql.conf` to include `vectors.so`, \
or simply run the command `psql -U postgres -c 'ALTER SYSTEM SET shared_preload_libraries = \"vectors.so\"'`.");
}

pub fn bad_guc_literal(key: &str, hint: &str) -> ! {
    error!(
        "\
Failed to parse a GUC variable.
INFORMATION: GUC = {key}, hint = {hint}"
    );
}

pub fn check_type_dims_u16(dims: Option<NonZeroU16>) -> NonZeroU16 {
    match dims {
        None => {
            error!(
                "\
pgvecto.rs: Modifier of the type is invalid.
ADVICE: Check if modifier of the type is an integer among 1 and 65535."
            )
        }
        Some(x) => x,
    }
}

pub fn check_type_dims_max(dims: Option<NonZeroU32>) -> NonZeroU32 {
    match dims {
        Some(x) if x.get() <= 1_048_575 => x,
        _ => {
            error!(
                "\
pgvecto.rs: Modifier of the type is invalid.
ADVICE: Check if modifier of the type is an integer among 1 and 1_048_575."
            )
        }
    }
}

pub fn check_value_dims_65535(dims: usize) -> NonZeroU16 {
    match u16::try_from(dims).and_then(NonZeroU16::try_from).ok() {
        None => {
            error!(
                "\
pgvecto.rs: Dimensions of the vector is invalid.
ADVICE: Check if dimensions of the vector are among 1 and 65535."
            )
        }
        Some(x) => x,
    }
}

pub fn check_value_dims_1048575(dims: usize) -> NonZeroU32 {
    if !(1..=1_048_575).contains(&dims) {
        error!(
            "\
pgvecto.rs: Dimensions of the vector is invalid.
ADVICE: Check if dimensions of the vector are among 1 and 1_048_575."
        )
    }
    NonZeroU32::new(dims as u32).unwrap()
}

pub fn bad_literal(hint: &str) -> ! {
    error!(
        "\
pgvecto.rs: Bad literal.
INFORMATION: hint = {hint}"
    );
}

#[inline(always)]
pub fn check_matched_dims(left_dims: usize, right_dims: usize) -> usize {
    if left_dims != right_dims {
        error!(
            "\
pgvecto.rs: Operands of the operator differs in dimensions or scalar type.
INFORMATION: left_dimensions = {left_dims}, right_dimensions = {right_dims}",
        )
    }
    left_dims
}

#[inline(always)]
pub fn check_column_dims(dims: Option<NonZeroU32>) -> NonZeroU32 {
    match dims {
        None => error!(
            "\
pgvecto.rs: Dimensions type modifier of a vector column is needed for building the index.",
        ),
        Some(x) => x,
    }
}

pub fn bad_opclass() -> ! {
    error!(
        "\
pgvecto.rs: Indexes can only be built on built-in distance functions.
ADVICE: If you want pgvecto.rs to support more distance functions, \
visit `https://github.com/tensorchord/pgvecto.rs/issues` and contribute your ideas."
    );
}

pub fn bad_service_not_exist() -> ! {
    error!(
        "\
pgvecto.rs: The index is not existing in the background worker.
ADVICE: Drop or rebuild the index.\
        "
    );
}

pub fn check_connection<T>(result: Result<T, ConnectionError>) -> T {
    match result {
        Err(_) => error!(
            "\
pgvecto.rs: IPC connection is closed unexpectedly.
ADVICE: Visit `https://github.com/tensorchord/pgvecto.rs/issues` for help."
        ),
        Ok(x) => x,
    }
}

pub fn check_client(option: Option<ClientRpc>) -> ClientRpc {
    match option {
        None => error!(
            "\
pgvecto.rs: The extension is upgraded so all index files are outdated.
ADVICE: Delete all index files. Please read `https://docs.pgvecto.rs/admin/upgrading.html`"
        ),
        Some(x) => x,
    }
}

pub fn bad_service_upgrade() -> ! {
    error!(
        "\
pgvecto.rs: The extension is upgraded so this index is outdated.
ADVICE: Rebuild the index. Please read `https://docs.pgvecto.rs/admin/upgrading.html`."
    )
}

pub fn bad_service_exists() -> ! {
    error!(
        "\
pgvecto.rs: The index is already existing in the background worker."
    )
}

pub fn bad_service_invalid_index_options(reason: &str) -> ! {
    error!(
        "\
pgvecto.rs: The given index option is invalid.
INFORMATION: reason = {reason:?}"
    )
}

pub fn bad_service_invalid_vector() -> ! {
    error!(
        "\
pgvecto.rs: The dimension of a vector does not matched that in a vector index column."
    )
}
