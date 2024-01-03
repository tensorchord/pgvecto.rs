use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Serialize, Deserialize)]
#[rustfmt::skip]
pub enum FriendlyError {
    #[error("\
pgvecto.rs must be loaded via shared_preload_libraries.
ADVICE: If you encounter this error for your first use of pgvecto.rs, \
please read `https://github.com/tensorchord/pgvecto.rs/blob/main/docs/install.md`. \
You should edit `shared_preload_libraries` in `postgresql.conf` to include `vectors.so`, \
or simply run the command `psql -U postgres -c 'ALTER SYSTEM SET shared_preload_libraries = \"vectors.so\"'`.\
")]
    BadInit,
    #[error("\
Bad literal.
INFORMATION: hint = {hint}\
")]
    BadLiteral {
        hint: String,
    },
    #[error("\
Modifier of the type is invalid.
ADVICE: Check if modifier of the type is an integer among 1 and 65535.\
")]
    BadTypeDimensions,
    #[error("\
Dimensions of the vector is invalid.
ADVICE: Check if dimensions of the vector are among 1 and 65535.\
")]
    BadValueDimensions,
    #[error("\
The given index option is invalid.
INFORMATION: reason = {validation:?}\
")]
    BadOption { validation: String },
    #[error("\
Dimensions type modifier of a vector column is needed for building the index.\
")]
    BadOption2,
    #[error("\
Indexes can only be built on built-in distance functions.
ADVICE: If you want pgvecto.rs to support more distance functions, \
visit `https://github.com/tensorchord/pgvecto.rs/issues` and contribute your ideas.\
")]
    BadOptions3,
    #[error("\
The index is not existing in the background worker.
ADVICE: Drop or rebuild the index.\
")]
    UnknownIndex,
    #[error("\
Operands of the operator differs in dimensions or scalar type.
INFORMATION: left_dimensions = {left_dimensions}, right_dimensions = {right_dimensions}\
")]
    Unmatched {
        left_dimensions: u16,
        right_dimensions: u16,
    },
    #[error("\
The given vector is invalid for input.
ADVICE: Check if dimensions and scalar type of the vector is matched with the index.\
")]
    Unmatched2,
    #[error("\
IPC connection is closed unexpected.
ADVICE: The error is raisen by background worker errors. \
Please check the full PostgreSQL log to get more information.\
")]
    Ipc,
    #[error("\
The extension is upgraded so all index files are outdated.
ADVICE: Delete all index files. Please read `https://github.com/tensorchord/pgvecto.rs/blob/main/docs/upgrade.md`.\
")]
    Upgrade,
    #[error("\
The extension is upgraded so this index is outdated.
ADVICE: Rebuild the index. Please read `https://github.com/tensorchord/pgvecto.rs/blob/main/docs/upgrade.md`.\
")]
    Upgrade2,
}

pub trait FriendlyErrorLike: Sized {
    fn convert(self) -> FriendlyError;
    fn friendly(self) -> ! {
        panic!("pgvecto.rs: {}", self.convert());
    }
}

impl FriendlyErrorLike for FriendlyError {
    fn convert(self) -> FriendlyError {
        self
    }
}

pub trait FriendlyResult {
    type Output;

    fn friendly(self) -> Self::Output;
}

impl<T, E> FriendlyResult for Result<T, E>
where
    E: FriendlyErrorLike,
{
    type Output = T;

    fn friendly(self) -> T {
        match self {
            Ok(x) => x,
            Err(e) => e.friendly(),
        }
    }
}
