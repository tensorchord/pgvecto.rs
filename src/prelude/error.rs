use crate::ipc::IpcError;
use crate::prelude::*;
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
The given index option is invalid.
INFORMATION: reason = {0:?}\
")]
    BadOption(String),
    #[error("\
The given vector is invalid for input.
INFORMATION: vector = {0:?}
ADVICE: Check if dimensions of the vector is matched with the index.\
")]
    BadVector(Vec<Scalar>),
    #[error("\
Modifier of the type is invalid.
ADVICE: Check if modifier of the type is an integer among 1 and 65535.\
")]
    BadTypmod,
    #[error("\
Dimensions of the vector is invalid.
ADVICE: Check if dimensions of the vector are among 1 and 65535.\
")]
    BadVecForDims,
    #[error("\
Dimensions of the vector is unmatched with the type modifier.
INFORMATION: type_dimensions = {type_dimensions}, value_dimensions = {value_dimensions}\
")]
    BadVecForUnmatchedDims {
        value_dimensions: u16,
        type_dimensions: u16,
    },
    #[error("\
Operands of the operator differs in dimensions.
INFORMATION: left_dimensions = {left_dimensions}, right_dimensions = {right_dimensions}\
")]
    DifferentVectorDims {
        left_dimensions: u16,
        right_dimensions: u16,
    },
    #[error("\
Indexes can only be built on built-in distance functions.
ADVICE: If you want pgvecto.rs to support more distance functions, \
visit `https://github.com/tensorchord/pgvecto.rs/issues` and contribute your ideas.\
")]
    UnsupportedOperator,
    #[error("\
The index is not existing in the background worker.
ADVICE: Drop or rebuild the index.\
")]
    Index404,
    #[error("\
Dimensions type modifier of a vector column is needed for building the index.\
")]
    DimsIsNeeded,
    #[error("\
Bad vector string.
INFORMATION: hint = {hint}\
")]
    BadVectorString {
        hint: String,
    },
    #[error("\
`mmap` transport is not supported by MacOS.\
")]
    MmapTransportNotSupported,
}

impl FriendlyError {
    pub fn friendly(self) -> ! {
        panic!("pgvecto.rs: {}", self);
    }
}

impl IpcError {
    pub fn friendly(self) -> ! {
        panic!("pgvecto.rs: {}", self);
    }
}

pub trait Friendly {
    type Output;

    fn friendly(self) -> Self::Output;
}

impl<T> Friendly for Result<T, FriendlyError> {
    type Output = T;

    fn friendly(self) -> T {
        match self {
            Ok(x) => x,
            Err(e) => e.friendly(),
        }
    }
}

impl<T> Friendly for Result<T, IpcError> {
    type Output = T;

    fn friendly(self) -> T {
        match self {
            Ok(x) => x,
            Err(e) => e.friendly(),
        }
    }
}
