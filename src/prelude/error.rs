use service::prelude::ServiceError;
use std::fmt::Display;
use thiserror::Error;

pub trait FriendlyError: Display {
    fn friendly(&self) -> ! {
        panic!("pgvecto.rs: {}", self);
    }
}

impl FriendlyError for ServiceError {}

pub trait Friendly<T> {
    fn friendly(self) -> T;
}

impl<T, E: FriendlyError> Friendly<T> for Result<T, E> {
    fn friendly(self) -> T {
        match self {
            Ok(x) => x,
            Err(e) => e.friendly(),
        }
    }
}

#[must_use]
#[derive(Debug, Error)]
#[rustfmt::skip]
pub enum SessionError {
    #[error("\
pgvecto.rs must be loaded via shared_preload_libraries.
ADVICE: If you encounter this error for your first use of pgvecto.rs, \
please read `https://docs.pgvecto.rs/getting-started/installation.html`. \
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
Dimensions type modifier of a vector column is needed for building the index.\
")]
    BadOption1,
    #[error("\
Indexes can only be built on built-in distance functions.
ADVICE: If you want pgvecto.rs to support more distance functions, \
visit `https://github.com/tensorchord/pgvecto.rs/issues` and contribute your ideas.\
")]
    BadOptions2,
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
Operands of the operator differs in dimensions or scalar type.
INFORMATION: left_dimensions = {left_dimensions}, right_dimensions = {right_dimensions}\
")]
    Unmatched {
        left_dimensions: u16,
        right_dimensions: u16,
    }
}

impl FriendlyError for SessionError {}
