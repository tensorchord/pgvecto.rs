use serde::{Deserialize, Serialize};
use thiserror::Error;

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[rustfmt::skip]
pub enum ServiceError {
    #[error("\
The given index option is invalid.
INFORMATION: reason = {validation:?}\
")]
    BadOption { validation: String },
    #[error("\
The index is not existing in the background worker.
ADVICE: Drop or rebuild the index.\
")]
    UnknownIndex,
#[error("\
The index is already existing in the background worker.\
")]
    KnownIndex,
    #[error("\
The given vector is invalid for input.
ADVICE: Check if dimensions and scalar type of the vector is matched with the index.\
")]
    Unmatched,
    #[error("\
The extension is upgraded so all index files are outdated.
ADVICE: Delete all index files. Please read `https://docs.pgvecto.rs/admin/upgrading.html`.\
")]
    Upgrade,
    #[error("\
The extension is upgraded so this index is outdated.
ADVICE: Rebuild the index. Please read `https://docs.pgvecto.rs/admin/upgrading.html`.\
")]
    Upgrade2,
}
