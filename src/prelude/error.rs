use crate::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum FriendlyError {
    #[error("The given index option is invalid. Reason = {0:?}.")]
    InvalidOption(String),
    #[error("The given vector is invalid for input. Vector = {0:?}.")]
    InvalidVector(Vec<Scalar>),
}

impl FriendlyError {
    pub fn friendly(self) -> ! {
        panic!("{}", self);
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
