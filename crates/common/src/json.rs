use serde::{Deserialize, Serialize};
use std::{ops::Deref, path::Path};

#[derive(Debug, Clone, Copy)]
pub struct Json<T>(pub T);

impl<T: Serialize> Json<T> {
    pub fn create(path: impl AsRef<Path>, x: T) -> Self {
        std::fs::write(path, serde_json::to_string(&x).unwrap()).unwrap();
        Self(x)
    }
}

impl<T: for<'a> Deserialize<'a>> Json<T> {
    pub fn open(path: impl AsRef<Path>) -> Self {
        let x = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        Self(x)
    }
}

impl<T> AsRef<T> for Json<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T> Deref for Json<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}
