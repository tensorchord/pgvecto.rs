use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, hash::Hash};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct AlwaysEqual<T>(pub T);

impl<T> PartialEq for AlwaysEqual<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl<T> Eq for AlwaysEqual<T> {}

impl<T> PartialOrd for AlwaysEqual<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for AlwaysEqual<T> {
    fn cmp(&self, _: &Self) -> Ordering {
        Ordering::Equal
    }
}

impl<T> Hash for AlwaysEqual<T> {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}
