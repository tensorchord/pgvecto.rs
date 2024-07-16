use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, Default)]
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
