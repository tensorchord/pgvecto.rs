use base::search::{Collection, Payload, Pointer, Source, Vectors};
use base::vector::*;

pub struct WithLabels<'a, T> {
    dataset: T,
    labels: &'a [i64],
}

impl<'a, T: Vectors<Vecf32Owned>> WithLabels<'a, T> {
    pub fn new(dataset: T, labels: &'a [i64]) -> Self {
        assert!(dataset.len() as usize == labels.len());
        Self { dataset, labels }
    }
}

impl<'a, V: VectorOwned, T: Vectors<V>> Vectors<V> for WithLabels<'a, T> {
    fn dims(&self) -> u32 {
        self.dataset.dims()
    }

    fn len(&self) -> u32 {
        self.dataset.len()
    }

    fn vector(&self, i: u32) -> V::Borrowed<'_> {
        self.dataset.vector(i)
    }
}

impl<T> Collection for WithLabels<'_, T> {
    fn payload(&self, i: u32) -> Payload {
        Payload::new(Pointer::new(self.labels[i as usize] as u64), 0)
    }
}

impl<T> Source for WithLabels<'_, T> {
    fn get_main<X: std::any::Any>(&self) -> Option<&X> {
        None
    }

    fn get_main_len(&self) -> u32 {
        0
    }

    fn check_existing(&self, _: u32) -> bool {
        true
    }
}
