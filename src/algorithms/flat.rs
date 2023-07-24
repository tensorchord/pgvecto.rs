use crate::algorithms::Vectors;
use crate::memory::Address;
use crate::prelude::*;
use crate::utils::fixed_heap::FixedHeap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatOptions {}

pub struct Flat {
    distance: Distance,
    vectors: Arc<Vectors>,
}

impl Algorithm for Flat {
    type Options = FlatOptions;

    fn build(options: Options, vectors: Arc<Vectors>, _: usize) -> anyhow::Result<Self> {
        Ok(Self {
            distance: options.distance,
            vectors,
        })
    }

    fn address(&self) -> Address {
        Address::DANGLING
    }

    fn load(options: Options, vectors: Arc<Vectors>, _: Address) -> anyhow::Result<Self> {
        Ok(Self {
            distance: options.distance,
            vectors,
        })
    }

    fn insert(&self, _: usize) -> anyhow::Result<()> {
        Ok(())
    }

    fn search(&self, (vector, k): (Box<[Scalar]>, usize)) -> anyhow::Result<Vec<(Scalar, u64)>> {
        let mut result = FixedHeap::<(Scalar, u64)>::new(k);
        for i in 0..self.vectors.len() {
            let this_vector = self.vectors.get_vector(i);
            let this_data = self.vectors.get_data(i);
            let dis = self.distance.distance(&vector, this_vector);
            result.push((dis, this_data));
        }
        Ok(result.into_sorted_vec())
    }
}
