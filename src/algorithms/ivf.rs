use super::impls::ivf::IvfImpl;
use crate::algorithms::Vectors;
use crate::memory::using;
use crate::memory::Address;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IvfOptions {
    pub storage: Storage,
    #[serde(default = "IvfOptions::default_build_threads")]
    pub build_threads: usize,
    pub nlist: usize,
    pub nprobe: usize,
    #[serde(default = "IvfOptions::default_least_iterations")]
    pub least_iterations: usize,
    #[serde(default = "IvfOptions::default_iterations")]
    pub iterations: usize,
}

impl IvfOptions {
    fn default_build_threads() -> usize {
        std::thread::available_parallelism().unwrap().get()
    }
    fn default_least_iterations() -> usize {
        16
    }
    fn default_iterations() -> usize {
        500
    }
}

pub struct Ivf {
    implementation: IvfImpl,
}

impl Algorithm for Ivf {
    type Options = IvfOptions;

    fn build(options: Options, vectors: Arc<Vectors>, n: usize) -> anyhow::Result<Self> {
        let ivf_options = options.algorithm.clone().unwrap_ivf();
        let implementation = IvfImpl::new(
            vectors.clone(),
            options.dims,
            options.distance,
            n,
            ivf_options.nlist,
            ivf_options.nlist * 50,
            ivf_options.nprobe,
            ivf_options.least_iterations,
            ivf_options.iterations,
            options.capacity,
            ivf_options.storage,
        )?;
        let i = AtomicUsize::new(0);
        using().scope(|scope| -> anyhow::Result<()> {
            let mut handles = Vec::new();
            for _ in 0..ivf_options.build_threads {
                handles.push(scope.spawn(|| -> anyhow::Result<()> {
                    loop {
                        let i = i.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        implementation.insert(i)?;
                    }
                    anyhow::Result::Ok(())
                }));
            }
            for handle in handles.into_iter() {
                handle.join().unwrap()?;
            }
            anyhow::Result::Ok(())
        })?;
        Ok(Self { implementation })
    }
    fn address(&self) -> Address {
        self.implementation.address
    }
    fn load(options: Options, vectors: Arc<Vectors>, address: Address) -> anyhow::Result<Self> {
        let ivf_options = options.algorithm.clone().unwrap_ivf();
        let implementation = IvfImpl::load(vectors, options.distance, address, ivf_options.nprobe)?;
        Ok(Self { implementation })
    }
    fn insert(&self, insert: usize) -> anyhow::Result<()> {
        self.implementation.insert(insert)
    }
    fn search(&self, search: (Box<[Scalar]>, usize)) -> anyhow::Result<Vec<(Scalar, u64)>> {
        self.implementation.search(search)
    }
}
