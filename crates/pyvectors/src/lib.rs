use base::index::{IndexOptions, SearchOptions};
use base::operator::Vecf32L2;
use base::scalar::F32;
use base::search::{Collection, Element, Payload, Pointer, Source, Vectors};
use base::vector::{Vecf32Borrowed, Vecf32Owned, VectorOwned};
use numpy::ndarray::s;
use numpy::{PyReadonlyArray1, PyReadonlyArray2};
use pyo3::prelude::*;
use std::sync::Arc;

#[pymodule]
fn pyvectors(m: &Bound<'_, PyModule>) -> PyResult<()> {
    detect::init();
    m.add_class::<Hnsw>()?;
    Ok(())
}

struct Pythonized<T>(T);

impl<'py, T> FromPyObject<'py> for Pythonized<T>
where
    for<'a> T: serde::Deserialize<'a>,
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let value = pythonize::depythonize_bound::<T>(ob.clone())?;
        Ok(Self(value))
    }
}

struct IndexSource(u32, Vec<Vecf32Owned>);

impl Vectors<Vecf32L2> for IndexSource {
    fn dims(&self) -> u32 {
        self.0
    }

    fn len(&self) -> u32 {
        self.1.len() as _
    }

    fn vector(&self, i: u32) -> Vecf32Borrowed<'_> {
        self.1[i as usize].as_borrowed()
    }
}

impl Collection<Vecf32L2> for IndexSource {
    fn payload(&self, i: u32) -> base::search::Payload {
        Payload::new(Pointer::new(i as u64), 0)
    }
}

impl Source<Vecf32L2> for IndexSource {
    fn get_main<T: std::any::Any>(&self) -> Option<&T> {
        None
    }

    fn get_main_len(&self) -> u32 {
        0
    }

    fn check_existing(&self, _: u32) -> bool {
        true
    }
}

#[pyclass]
struct Hnsw(Arc<hnsw::Hnsw<Vecf32L2>>);

#[pymethods]
impl Hnsw {
    #[staticmethod]
    fn create(
        path: &str,
        index_options: Pythonized<IndexOptions>,
        dataset: PyReadonlyArray2<'_, f32>,
    ) -> Self {
        let dataset = dataset.as_array();
        let (len, dims) = dataset.dim();
        let source = IndexSource(
            dims as u32,
            (0..len)
                .map(|x| {
                    let vector = dataset.slice(s!(x, ..));
                    let vector = vector.as_slice().unwrap();
                    let vector = vector.iter().copied().map(F32).collect::<Vec<_>>();
                    Vecf32Owned::new(vector)
                })
                .collect(),
        );
        let x = stoppable_rayon::ThreadPoolBuilder::new()
            .build_scoped(|pool| {
                pool.install(|| hnsw::Hnsw::create(path, index_options.0, &source))
            })
            .unwrap()
            .unwrap();
        Self(Arc::new(x))
    }
    #[staticmethod]
    fn open(path: &str) -> Self {
        Self(Arc::new(hnsw::Hnsw::open(path)))
    }
    fn search(&self, vector: PyReadonlyArray1<'_, f32>, k: u32) -> Vec<u32> {
        let vector = Vecf32Owned::new(
            vector
                .as_slice()
                .unwrap()
                .iter()
                .copied()
                .map(F32)
                .collect::<Vec<_>>(),
        );
        self.0
            .vbase(vector.as_borrowed(), &SearchOptions::default())
            .map(|Element { payload, .. }| payload.0.pointer().as_u64() as u32)
            .take(k as usize)
            .collect()
    }
}
