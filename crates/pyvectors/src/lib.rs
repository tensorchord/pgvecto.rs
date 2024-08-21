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
    m.add_class::<Rabitq>()?;
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

#[pyclass]
struct Rabitq(Arc<rabitq::Rabitq<Vecf32L2>>);

#[pymethods]
impl Rabitq {
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
                pool.install(|| rabitq::Rabitq::create(path, index_options.0, &source))
            })
            .unwrap()
            .unwrap();
        Self(Arc::new(x))
    }
    #[staticmethod]
    fn open(path: &str) -> Self {
        Self(Arc::new(rabitq::Rabitq::open(path)))
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
        let mut options = SearchOptions::default();
        options.rabitq_nprobe = 300;
        self.0
            .vbase(vector.as_borrowed(), &options)
            .map(|Element { payload, .. }| payload.0.pointer().as_u64() as u32)
            .take(k as usize)
            .collect()
    }
    fn test_all(
        &self,
        test: PyReadonlyArray2<'_, f32>,
        k: u32,
        expected: PyReadonlyArray2<'_, i32>,
    ) -> (f32, f32) {
        let mut options = SearchOptions::default();
        options.rabitq_nprobe = 300;
        let test = test.as_array();
        let expected = expected.as_array();
        let (n, _) = test.dim();
        let mut sum = 0;
        let mut total_time_ms = 0;
        for i in 0..n {
            let vector = test.slice(s!(i, ..));
            let vector = Vecf32Borrowed::new(cast_slice(vector.as_slice().unwrap()));
            let start = user_time(); // ms
            let result = self
                .0
                .vbase(vector, &options)
                .map(|Element { payload, .. }| payload.0.pointer().as_u64() as u32)
                .take(k as usize)
                .collect::<Vec<_>>();
            let end = user_time(); // ms
            let e = expected.slice(s!(i, ..));
            let e = e.as_slice().unwrap();
            let mut count = 0_usize;
            for i in 0..k as usize {
                for j in 0..k as usize {
                    if e[i] == result[j] as i32 {
                        count += 1;
                    }
                }
            }
            sum += count;
            total_time_ms += end - start;
        }
        let recall = sum as f32 / (n as u32 * k) as f32;
        let qps = ((1000 * n) as f32) / total_time_ms as f32;
        (recall, qps)
    }
}

fn cast_slice(x: &[f32]) -> &[F32] {
    unsafe { std::mem::transmute(x) }
}

fn user_time() -> i64 {
    unsafe {
        let mut cur_time = std::mem::zeroed::<libc::rusage>();
        let ret = libc::getrusage(libc::RUSAGE_SELF, &mut cur_time);
        assert_eq!(ret, 0);
        let secs = cur_time.ru_utime.tv_sec;
        let micros = cur_time.ru_utime.tv_usec / 1000;
        secs * 1000 + micros
    }
}
