mod dataset;
mod indexing;
mod with_labels;

use base::distance::DistanceKind;
use base::index::*;
use base::search::Vectors;
use base::vector::{BorrowedVector, VectorKind};
use dataset::Dataset;
use ndarray::{Array1, Array2};
use numpy::{IntoPyArray, PyArray2, PyReadonlyArray1, PyReadonlyArray2};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::path::PathBuf;
use validator::Validate;
use with_labels::WithLabels;

#[pymodule]
fn vectors(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Indexing>()?;
    Ok(())
}

#[pyclass]
pub struct Indexing(indexing::Indexing);

#[pymethods]
impl Indexing {
    #[staticmethod]
    #[pyo3(signature = (path, distance, dims, dataset, labels, **indexing_options))]
    pub fn create(
        path: &str,
        distance: &str,
        dims: u32,
        dataset: PyReadonlyArray2<'_, f32>,
        labels: PyReadonlyArray1<'_, i64>,
        indexing_options: Option<Bound<'_, PyDict>>,
    ) -> Self {
        // path
        let path = PathBuf::from(path);
        assert_eq!(std::fs::exists(&path).ok(), Some(false), "file exists");
        // distance, dims
        assert!(matches!(dims, 1..=65535));
        let vector_options = VectorOptions {
            dims,
            v: VectorKind::Vecf32,
            d: match distance {
                "dot" => DistanceKind::Dot,
                "l2" => DistanceKind::L2,
                "hamming" => DistanceKind::Hamming,
                "jaccard" => DistanceKind::Jaccard,
                _ => unimplemented!("distance type {distance} is not implemented"),
            },
        };
        vector_options.validate().expect("not valid vector options");
        // dataset
        let dataset = dataset.as_array();
        assert!(dataset.dim().1 == dims as usize, "bad dataset");
        let dataset = Dataset::new(dataset);
        let source = WithLabels::new(
            dataset,
            labels.as_slice().expect("memory is non continuous"),
        );
        // indexing_options
        let indexing_options: IndexingOptions = indexing_options
            .map(|obj| pythonize::depythonize_bound(obj.into_any()).expect("failed to deserialize"))
            .unwrap_or_default();
        let index_options = IndexOptions {
            vector: vector_options,
            indexing: indexing_options,
        };
        index_options.validate().expect("not valid index options");
        // build
        Self(indexing::Indexing::create(
            &path,
            index_options.clone(),
            source,
        ))
    }
    #[staticmethod]
    pub fn open(path: &str) -> Self {
        Self(indexing::Indexing::open(path))
    }
    #[pyo3(signature = (dataset, k, **search_options))]
    pub fn search<'py>(
        &self,
        py: Python<'py>,
        dataset: PyReadonlyArray2<'py, f32>,
        k: u32,
        search_options: Option<Bound<'py, PyDict>>,
    ) -> (Bound<'py, PyArray2<f32>>, Bound<'py, PyArray2<i64>>) {
        // dataset
        let dataset = dataset.as_array();
        assert!(dataset.dim().1 == self.0.dims() as usize, "bad dataset");
        let dataset = Dataset::new(dataset);
        // search_options
        let search_options: SearchOptions = search_options
            .map(|obj| pythonize::depythonize_bound(obj.into_any()).expect("failed to deserialize"))
            .unwrap_or_default();
        // results
        let mut d = Array2::zeros((0, k as usize));
        let mut l = Array2::zeros((0, k as usize));
        for i in 0..dataset.len() {
            let (distances, labels) = self
                .0
                .vbase(BorrowedVector::Vecf32(dataset.vector(i)), &search_options)
                .map(|(distance, label)| (f32::from(distance), label.as_u64() as i64))
                .chain(std::iter::repeat((f32::INFINITY, i64::MAX)))
                .take(k as usize)
                .unzip::<_, _, Vec<_>, Vec<_>>();
            d.push_row(Array1::from_vec(distances).view()).unwrap();
            l.push_row(Array1::from_vec(labels).view()).unwrap();
        }
        (d.into_pyarray_bound(py), l.into_pyarray_bound(py))
    }
}
