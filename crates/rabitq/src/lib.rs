#![allow(clippy::needless_range_loop)]
#![allow(clippy::type_complexity)]
#![allow(clippy::identity_op)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::len_without_is_empty)]
#![feature(pointer_is_aligned_to)]

pub mod operator;
pub mod quant;

use crate::operator::OperatorRabitq as Op;
use crate::quant::quantization::Quantization;
use base::always_equal::AlwaysEqual;
use base::index::{IndexOptions, RabitqIndexingOptions, SearchOptions};
use base::operator::Borrowed;
use base::scalar::F32;
use base::search::{Collection, Element, Payload, Source, Vectors};
use common::json::Json;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use common::vec2::Vec2;
use k_means::{k_means_lookup, k_means_lookup_many, k_means_lookup_many_2};
use num_traits::Float;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::path::Path;
use stoppable_rayon as rayon;
use storage::Storage;

pub struct Rabitq<O: Op> {
    storage: O::Storage,
    quantization: Quantization<O>,
    payloads: MmapArray<Payload>,
    offsets: Json<Vec<u32>>,
    projected_centroids: Json<Vec2<F32>>,
    projection: Json<Vec<Vec<F32>>>,
}

impl<O: Op> Rabitq<O> {
    pub fn create(path: impl AsRef<Path>, options: IndexOptions, source: &impl Source<O>) -> Self {
        let remapped = RemappedCollection::from_source(source);
        from_nothing(path, options, &remapped)
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        open(path)
    }

    pub fn len(&self) -> u32 {
        self.storage.len()
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
        self.storage.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.payloads[i as usize]
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> Box<dyn Iterator<Item = Element> + 'a> {
        let projected_query = O::proj(&self.projection, O::cast(vector));
        let lists = {
            let mut lists = select(
                k_means_lookup_many(&projected_query, &self.projected_centroids),
                opts.rabitq_nprobe as usize,
            );
            lists.sort();
            // lists.sort_unstable_by_key(|(_, i)| *i);
            lists
        };
        let mut result = BinaryHeap::new();
        for _ in 0..10 {
            result.push((
                F32::infinity().0.to_bits() as i32,
                AlwaysEqual(u32::MAX),
                (),
            ));
        }
        for &(dist, i) in lists.iter() {
            // println!("centroid = {i}");
            let preprocessed = self.quantization.preprocess(
                dist,
                &projected_query,
                &self.projected_centroids[(i,)],
            );
            let start = self.offsets[i];
            let end = self.offsets[i + 1];
            // println!("");
            self.quantization.push_batch(
                &preprocessed,
                start..end,
                &mut result,
                move |u| {
                    // print!("F");
                    // println!("rerank, hint = {}", O::cast(self.storage.vector(u))[0]);
                    (O::distance(vector, self.storage.vector(u)), ())
                },
                opts.rabitq_fast_scan,
                |u| O::cast(self.vector(u))[0],
            );
            // println!("");
        }
        /*
        let mut reranker = self.quantization.rerank(heap, move |u| {
            (O::distance(vector, self.storage.vector(u)), ())
        });
        */
        let mut result = result.into_vec();
        result.sort_unstable_by_key(|(dis_u, ..)| Reverse(*dis_u));
        Box::new(std::iter::from_fn(move || {
            result.pop().map(|(dis_u, AlwaysEqual(u), ())| Element {
                distance: F32(0.0),
                payload: AlwaysEqual(self.payload(u)),
            })
        }))
    }
}

fn from_nothing<O: Op>(
    path: impl AsRef<Path>,
    options: IndexOptions,
    collection: &impl Collection<O>,
) -> Rabitq<O> {
    create_dir(path.as_ref()).unwrap();
    let RabitqIndexingOptions { nlist } = options.indexing.clone().unwrap_rabitq();
    let projection = {
        let dims = options.vector.dims as usize;
        /*
        use nalgebra::debug::RandomOrthogonal;
        use nalgebra::{Dim, Dyn};
        use rand::Rng;

        let mut rng = rand::thread_rng();
        let random_orth: RandomOrthogonal<f32, Dyn> =
            RandomOrthogonal::new(Dim::from_usize(dims), || rng.gen());
        let random_matrix = random_orth.unwrap();
        let mut projection = vec![Vec::with_capacity(dims); dims];
        // use the transpose of the random matrix as the inverse of the orthogonal matrix,
        // but we need to transpose it again to make it efficient for the dot production
        for (i, vec) in random_matrix.row_iter().enumerate() {
            for &val in vec.iter() {
                projection[i].push(F32(val));
            }
        }
        projection

        */
        /*
        let mut result = vec![vec![F32(0.0); dims]; dims];
        for i in 0..dims {
            result[i][i] = F32(1.0);
        }
        result
        */
        serde_json::from_str::<Vec<Vec<F32>>>(
            &std::fs::read_to_string("/usamoi/repos/pgvecto.rs/crates/rabitq/src/o.txt").unwrap(),
        )
        .unwrap()
    };
    // let samples = common::sample::sample_cast(collection);
    // rayon::check();
    // let centroids: Vec2<F32> = k_means(nlist as usize, samples, false);
    let centroids: Vec2<F32> = {
        fn load_centroids_from_fvecs(path: impl AsRef<Path>) -> Vec2<F32> {
            let fvecs = read_vecs::<f32>(&path).expect("read centroids error");
            let nlist = fvecs.len();
            let dims = fvecs[0].len();
            Vec2::from_vec(
                (nlist, dims),
                fvecs.into_iter().flatten().map(F32).collect(),
            )
        }
        load_centroids_from_fvecs("/usamoi/repos/RaBitQ/gist/gist_centroid_4096.fvecs")
    };
    let projected_centroids = Vec2::from_vec(
        (centroids.shape_0(), centroids.shape_1()),
        (0..centroids.shape_0())
            .flat_map(|x| O::proj(&projection, &centroids[(x,)]))
            .collect(),
    );
    rayon::check();
    let ls = (0..collection.len())
        .into_par_iter()
        .fold(
            || vec![Vec::new(); nlist as usize],
            |mut state, i| {
                state[k_means_lookup(O::cast(collection.vector(i)), &centroids)].push(i);
                state
            },
        )
        .reduce(
            || vec![Vec::new(); nlist as usize],
            |lhs, rhs| {
                std::iter::zip(lhs.into_iter(), rhs.into_iter())
                    .map(|(lhs, rhs)| {
                        let mut x = lhs;
                        x.extend(rhs);
                        x
                    })
                    .collect()
            },
        );
    let mut offsets = vec![0u32; nlist as usize + 1];
    for i in 0..nlist {
        offsets[i as usize + 1] = offsets[i as usize] + ls[i as usize].len() as u32;
    }
    let remap = ls
        .into_iter()
        .flat_map(|x| x.into_iter())
        .collect::<Vec<_>>();
    let collection = RemappedCollection::from_collection(collection, remap);
    rayon::check();
    let storage = O::Storage::create(path.as_ref().join("storage"), &collection);
    let quantization = Quantization::create(
        path.as_ref().join("quantization"),
        options.vector,
        collection.len(),
        |vector| {
            let vector = O::cast(collection.vector(vector));
            let target = k_means_lookup(vector, &centroids);
            let projected_vector = O::proj(&projection, vector);
            O::_residual(&projected_vector, &projected_centroids[(target,)])
        },
    );
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
    let offsets = Json::create(path.as_ref().join("offsets"), offsets);
    let projected_centroids = Json::create(
        path.as_ref().join("projected_centroids"),
        projected_centroids,
    );
    let projection = Json::create(path.as_ref().join("projection"), projection);
    Rabitq {
        storage,
        payloads,
        offsets,
        projected_centroids,
        quantization,
        projection,
    }
}

fn open<O: Op>(path: impl AsRef<Path>) -> Rabitq<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let offsets = Json::open(path.as_ref().join("offsets"));
    let projected_centroids = Json::open(path.as_ref().join("projected_centroids"));
    let projection = Json::open(path.as_ref().join("projection"));
    Rabitq {
        storage,
        quantization,
        payloads,
        offsets,
        projected_centroids,
        projection,
    }
}

fn select<T: Ord>(mut lists: Vec<T>, n: usize) -> Vec<T> {
    if lists.is_empty() || n == 0 {
        return Vec::new();
    }
    let n = n.min(lists.len());
    lists.select_nth_unstable(n - 1);
    lists.truncate(n);
    lists
}

fn read_vecs<T>(path: impl AsRef<Path>) -> std::io::Result<Vec<Vec<T>>>
where
    T: Sized + num_traits::FromBytes<Bytes = [u8; 4]>,
{
    use std::io::Read;

    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut buf = [0u8; 4];
    let mut count: usize;
    let mut vecs = Vec::new();
    loop {
        count = reader.read(&mut buf)?;
        if count == 0 {
            break;
        }
        let dim = u32::from_le_bytes(buf) as usize;
        let mut vec = Vec::with_capacity(dim);
        for _ in 0..dim {
            reader.read_exact(&mut buf)?;
            vec.push(T::from_le_bytes(&buf));
        }
        vecs.push(vec);
    }
    Ok(vecs)
}
