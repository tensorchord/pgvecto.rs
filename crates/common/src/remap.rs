use base::search::*;
use base::vector::VectorOwned;
use std::marker::PhantomData;

pub fn remap(
    /* source.len() */ n: u32,
    /* main.len() */ m: u32,
    check_existing: impl Fn(u32) -> bool,
) -> Vec<u32> {
    let mut remap = vec![0u32; m as usize];
    let mut holes = vec![];
    // delete main points, leaving holes
    for i in 0..m {
        if check_existing(i) {
            remap[i as usize] = i;
        } else {
            holes.push(i);
        }
    }
    holes.reverse();
    // insert new points, filling holes
    for i in m..n {
        if check_existing(i) {
            if let Some(x) = holes.pop() {
                remap[x as usize] = i;
            } else {
                remap.push(i);
            }
        }
    }
    holes.reverse();
    // fill holes (only if there are more deleted points than inserted points)
    while let Some(x) = holes.pop() {
        remap.swap_remove(x as usize);
    }
    remap
}

pub struct RemappedCollection<'a, V: VectorOwned, C: Collection> {
    collection: &'a C,
    remap: Vec<u32>,
    barrier: u32,
    _phantom: PhantomData<fn(V) -> V>,
}

impl<'a, V: VectorOwned, S: Vectors<V> + Collection + Source> RemappedCollection<'a, V, S> {
    pub fn from_source(source: &'a S) -> Self {
        let barrier = source.get_main_len();
        let remap = remap(source.len(), barrier, |i| source.check_existing(i));
        Self {
            collection: source,
            remap,
            barrier,
            _phantom: PhantomData,
        }
    }
}

impl<'a, V: VectorOwned, C: Vectors<V> + Collection> RemappedCollection<'a, V, C> {
    pub fn from_collection(collection: &'a C, remap: Vec<u32>) -> Self {
        assert_eq!(remap.len(), collection.len() as usize);
        let barrier = collection.len();
        Self {
            collection,
            remap,
            barrier,
            _phantom: PhantomData,
        }
    }
}

impl<V: VectorOwned, C: Collection> RemappedCollection<'_, V, C> {
    #[inline(always)]
    pub fn skip(&self, x: u32) -> bool {
        x < self.barrier && (x as usize) < self.remap.len() && self.remap[x as usize] == x
    }
    #[inline(always)]
    pub fn barrier(&self) -> u32 {
        self.barrier
    }
}

impl<V: VectorOwned, C: Vectors<V> + Collection> Vectors<V> for RemappedCollection<'_, V, C> {
    fn dims(&self) -> u32 {
        self.collection.dims()
    }

    fn len(&self) -> u32 {
        self.remap.len() as u32
    }

    fn vector(&self, i: u32) -> V::Borrowed<'_> {
        self.collection.vector(self.remap[i as usize])
    }
}

impl<V: VectorOwned, C: Collection> Collection for RemappedCollection<'_, V, C> {
    fn payload(&self, i: u32) -> Payload {
        self.collection.payload(self.remap[i as usize])
    }
}
