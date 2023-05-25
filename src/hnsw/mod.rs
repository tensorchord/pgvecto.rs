use crate::datatype::Scalar;
use crate::postgres::table::*;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

const MAX_CONNECTIONS: usize = 16;
const MAKE_CONNECTIONS: usize = 256;

const META_TUPLE: Pointer = Pointer { page: 0, id: 0 };

pub struct Build {
    relation: pgrx::pg_sys::Relation,
}

impl Build {
    pub unsafe fn new(relation: pgrx::pg_sys::Relation) -> Self {
        Self { relation }
    }
    pub fn build(&mut self) {
        let mut table = unsafe { GiantLockGuard::new(self.relation, LockMode::Exclusive) };
        let mut write = table.write();
        assert_eq!(write.pages(), 0);
        unsafe {
            let immutable = Vec::<u8>::new();
            let mut mutable = Vec::<u8>::new();
            mutable.extend_from_slice(
                [MetadataTupleMutable {
                    pointer: Pointer::NULL,
                    level: 0,
                }]
                .align_to::<u8>()
                .1,
            );
            write.append(&immutable, &mutable);
        }
    }
    pub fn build_insert(&mut self, vector: &[Scalar], heap_pointer: HeapPointer) {
        insert(self.relation, vector, heap_pointer);
    }
}

pub fn search(relation: pgrx::pg_sys::Relation, vector: &[Scalar], k: usize) -> Vec<HeapPointer> {
    let mut table = unsafe { GiantLockGuard::new(relation, LockMode::Shared) };
    let mut read = table.read();
    if let Some((entry, top)) = read_metadata(&mut read) {
        let mut e = entry;
        for i in (1..=top).rev() {
            e = search_in_layer(&mut read, i, &[e], vector, 1)[0].0;
        }
        let result = search_in_layer(&mut read, 0, &[e], vector, k);
        result
            .into_iter()
            .filter_map(|(pointer, _)| unsafe {
                let guard = read.read(pointer);
                let immutable = guard.immutable().as_ptr() as *const DataTupleImmutableHeader;
                let mutable = guard.mutable().as_ptr() as *const DataTupleMutableHeader;
                if !(*mutable).deleted {
                    Some((*immutable).heap_pointer)
                } else {
                    None
                }
            })
            .collect()
    } else {
        Vec::new()
    }
}

pub fn insert(relation: pgrx::pg_sys::Relation, vector: &[Scalar], heap_pointer: HeapPointer) {
    let mut table = unsafe { GiantLockGuard::new(relation, LockMode::Exclusive) };
    let level = rand::random::<u32>().trailing_zeros() as u8;
    let mut layers = vec![Vec::<(Pointer, Scalar)>::new(); 1 + level as usize];
    let start_vectors;
    let start_level;
    {
        let mut write = table.write();
        if let Some((entry, top)) = read_metadata(&mut write) {
            let entry_vector = read_vector(&write, entry);
            let dis = distance(&entry_vector, vector);
            if top >= level {
                let mut entry = (entry, dis);
                for i in (level + 1..=top).rev() {
                    entry = search_in_layer(&mut write, i, &[entry.0], &vector, 1)[0];
                }
                start_vectors = vec![entry];
                start_level = level;
            } else {
                start_vectors = vec![(entry, dis)];
                start_level = top;
            }
        } else {
            let pointer = append(&mut write, heap_pointer, vector, &layers);
            try_to_write_metadata(&mut write, (pointer, level));
            return;
        }
    }
    {
        let mut read = table.read();
        for i in (0..=start_level).rev() {
            let v = if i == start_level {
                start_vectors
                    .iter()
                    .map(|(x, _)| *x)
                    .collect::<Vec<Pointer>>()
            } else {
                layers[i as usize + 1]
                    .iter()
                    .map(|(x, _)| *x)
                    .collect::<Vec<Pointer>>()
            };
            layers[i as usize] = search_in_layer(&mut read, i, &v, &vector, MAKE_CONNECTIONS);
            layers[i as usize].truncate(MAX_CONNECTIONS);
        }
    }
    let pointer;
    {
        let mut write = table.write();
        pointer = append(&mut write, heap_pointer, vector, &layers);
    }
    for i in 0..=level {
        let mut write = table.write();
        for &(neighbour, distance) in layers[i as usize].iter() {
            let mut layer = read_layer(&mut write, neighbour, i);
            layer.push((pointer, distance));
            push(&mut layer);
            layer.truncate(MAX_CONNECTIONS);
            write_layer(&mut write, neighbour, i, layer);
        }
    }
    {
        let mut write = table.write();
        if start_level <= level {
            try_to_write_metadata(&mut write, (pointer, level));
        }
    }
}

pub fn vacuum<F>(_relation: pgrx::pg_sys::Relation, mut _f: F)
where
    F: FnMut(HeapPointer) -> bool,
{
    unimplemented!()
}

fn push(source: &mut [(Pointer, Scalar)]) {
    if source.len() == 0 {
        return;
    }
    let mut index = source.len() - 1;
    while index != 0 && source[index].1 < source[(index - 1) / 2].1 {
        source.swap(index, (index - 1) / 2);
        index = (index - 1) / 2;
    }
}

fn search_in_layer(
    read: &mut impl ReadLike,
    level: u8,
    source: &[Pointer],
    target: &[Scalar],
    k: usize,
) -> Vec<(Pointer, Scalar)> {
    assert_ne!(source.len(), 0);
    let mut visited: BTreeSet<Pointer> = source.iter().copied().collect();
    let mut candidates: BTreeSet<(F64, Pointer)> = source
        .iter()
        .map(|&x| (distance(&read_vector(read, x), target).into(), x))
        .collect();
    let mut result = candidates.clone();
    while result.len() > k {
        result.pop_last();
    }
    while let Some((c_dis, c)) = candidates.pop_first() {
        let (f_dis, _) = result.last().copied().unwrap();
        if c_dis > f_dis.into() {
            break;
        }
        for (e, _) in read_layer(read, c, level).into_iter() {
            if visited.contains(&e) {
                continue;
            }
            visited.insert(e);
            let e_dis = distance(&read_vector(read, e), target);
            let (f_dis, _) = result.last().copied().unwrap();
            if e_dis < f_dis.into() || result.len() < k {
                candidates.insert((e_dis.into(), e));
                result.insert((e_dis.into(), e));
                if result.len() > k {
                    result.pop_last();
                }
            }
        }
    }
    result
        .into_iter()
        .map(|(s, pointer)| (pointer, s.0))
        .collect()
}

#[repr(C, align(8))]
struct MetadataTupleMutable {
    pointer: Pointer,
    level: u8,
}

#[repr(C, align(8))]
#[derive(Debug)]
struct DataTupleImmutableHeader {
    heap_pointer: HeapPointer,
    level: u8,
    dimensions: u16,
    _x: [u64; 0],
    // ----------------
    // [Scalar; dimensions]
}

#[repr(C, align(8))]
#[derive(Debug)]
struct DataTupleMutableHeader {
    deleted: bool,
    _x: [u64; 0],
    // ----------------
    // [(u8, [(Pointer, Scalar); MAX_CONNECTIONS]); 1 + level]
}

fn read_metadata(read: &mut impl ReadLike) -> Option<(Pointer, u8)> {
    unsafe {
        let guard = read.read(META_TUPLE);
        let mutable = guard.mutable().as_ptr() as *const MetadataTupleMutable;
        let x = mutable.read();
        if x.pointer == Pointer::NULL {
            None
        } else {
            Some((x.pointer, x.level))
        }
    }
}

fn try_to_write_metadata(write: &mut impl WriteLike, metadata: (Pointer, u8)) {
    unsafe {
        let mut guard = write.write(META_TUPLE);
        let mutable = guard.mutable_mut().as_ptr() as *mut MetadataTupleMutable;
        let x = mutable.read();
        if x.pointer.is_null() || x.level < metadata.1 {
            mutable.write(MetadataTupleMutable {
                pointer: metadata.0,
                level: metadata.1,
            });
        }
    }
}

fn append(
    write: &mut impl WriteLike,
    heap_pointer: HeapPointer,
    vector: &[Scalar],
    layers: &[Vec<(Pointer, Scalar)>],
) -> Pointer {
    unsafe {
        let mut immutable = Vec::<u8>::new();
        let mut mutable = Vec::<u8>::new();
        immutable.extend_from_slice(
            [DataTupleImmutableHeader {
                heap_pointer,
                level: (layers.len() - 1) as _,
                dimensions: vector.len() as _,
                _x: Default::default(),
            }]
            .align_to::<u8>()
            .1,
        );
        immutable.extend_from_slice(vector.align_to::<u8>().1);
        mutable.extend_from_slice(
            [DataTupleMutableHeader {
                deleted: false,
                _x: Default::default(),
            }]
            .align_to::<u8>()
            .1,
        );
        for i in 0..layers.len() {
            let len = layers[i].len();
            let mut layer = layers[i].clone();
            layer.resize(MAX_CONNECTIONS, (Pointer::NULL, 0.0));
            let array = <[(Pointer, Scalar); MAX_CONNECTIONS]>::try_from(layer).unwrap();
            mutable.extend_from_slice([(len as u8, array)].align_to::<u8>().1);
        }
        let guard = write.append(&immutable, &mutable);
        guard.pointer()
    }
}

fn read_vector(read: &impl ReadLike, pointer: Pointer) -> Vec<Scalar> {
    unsafe {
        let guard = read.read(pointer);
        let immutable = guard.immutable().as_ptr() as *const DataTupleImmutableHeader;
        let len_vector = (*immutable).dimensions as usize;
        let ptr_vector = (*immutable)._x.as_ptr() as *const Scalar;
        let slice = std::slice::from_raw_parts(ptr_vector, len_vector);
        Vec::from(slice)
    }
}

fn read_layer(read: &impl ReadLike, pointer: Pointer, level: u8) -> Vec<(Pointer, Scalar)> {
    unsafe {
        let guard = read.read(pointer);
        let immutable = guard.immutable().as_ptr() as *const DataTupleImmutableHeader;
        let mutable = guard.mutable().as_ptr() as *const DataTupleMutableHeader;
        let len_layers = 1 + (*immutable).level as usize;
        let kth = level as usize;
        assert!(kth < len_layers);
        let ptr_layer = (*mutable)._x.as_ptr() as *const (u8, [(Pointer, Scalar); MAX_CONNECTIONS]);
        let element = ptr_layer.add(kth);
        let element = element.read();
        Vec::from(&element.1[..element.0 as usize])
    }
}

fn write_layer(
    write: &impl WriteLike,
    pointer: Pointer,
    level: u8,
    mut result: Vec<(Pointer, Scalar)>,
) {
    unsafe {
        let mut guard = write.write(pointer);
        let immutable = guard.immutable().as_ptr() as *const DataTupleImmutableHeader;
        let mutable = guard.mutable_mut().as_ptr() as *const DataTupleMutableHeader;
        let len_layers = 1 + (*immutable).level as usize;
        let kth = level as usize;
        assert!(kth < len_layers);
        let ptr_layer = (*mutable)._x.as_ptr() as *mut (u8, [(Pointer, Scalar); MAX_CONNECTIONS]);
        let element = ptr_layer.add(kth);
        element.write({
            let len = result.len();
            result.resize(MAX_CONNECTIONS, (Pointer::NULL, 0.0));
            let array = <[(Pointer, Scalar); MAX_CONNECTIONS]>::try_from(result).unwrap();
            (len as _, array)
        });
    }
}

fn distance(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    if lhs.len() != rhs.len() {
        return Scalar::NAN;
    }
    let mut result = 0.0 as Scalar;
    for i in 0..lhs.len() {
        result += (lhs[i] - rhs[i]) * (lhs[i] - rhs[i]);
    }
    result
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct F64(pub f64);

impl PartialEq for F64 {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for F64 {}

impl PartialOrd for F64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for F64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl From<f64> for F64 {
    fn from(value: f64) -> Self {
        Self(value)
    }
}

impl From<F64> for f64 {
    fn from(value: F64) -> Self {
        value.0
    }
}

impl Hash for F64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0.to_bits())
    }
}
