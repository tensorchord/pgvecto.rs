use crate::datatype::Scalar;
use crate::postgres::table::{GrandLocking, HeapPointer, Pointer, Table};
use std::alloc::{Allocator, Layout};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
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
        let mut table = unsafe { Table::new(self.relation, GrandLocking::Exclusive) };
        assert_eq!(table.pages(), 0);
        table.append(
            MetadataTuple {
                pointer: Pointer::NULL,
                level: 0,
            }
            .to_bytes(),
        );
    }
    pub fn build_insert(&mut self, vector: &[Scalar], heap_pointer: HeapPointer) {
        let mut table = unsafe { Table::new(self.relation, GrandLocking::Exclusive) };
        assert_ne!(table.pages(), 0);
        let level = rand::random::<u32>().trailing_zeros() as u8;
        let mut layers = vec![Vec::<Pointer>::new(); 1 + level as usize];
        let start_vectors;
        let start_level;
        if let Some((entry, top)) = read_metadata(&mut table) {
            if top >= level {
                let mut entry = entry;
                for i in (level + 1..=top).rev() {
                    entry = search_in_layer(&mut table, i, &[entry], &vector, 1)[0];
                }
                start_vectors = vec![entry];
                start_level = level;
            } else {
                start_vectors = vec![entry];
                start_level = top;
            }
        } else {
            let pointer = append(&mut table, heap_pointer, vector, &layers);
            try_to_write_metadata(&mut table, Some((pointer, level)));
            return;
        }
        for i in (0..=start_level).rev() {
            layers[i as usize] = search_in_layer(
                &mut table,
                i,
                if i == start_level {
                    &start_vectors
                } else {
                    &layers[i as usize + 1]
                },
                &vector,
                MAKE_CONNECTIONS,
            );
            layers[i as usize].truncate(MAX_CONNECTIONS);
        }
        let pointer = append(&mut table, heap_pointer, vector, &layers);
        for i in 0..=level {
            for &neighbour in layers[i as usize].iter() {
                let mut layer = read_layer(&mut table, neighbour, i);
                layer.push(pointer);
                push(&mut table, &mut layer, neighbour);
                layer.truncate(MAX_CONNECTIONS);
                write_layer(&mut table, neighbour, i, &layer);
            }
        }
        if start_level <= level {
            try_to_write_metadata(&mut table, Some((pointer, level)));
        }
    }
}

pub fn search(relation: pgrx::pg_sys::Relation, vector: &[Scalar], k: usize) -> Vec<HeapPointer> {
    let mut table = unsafe { Table::new(relation, GrandLocking::Shared) };
    if let Some((entry, top)) = read_metadata(&mut table) {
        let mut e = entry;
        for i in (1..=top).rev() {
            e = search_in_layer(&mut table, i, &[e], vector, 1)[0];
        }
        let result = search_in_layer(&mut table, 0, &[e], vector, k);
        result
            .into_iter()
            .filter_map(|p| unsafe {
                let tuple = force(table.read(p));
                if !(*tuple).deleted {
                    Some((*tuple).heap_pointer)
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
    let mut table = unsafe { Table::new(relation, GrandLocking::Exclusive) };
    let level = rand::random::<u32>().trailing_zeros() as u8;
    let mut layers = vec![Vec::<Pointer>::new(); 1 + level as usize];
    let start_vectors;
    let start_level;
    if let Some((entry, top)) = read_metadata(&mut table) {
        if top >= level {
            let mut entry = entry;
            for i in (level + 1..=top).rev() {
                entry = search_in_layer(&mut table, i, &[entry], &vector, 1)[0];
            }
            start_vectors = vec![entry];
            start_level = level;
        } else {
            start_vectors = vec![entry];
            start_level = top;
        }
    } else {
        let pointer = append(&mut table, heap_pointer, vector, &layers);
        try_to_write_metadata(&mut table, Some((pointer, level)));
        return;
    }
    for i in (0..=start_level).rev() {
        layers[i as usize] = search_in_layer(
            &mut table,
            i,
            if i == start_level {
                &start_vectors
            } else {
                &layers[i as usize + 1]
            },
            &vector,
            MAKE_CONNECTIONS,
        );
        layers[i as usize].truncate(MAX_CONNECTIONS);
    }
    let pointer = append(&mut table, heap_pointer, vector, &layers);
    for i in 0..=level {
        for &neighbour in layers[i as usize].iter() {
            let mut layer = read_layer(&mut table, neighbour, i);
            layer.push(pointer);
            push(&mut table, &mut layer, neighbour);
            layer.truncate(MAX_CONNECTIONS);
            write_layer(&mut table, neighbour, i, &layer);
        }
    }
    if start_level <= level {
        try_to_write_metadata(&mut table, Some((pointer, level)));
    }
}

pub fn vacuum<F>(relation: pgrx::pg_sys::Relation, mut f: F)
where
    F: FnMut(HeapPointer) -> bool,
{
    let n = {
        let mut table = unsafe { Table::new(relation, GrandLocking::Shared) };
        table.pages()
    };
    for page in 0..n {
        let mut table = unsafe { Table::new(relation, GrandLocking::Shared) };
        for id in 0..=u16::MAX {
            let pointer = Pointer::new(page, id);
            if pointer == META_TUPLE {
                continue;
            }
            let Some(tuple) = table.write(pointer) else { break };
            unsafe {
                let tuple = force_mut(Some(tuple));
                if !(*tuple).deleted {
                    if f((*tuple).heap_pointer) {
                        (*tuple).deleted = true;
                    }
                }
            }
        }
    }
}

fn search_in_layer(
    table: &mut Table,
    level: u8,
    source: &[Pointer],
    target: &[Scalar],
    k: usize,
) -> Vec<Pointer> {
    assert_ne!(source.len(), 0);
    let mut visited: HashSet<Pointer> = source.iter().copied().collect();
    let mut candidates: BTreeSet<(F64, Pointer)> = source
        .iter()
        .map(|&x| (distance(&read_vector(table, x), target).into(), x))
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
        for e in read_layer(table, c, level).into_iter() {
            if visited.contains(&e) {
                continue;
            }
            visited.insert(e);
            let e_dis = distance(&read_vector(table, e), target);
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
    result.into_iter().map(|(_, pointer)| pointer).collect()
}

fn push(table: &mut Table, source: &mut [Pointer], target: Pointer) {
    if source.len() == 0 {
        return;
    }
    let vector = read_vector(table, target);
    let mut index = source.len() - 1;
    while index != 0
        && distance(&vector, &read_vector(table, source[index]))
            < distance(&vector, &read_vector(table, source[(index - 1) / 2]))
    {
        source.swap(index, (index - 1) / 2);
        index = (index - 1) / 2;
    }
}

fn read_metadata(table: &mut Table) -> Option<(Pointer, u8)> {
    unsafe { force_metadata(table.read(META_TUPLE)).get() }
}

fn try_to_write_metadata(table: &mut Table, metadata: Option<(Pointer, u8)>) {
    unsafe {
        let tuple = force_metadata_mut(table.write(META_TUPLE));
        if (*tuple).pointer.is_null() || (*tuple).level < metadata.unwrap().1 {
            tuple.set(metadata)
        }
    }
}

fn append(
    table: &mut Table,
    heap_pointer: HeapPointer,
    vector: &[Scalar],
    layers: &[Vec<Pointer>],
) -> Pointer {
    unsafe {
        let level = layers.len() - 1;
        let size = DataTuple::size(vector.len() as _, level as _);
        let layout = Layout::from_size_align(size, 8).unwrap().pad_to_align();
        let ptr = std::alloc::Global.allocate_zeroed(layout).unwrap();
        let raw = ptr.as_ptr() as *mut DataTuple;
        (*raw).deleted = false;
        (*raw).heap_pointer = heap_pointer;
        (*raw).size_layers = level as _;
        (*raw).size_vector = vector.len() as _;
        raw.vector_mut().copy_from_slice(vector);
        for i in 0..=level {
            raw.layer_set(i as _, &layers[i as usize]);
        }
        let pointer = table.append(ptr.as_ref());
        std::alloc::Global.deallocate(ptr.cast(), layout);
        pointer
    }
}

fn read_layer(table: &mut Table, pointer: Pointer, level: u8) -> Vec<Pointer> {
    unsafe { force(table.read(pointer)).layer_get(level) }
}

fn read_vector(table: &mut Table, pointer: Pointer) -> Vec<Scalar> {
    unsafe { force(table.read(pointer)).vector().to_vec() }
}

fn write_layer(table: &mut Table, pointer: Pointer, level: u8, layer: &[Pointer]) {
    unsafe { force_mut(table.write(pointer)).layer_set(level, layer) }
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

#[repr(C, align(8))]
struct MetadataTuple {
    pointer: Pointer,
    level: u8,
}

impl MetadataTuple {
    unsafe fn get(self: *const Self) -> Option<(Pointer, u8)> {
        if (*self).pointer.is_null() {
            None
        } else {
            Some(((*self).pointer, (*self).level))
        }
    }
    unsafe fn set(self: *mut Self, metadata: Option<(Pointer, u8)>) {
        if let Some((pointer, level)) = metadata {
            (*self).pointer = pointer;
            (*self).level = level;
        } else {
            (*self).pointer = Pointer::NULL;
        }
    }
    fn to_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const _ as *const u8, std::mem::size_of::<Self>())
        }
    }
}

#[repr(C, align(8))]
struct DataTuple {
    deleted: bool,
    heap_pointer: HeapPointer,
    size_layers: u8,
    size_vector: u16,
    _x: [u64; 0],
}

static_assertions::assert_eq_align!(Scalar, u64);

impl DataTuple {
    fn size(size_vector: u16, size_layers: u8) -> usize {
        std::mem::size_of::<Self>()
            + std::mem::size_of::<Scalar>() * size_vector as usize
            + std::mem::size_of::<(u64, [Pointer; MAX_CONNECTIONS])>() * (1 + size_layers) as usize
    }
    unsafe fn vector<'a>(self: *const Self) -> &'a [Scalar] {
        let offset = std::mem::size_of::<Self>();
        let ptr = (self as *const u8).add(offset);
        std::slice::from_raw_parts(ptr as _, (*self).size_vector as usize)
    }
    unsafe fn vector_mut<'a>(self: *mut Self) -> &'a mut [Scalar] {
        let offset = std::mem::size_of::<Self>();
        let ptr = (self as *mut u8).add(offset);
        std::slice::from_raw_parts_mut(ptr as _, (*self).size_vector as usize)
    }
    unsafe fn layer_get(self: *const Self, level: u8) -> Vec<Pointer> {
        assert!(level <= (*self).size_layers);
        let offset = std::mem::size_of::<Self>()
            + std::mem::size_of::<Scalar>() * (*self).size_vector as usize
            + std::mem::size_of::<(u64, [Pointer; MAX_CONNECTIONS])>() * level as usize;
        let ptr = (self as *mut u8).add(offset);
        let (len, array) = (ptr as *mut (u64, [Pointer; MAX_CONNECTIONS])).read();
        Vec::from(&array[..len as usize])
    }
    unsafe fn layer_set(self: *mut Self, level: u8, data: &[Pointer]) {
        assert!(level <= (*self).size_layers);
        let offset = std::mem::size_of::<Self>()
            + std::mem::size_of::<Scalar>() * (*self).size_vector as usize
            + std::mem::size_of::<(u64, [Pointer; MAX_CONNECTIONS])>() * level as usize;
        let ptr = (self as *mut u8).add(offset);
        let mut array = [Pointer::NULL; MAX_CONNECTIONS];
        let len = MAX_CONNECTIONS.min(data.len());
        array[..len].copy_from_slice(&data[..len]);
        (ptr as *mut (u64, [Pointer; MAX_CONNECTIONS])).write((len as u64, array));
    }
}

fn force(data: Option<&[u8]>) -> *const DataTuple {
    data.unwrap().as_ptr() as _
}

fn force_mut(data: Option<&mut [u8]>) -> *mut DataTuple {
    data.unwrap().as_ptr() as _
}

fn force_metadata(data: Option<&[u8]>) -> *const MetadataTuple {
    data.unwrap().as_ptr() as _
}

fn force_metadata_mut(data: Option<&mut [u8]>) -> *mut MetadataTuple {
    data.unwrap().as_ptr() as _
}
