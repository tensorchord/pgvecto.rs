use crate::datatype::Scalar;
use crate::postgres::table::{BuildTable, HeapPointer, Pointer, RegularTable};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet};
use std::hash::{Hash, Hasher};

const MAX_CONNECTIONS: usize = 16;
const MAKE_CONNECTIONS: usize = 256;

const META_TUPLE: Pointer = Pointer { page: 0, id: 0 };

pub struct BuildAlgo {
    table: BuildTable,
}

impl BuildAlgo {
    pub unsafe fn new(table: BuildTable) -> Self {
        Self { table }
    }
    pub fn build(&mut self) {
        assert_eq!(self.table.pages(), 0);
        let size = MetadataTuple::size();
        let mut guard = self.table.append(size as u16);
        unsafe {
            (*guard.cast_mut::<MetadataTuple>()).pointer = Pointer::NULL;
            (*guard.cast_mut::<MetadataTuple>()).level = 0;
        }
    }
    pub fn build_insert(&mut self, vector: &[Scalar], heap_pointer: HeapPointer) {
        assert_ne!(self.table.pages(), 0);
        let level = rand::random::<u32>().trailing_zeros() as u8;
        let mut layers = vec![Vec::<Pointer>::new(); 1 + level as usize];
        let start_points;
        let start_level;
        if let Some((entry, top)) = self.read_metadata() {
            if top >= level {
                let mut entry = entry;
                for i in (level + 1..=top).rev() {
                    entry = self.search_in_layer(i, &[entry], &vector, 1)[0];
                }
                start_points = vec![entry];
                start_level = level;
            } else {
                start_points = vec![entry];
                start_level = top;
            }
        } else {
            let pointer = self.append(heap_pointer, vector, &layers);
            self.try_to_write_metadata(Some((pointer, level)));
            return;
        }
        for i in (0..=start_level).rev() {
            layers[i as usize] = self.search_in_layer(
                i,
                if i == start_level {
                    &start_points
                } else {
                    &layers[i as usize + 1]
                },
                &vector,
                MAKE_CONNECTIONS,
            );
            layers[i as usize].truncate(MAX_CONNECTIONS);
        }
        let pointer = self.append(heap_pointer, vector, &layers);
        for i in (0..=start_level).rev() {
            for &neighbour in layers[i as usize].iter() {
                let mut layer = self.read_layer(neighbour, i);
                layer.push(pointer);
                self.push(&mut layer, neighbour);
                layer.truncate(MAX_CONNECTIONS);
                self.write_layer(neighbour, i, &layer);
            }
        }
        self.try_to_write_metadata(Some((pointer, level)));
    }
    fn push(&mut self, source: &mut [Pointer], target: Pointer) {
        if source.len() == 0 {
            return;
        }
        let vector = self.read_vector(target);
        let mut index = source.len() - 1;
        while index != 0
            && distance(&vector, &self.read_vector(source[index]))
                < distance(&vector, &self.read_vector(source[(index - 1) / 2]))
        {
            source.swap(index, (index - 1) / 2);
            index = (index - 1) / 2;
        }
    }
    fn search_in_layer(
        &mut self,
        level: u8,
        source: &[Pointer],
        target: &[Scalar],
        k: usize,
    ) -> Vec<Pointer> {
        assert_ne!(source.len(), 0);
        let mut visited: HashSet<Pointer> = source.iter().copied().collect();
        let mut candidates: BTreeSet<(F64, Pointer)> = source
            .iter()
            .map(|&x| (distance(&self.read_vector(x), target).into(), x))
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
            for e in self.read_layer(c, level).into_iter() {
                if visited.contains(&e) {
                    continue;
                }
                visited.insert(e);
                let e_dis = distance(&self.read_vector(e), target);
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
    fn read_metadata(&mut self) -> Option<(Pointer, u8)> {
        unsafe {
            let guard = self.table.write(META_TUPLE).unwrap();
            let tuple = guard.cast::<MetadataTuple>();
            tuple.get()
        }
    }
    fn try_to_write_metadata(&mut self, metadata: Option<(Pointer, u8)>) {
        unsafe {
            let mut guard = self.table.write(META_TUPLE).unwrap();
            let tuple = guard.cast_mut::<MetadataTuple>();
            if (*tuple).pointer.is_null() || (*tuple).level < metadata.unwrap().1 {
                tuple.set(metadata)
            }
        }
    }
    fn append(
        &mut self,
        heap_pointer: HeapPointer,
        vector: &[Scalar],
        layers: &[Vec<Pointer>],
    ) -> Pointer {
        let level = layers.len() - 1;
        let size = DataTuple::size(vector.len() as _, level as _);
        let mut guard = self.table.append(size as u16);
        unsafe {
            let tuple = guard.cast_mut::<DataTuple>();
            (*tuple).deleted = false;
            (*tuple).heap_pointer = heap_pointer;
            (*tuple).size_layers = level as _;
            (*tuple).size_vector = vector.len() as _;
            tuple.vector_mut().copy_from_slice(vector);
            for i in 0..=level {
                tuple.layer_set(level as _, &layers[i as usize]);
            }
        }
        guard.pointer()
    }
    fn read_layer(&mut self, pointer: Pointer, level: u8) -> Vec<Pointer> {
        unsafe {
            let guard = self.table.read(pointer).unwrap();
            let tuple = guard.cast::<DataTuple>();
            tuple.layer_get(level)
        }
    }
    fn read_vector(&mut self, pointer: Pointer) -> Vec<Scalar> {
        unsafe {
            let guard = self.table.read(pointer).unwrap();
            let tuple = guard.cast::<DataTuple>();
            tuple.vector().to_vec()
        }
    }
    fn write_layer(&mut self, pointer: Pointer, level: u8, layer: &[Pointer]) {
        unsafe {
            let mut guard = self.table.write(pointer).unwrap();
            let tuple = guard.cast_mut::<DataTuple>();
            tuple.layer_set(level, layer)
        }
    }
}

pub struct RegularAlgo {
    table: RegularTable,
}

impl RegularAlgo {
    pub unsafe fn new(table: RegularTable) -> Self {
        Self { table }
    }
    pub fn search(&mut self, vector: &[Scalar], k: usize) -> Vec<HeapPointer> {
        if let Some((entry, top)) = self.read_metadata() {
            let mut e = entry;
            for i in (1..=top).rev() {
                e = self.search_in_layer(i, &[e], vector, 1)[0];
            }
            let result = self.search_in_layer(0, &[e], vector, k);
            result
                .into_iter()
                .filter_map(|p| unsafe {
                    let guard = self.table.read(p).unwrap();
                    if !(*guard.cast::<DataTuple>()).deleted {
                        Some((*guard.cast::<DataTuple>()).heap_pointer)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }
    pub fn insert(&mut self, vector: &[Scalar], heap_pointer: HeapPointer) {
        let level = rand::random::<u32>().trailing_zeros() as u8;
        let mut layers = vec![Vec::<Pointer>::new(); 1 + level as usize];
        let start_vectors;
        let start_level;
        if let Some((entry, top)) = self.read_metadata() {
            if top >= level {
                let mut entry = entry;
                for i in (level + 1..=top).rev() {
                    entry = self.search_in_layer(i, &[entry], &vector, 1)[0];
                }
                start_vectors = vec![entry];
                start_level = level;
            } else {
                start_vectors = vec![entry];
                start_level = top;
            }
        } else {
            let pointer = self.append(heap_pointer, vector, &layers);
            self.try_to_write_metadata(Some((pointer, level)));
            return;
        }
        for i in (0..=start_level).rev() {
            layers[i as usize] = self.search_in_layer(
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
        let pointer = self.append(heap_pointer, vector, &layers);
        for i in (0..=start_level).rev() {
            for &neighbour in layers[i as usize].iter() {
                let mut layer = self.read_layer(neighbour, i);
                layer.push(pointer);
                self.push(&mut layer, neighbour);
                layer.truncate(MAX_CONNECTIONS);
                self.write_layer(neighbour, i, &layer);
            }
        }
        self.try_to_write_metadata(Some((pointer, level)));
    }
    fn push(&mut self, source: &mut [Pointer], target: Pointer) {
        if source.len() == 0 {
            return;
        }
        let vector = self.read_vector(target);
        let mut index = source.len() - 1;
        while index != 0
            && distance(&vector, &self.read_vector(source[index]))
                < distance(&vector, &self.read_vector(source[(index - 1) / 2]))
        {
            source.swap(index, (index - 1) / 2);
            index = (index - 1) / 2;
        }
    }
    pub fn vacuum<F>(&mut self, mut f: F)
    where
        F: FnMut(HeapPointer) -> bool,
    {
        let n = self.table.pages();
        for page in 0..n {
            for id in 0..=u16::MAX {
                let pointer = Pointer::new(page, id);
                if pointer == META_TUPLE {
                    continue;
                }
                let Some(mut guard) = self.table.write(pointer) else { break };
                let tuple = guard.cast_mut::<DataTuple>();
                unsafe {
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
        &mut self,
        level: u8,
        source: &[Pointer],
        target: &[Scalar],
        k: usize,
    ) -> Vec<Pointer> {
        assert_ne!(source.len(), 0);
        let mut visited: HashSet<Pointer> = source.iter().copied().collect();
        let mut candidates: BTreeSet<(F64, Pointer)> = source
            .iter()
            .map(|&x| (distance(&self.read_vector(x), target).into(), x))
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
            for e in self.read_layer(c, level).into_iter() {
                if visited.contains(&e) {
                    continue;
                }
                visited.insert(e);
                let e_dis = distance(&self.read_vector(e), target);
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
    fn read_metadata(&mut self) -> Option<(Pointer, u8)> {
        unsafe {
            let guard = self.table.write(META_TUPLE).unwrap();
            let tuple = guard.cast::<MetadataTuple>();
            tuple.get()
        }
    }
    fn try_to_write_metadata(&mut self, metadata: Option<(Pointer, u8)>) {
        unsafe {
            let mut guard = self.table.write(META_TUPLE).unwrap();
            let tuple = guard.cast_mut::<MetadataTuple>();
            if (*tuple).pointer.is_null() || (*tuple).level < metadata.unwrap().1 {
                tuple.set(metadata)
            }
        }
    }
    fn append(
        &mut self,
        heap_pointer: HeapPointer,
        vector: &[Scalar],
        layers: &[Vec<Pointer>],
    ) -> Pointer {
        let level = layers.len() - 1;
        let size = DataTuple::size(vector.len() as _, level as _);
        let mut guard = self.table.append(size as u16);
        unsafe {
            let tuple = guard.cast_mut::<DataTuple>();
            (*tuple).deleted = false;
            (*tuple).heap_pointer = heap_pointer;
            (*tuple).size_layers = level as _;
            (*tuple).size_vector = vector.len() as _;
            tuple.vector_mut().copy_from_slice(vector);
            for i in 0..=level {
                tuple.layer_set(level as _, &layers[i as usize]);
            }
        }
        guard.pointer()
    }
    fn read_layer(&mut self, pointer: Pointer, level: u8) -> Vec<Pointer> {
        unsafe {
            let guard = self.table.read(pointer).unwrap();
            let tuple = guard.cast::<DataTuple>();
            tuple.layer_get(level)
        }
    }
    fn read_vector(&mut self, pointer: Pointer) -> Vec<Scalar> {
        unsafe {
            let guard = self.table.read(pointer).unwrap();
            let tuple = guard.cast::<DataTuple>();
            tuple.vector().to_vec()
        }
    }
    fn write_layer(&mut self, pointer: Pointer, level: u8, layer: &[Pointer]) {
        unsafe {
            let mut guard = self.table.write(pointer).unwrap();
            let tuple = guard.cast_mut::<DataTuple>();
            tuple.layer_set(level, layer)
        }
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

#[repr(C, align(8))]
struct MetadataTuple {
    pointer: Pointer,
    level: u8,
}

impl MetadataTuple {
    fn size() -> usize {
        std::mem::size_of::<Self>()
    }
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
            + (std::mem::size_of::<u64>() + std::mem::size_of::<Pointer>() * MAX_CONNECTIONS)
                * (1 + size_layers) as usize
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
        if level > (*self).size_layers {
            unsafe {
                std::intrinsics::breakpoint();
            }
        }
        assert!(level <= (*self).size_layers);
        let offset = std::mem::size_of::<DataTuple>()
            + std::mem::size_of::<Scalar>() * (*self).size_vector as usize
            + std::mem::size_of::<(u64, [Pointer; MAX_CONNECTIONS])>() * level as usize;
        let ptr = (self as *mut u8).add(offset);
        let (len, array) = (ptr as *mut (u64, [Pointer; MAX_CONNECTIONS])).read();
        Vec::from(&array[..len as usize])
    }
    unsafe fn layer_set(self: *mut Self, level: u8, data: &[Pointer]) {
        assert!(level <= (*self).size_layers);
        let offset = std::mem::size_of::<DataTuple>()
            + std::mem::size_of::<Scalar>() * (*self).size_vector as usize
            + std::mem::size_of::<(u64, [Pointer; MAX_CONNECTIONS])>() * level as usize;
        let ptr = (self as *mut u8).add(offset);
        let mut array = [Pointer::NULL; MAX_CONNECTIONS];
        let len = MAX_CONNECTIONS.min(data.len());
        array[..len].copy_from_slice(&data[..len]);
        (ptr as *mut (u64, [Pointer; MAX_CONNECTIONS])).write((len as u64, array));
    }
}
