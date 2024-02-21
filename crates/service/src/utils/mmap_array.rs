use bytemuck::{Pod, Zeroable};
use std::fs::File;
use std::io::{BufWriter, Read, Seek, Write};
use std::ops::Index;
use std::ops::{Deref, Range, RangeInclusive};
use std::path::Path;

pub struct MmapArray<T> {
    info: Information,
    outp: *const [T],
    _mmap: memmap2::Mmap,
}

impl<T> MmapArray<T>
where
    T: Pod,
{
    pub fn create<I>(path: &Path, iter: I) -> Self
    where
        I: Iterator<Item = T>,
    {
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(path)
            .unwrap();
        let mut info = Information { len: 0 };
        let mut buffered = BufWriter::new(&file);
        for data in iter {
            buffered.write_all(bytemuck::bytes_of(&data)).unwrap();
            info.len += 1;
        }
        buffered.write_all(&[0u8; 4096]).unwrap();
        buffered.write_all(bytemuck::bytes_of(&info)).unwrap();
        buffered.flush().unwrap();
        file.sync_all().unwrap();
        let mmap = unsafe { read_mmap(&file, info.len * std::mem::size_of::<T>()) };
        let outp = unsafe { std::slice::from_raw_parts(mmap.as_ptr() as *const T, info.len) };
        Self {
            info,
            outp,
            _mmap: mmap,
        }
    }
    pub fn open(path: &Path) -> Self {
        let file = std::fs::OpenOptions::new().read(true).open(path).unwrap();
        let info = read_information(&file);
        let mmap = unsafe { read_mmap(&file, info.len * std::mem::size_of::<T>()) };
        let outp = unsafe { std::slice::from_raw_parts(mmap.as_ptr() as *const T, info.len) };
        Self {
            info,
            outp,
            _mmap: mmap,
        }
    }
    pub fn len(&self) -> usize {
        self.info.len
    }
}

impl<T> Deref for MmapArray<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        unsafe { &*self.outp }
    }
}

impl<T> Index<usize> for MmapArray<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        unsafe { &(*self.outp)[index] }
    }
}

impl<T> Index<Range<usize>> for MmapArray<T> {
    type Output = [T];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        unsafe { &(*self.outp)[index] }
    }
}

impl<T> Index<RangeInclusive<usize>> for MmapArray<T> {
    type Output = [T];

    fn index(&self, index: RangeInclusive<usize>) -> &Self::Output {
        unsafe { &(*self.outp)[index] }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct Information {
    len: usize,
}

unsafe impl Zeroable for Information {}
unsafe impl Pod for Information {}

fn read_information(mut file: &File) -> Information {
    let size = std::mem::size_of::<Information>();
    file.seek(std::io::SeekFrom::End(-(size as i64))).unwrap();
    let mut buff = vec![0u8; size];
    file.read_exact(&mut buff).unwrap();
    bytemuck::try_pod_read_unaligned::<Information>(&buff).unwrap()
}

unsafe fn read_mmap(file: &File, len: usize) -> memmap2::Mmap {
    let len = len.next_multiple_of(4096);
    unsafe {
        memmap2::MmapOptions::new()
            .populate()
            .len(len)
            .map(file)
            .unwrap()
    }
}
