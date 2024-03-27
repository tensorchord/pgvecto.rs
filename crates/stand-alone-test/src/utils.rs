use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use thiserror::Error;

/// Error types that vecs operations can return.
#[derive(Error, Debug)]
pub enum VecsError {
    #[error("input is not a file")]
    InputNotAFile,
    #[error("file has invalid length")]
    InvalidFileLength,
    #[error("i/o error")]
    IOError(#[from] std::io::Error),
    #[error("invalid dimension")]
    InvalidDimension(usize),
    #[error("file exists and overwrite is not allowed")]
    FileExists,
}

/// Trait defined for converting types from and to little endian bytes.
pub trait SupportedTypes {
    fn from_le_bytes(_: &[u8]) -> Result<Self, VecsError>
    where
        Self: Sized;
    fn to_le_bytes(&self) -> Vec<u8>;
}

impl SupportedTypes for u8 {
    fn from_le_bytes(arr: &[u8]) -> Result<Self, VecsError>
    where
        Self: Sized,
    {
        let arr = arr
            .try_into()
            .map_err(|_| VecsError::InvalidDimension(arr.len()))?;
        Ok(u8::from_le_bytes(arr))
    }

    fn to_le_bytes(&self) -> Vec<u8> {
        (*self).to_le_bytes().into_iter().collect()
    }
}

impl SupportedTypes for i32 {
    fn from_le_bytes(arr: &[u8]) -> Result<Self, VecsError>
    where
        Self: Sized,
    {
        let arr = arr
            .try_into()
            .map_err(|_| VecsError::InvalidDimension(arr.len()))?;
        Ok(i32::from_le_bytes(arr))
    }

    fn to_le_bytes(&self) -> Vec<u8> {
        (*self).to_le_bytes().into_iter().collect()
    }
}

impl SupportedTypes for f32 {
    fn from_le_bytes(arr: &[u8]) -> Result<Self, VecsError>
    where
        Self: Sized,
    {
        let arr = arr
            .try_into()
            .map_err(|_| VecsError::InvalidDimension(arr.len()))?;
        Ok(f32::from_le_bytes(arr))
    }

    fn to_le_bytes(&self) -> Vec<u8> {
        (*self).to_le_bytes().into_iter().collect()
    }
}

/// Main structure returned by readers or consumed by writers.
/// It contains vectors serialized into a single Vec<T> container.
pub struct Vectors<T> {
    values: Vec<T>,
    d: usize,
}

impl<T> Vectors<T>
where
    T: SupportedTypes + Default + Copy,
{
    /// Returns the dimensions of all vectors in the container.
    pub fn get_d(&self) -> usize {
        self.d
    }

    /// Returns the number of vectors in the container.
    pub fn len(&self) -> usize {
        self.values.len() / self.d
    }

    /// Gets a single vector, enumerated starting from 0.
    pub fn get_vector(&self, i: usize) -> Option<&[T]> {
        if i >= self.values.len() / self.d {
            None
        } else {
            Some(&self.values[self.d * i..self.d * (i + 1)])
        }
    }

    /// Appends a new vector.
    pub fn add_vector(&mut self, values: &Vec<T>) -> Result<(), VecsError> {
        if values.len() != self.d {
            Err(VecsError::InvalidDimension(values.len()))
        } else {
            self.values.extend(values);
            Ok(())
        }
    }

    /// Creates a new set of vectors. The length of the input
    /// needs to be divisible by d.
    pub fn new(values: Vec<T>, d: usize) -> Result<Vectors<T>, VecsError> {
        if values.len() % d != 0 {
            Err(VecsError::InvalidDimension(d))
        } else {
            Ok(Vectors {
                values: Vec::new(),
                d,
            })
        }
    }
}

const DIMENSION_SIZE: usize = core::mem::size_of::<u32>();

/// Reads a vecs file where T is the type of the vectors (i32|f32|u8).
pub fn read_vecs_file<T: SupportedTypes + Default + Copy>(
    path_name: &str,
) -> Result<Vectors<T>, VecsError> {
    let fp = File::open(path_name).map_err(VecsError::IOError)?;
    let file_metadata = fp.metadata().map_err(VecsError::IOError)?;
    if !file_metadata.is_file() {
        return Err(VecsError::InputNotAFile);
    }
    let file_size = file_metadata.len();
    if file_size < 4 {
        return Err(VecsError::InvalidFileLength);
    }
    read_vecs(&mut BufReader::new(fp), file_size as usize)
}

/// Same as `read_vecs_file` but takes a seek-able reader and a size to
/// read (how much to consume).
pub fn read_vecs<T: SupportedTypes + Default + Copy, I: Read + Seek>(
    input: &mut I,
    data_size: usize,
) -> Result<Vectors<T>, VecsError> {
    let mut result = Vec::new();

    let mut dimensions = [0u8; DIMENSION_SIZE];
    let stream_starting_position = input.stream_position()?;
    input
        .read_exact(&mut dimensions)
        .map_err(VecsError::IOError)?;

    let _ = input.seek(SeekFrom::Start(stream_starting_position))?;
    let dimensions = u32::from_le_bytes(dimensions);

    let t_size = core::mem::size_of::<T>();
    let vector_len = (dimensions as usize) * t_size + DIMENSION_SIZE;
    let vector_cnt = data_size / vector_len;
    let mut buffer = vec![0u8; vector_len];

    for vect in 1..=vector_cnt {
        input.read_exact(&mut buffer).map_err(VecsError::IOError)?;
        let arr = buffer[0..DIMENSION_SIZE]
            .try_into()
            .map_err(|_| VecsError::InvalidDimension(vect))?;
        if u32::from_le_bytes(arr) != dimensions {
            return Err(VecsError::InvalidDimension(vect));
        }
        for i in 0..dimensions as usize {
            let start = DIMENSION_SIZE + t_size * i;
            let arr = buffer[start..start + t_size]
                .try_into()
                .map_err(|_| VecsError::InvalidDimension(i))?;
            result.push(T::from_le_bytes(arr)?);
        }
    }

    Ok(Vectors {
        values: result,
        d: dimensions as usize,
    })
}

/// Writes a vecs file. This is the counterpart to `read_vecs_file`.
pub fn write_vecs_file<T: SupportedTypes + Default + Copy>(
    vectors: &Vectors<T>,
    file_name: &str,
    allow_overwrite: bool,
) -> Result<(), VecsError> {
    let exists = Path::new(file_name).exists();
    if exists && !allow_overwrite {
        return Err(VecsError::FileExists);
    }

    let fp = File::create(file_name)?;
    write_vecs(vectors, &mut BufWriter::new(fp))
}

/// Writes vectors to a writer instead of a file.
pub fn write_vecs<T: SupportedTypes + Default + Copy>(
    vectors: &Vectors<T>,
    w: &mut dyn Write,
) -> Result<(), VecsError> {
    let d = vectors.d;
    let dim = (d as u32).to_le_bytes();

    for i in 0..(vectors.values.len() / vectors.d) {
        w.write_all(&dim).map_err(VecsError::IOError)?;
        for j in i * d..(i + 1) * d {
            let val = vectors.values[j].to_le_bytes();
            w.write_all(&val).map_err(VecsError::IOError)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::io::Cursor;

    use super::*;

    trait TruncatedSum {
        fn get_sum(i: usize, j: usize) -> Self;
    }

    impl TruncatedSum for u8 {
        fn get_sum(i: usize, j: usize) -> u8 {
            (i + j) as u8
        }
    }

    impl TruncatedSum for f32 {
        fn get_sum(i: usize, j: usize) -> f32 {
            (i + j) as f32
        }
    }

    impl TruncatedSum for i32 {
        fn get_sum(i: usize, j: usize) -> i32 {
            (i + j) as i32
        }
    }

    #[test]
    fn test_u8() -> Result<(), VecsError> {
        round_trip::<u8>()
    }

    #[test]
    fn test_u32() -> Result<(), VecsError> {
        round_trip::<i32>()
    }

    #[test]
    fn test_f32() -> Result<(), VecsError> {
        round_trip::<f32>()
    }

    fn round_trip<T: PartialEq + TruncatedSum + SupportedTypes + Copy + Default + Debug>(
    ) -> Result<(), VecsError> {
        let n = 100;
        let d = 7;
        let mut vectors: Vectors<T> = Vectors::new(Vec::new(), d)?;
        for i in 0..n {
            let mut v: Vec<T> = Vec::new();
            for j in 0..d {
                v.push(T::get_sum(i, j));
            }
            vectors.add_vector(&v)?;
        }

        let mut c = Cursor::new(Vec::new());

        // save
        write_vecs(&vectors, &mut c)?;

        // read
        let vector_size = c.position() as usize;
        c.set_position(0u64);
        let read_vectors: Vectors<T> = read_vecs(&mut c, vector_size)?;

        // compare
        for i in 0..n {
            let v1 = vectors
                .get_vector(i)
                .ok_or_else(|| VecsError::InvalidFileLength)?;
            let v2 = read_vectors
                .get_vector(i)
                .ok_or_else(|| VecsError::InvalidFileLength)?;
            assert_eq!(v1, v2);
        }
        Ok(())
    }
}
