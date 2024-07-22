use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use base::scalar::F32;
use base::vector::{OwnedVector, Vecf32Owned};

use num_traits::{FromBytes, Num};

fn read_vecs<T>(path: &Path) -> std::io::Result<Vec<Vec<T>>>
where
    T: Sized + FromBytes<Bytes = [u8; 4]>,
{
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
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

pub fn convert_to_owned_vec(vec: &[f32]) -> OwnedVector {
    OwnedVector::Vecf32(Vecf32Owned::new(vec.iter().map(|v| F32(*v)).collect()))
}

pub fn read_vectors<T>(path: &Path) -> std::io::Result<Vec<Vec<T>>>
where
    T: Num + FromBytes<Bytes = [u8; 4]>,
{
    match path.extension().and_then(OsStr::to_str) {
        Some("fvecs") => read_vecs::<T>(path),
        Some("ivecs") => read_vecs::<T>(path),
        Some(_) => todo!(),
        None => Err(std::io::ErrorKind::Unsupported.into()),
    }
}
