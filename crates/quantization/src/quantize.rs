use base::scalar::F32;
use num_traits::Float;

pub fn quantize_255(lut: &[F32]) -> (F32, F32, Vec<u8>) {
    let min = lut.iter().copied().fold(F32::infinity(), std::cmp::min);
    let max = lut.iter().copied().fold(F32::neg_infinity(), std::cmp::max);
    let k = std::cmp::max(max - min, F32(0.0)) / F32(255.0);
    let b = min;
    (k, b, lut.iter().map(|&y| ((y - b) / k).0 as u8).collect())
}

pub fn quantize_15(lut: &[F32]) -> (F32, F32, Vec<u8>) {
    let min = lut.iter().copied().fold(F32::infinity(), std::cmp::min);
    let max = lut.iter().copied().fold(F32::neg_infinity(), std::cmp::max);
    let k = std::cmp::max(max - min, F32(0.0)) / F32(15.0);
    let b = min;
    (k, b, lut.iter().map(|&y| ((y - b) / k).0 as u8).collect())
}

pub fn dequantize(sum_1: u32, k: F32, b: F32, sum_x: u16) -> F32 {
    F32(sum_1 as f32) * b + F32(sum_x as f32) * k
}
