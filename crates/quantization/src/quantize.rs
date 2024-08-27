use base::scalar::*;

pub fn quantize_255(lut: &[f32]) -> (f32, f32, Vec<u8>) {
    let (min, max) = f32::reduce_min_max_of_x(lut);
    let k = 0.0f32.max((max - min) / 255.0);
    let b = min;
    (k, b, lut.iter().map(|&y| ((y - b) / k) as u8).collect())
}

pub fn quantize_15(lut: &[f32]) -> (f32, f32, Vec<u8>) {
    let (min, max) = f32::reduce_min_max_of_x(lut);
    let k = 0.0f32.max((max - min) / 15.0);
    let b = min;
    (k, b, lut.iter().map(|&y| ((y - b) / k) as u8).collect())
}

pub fn dequantize(sum_1: u32, k: f32, b: f32, sum_x: u16) -> f32 {
    (sum_1 as f32) * b + (sum_x as f32) * k
}
