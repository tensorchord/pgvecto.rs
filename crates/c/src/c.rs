#[link(name = "pgvectorsc", kind = "static")]
extern "C" {
    pub fn vectors_f16_cosine_axv512(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn vectors_f16_dot_axv512(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn vectors_f16_distance_squared_l2_axv512(a: *const u16, b: *const u16, n: usize) -> f32;
}
