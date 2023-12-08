#[link(name = "pgvectorsc", kind = "static")]
extern "C" {
    pub fn v_f16_cosine_axv512(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_dot_axv512(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_sl2_axv512(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_cosine_axv2(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_dot_axv2(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_sl2_axv2(a: *const u16, b: *const u16, n: usize) -> f32;
}
