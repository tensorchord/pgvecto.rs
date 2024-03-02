#[cfg(target_arch = "x86_64")]
#[link(name = "vectorsc", kind = "static")]
extern "C" {
    pub fn v_f16_cosine_avx512fp16(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_dot_avx512fp16(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_sl2_avx512fp16(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_cosine_v4(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_dot_v4(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_sl2_v4(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_cosine_v3(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_dot_v3(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_sl2_v3(a: *const u16, b: *const u16, n: usize) -> f32;

    pub fn v_sparse_dot_avx512vp2intersect(
        lhs_idx: *const u32,
        rhs_idx: *const u32,
        lhs_val: *const f32,
        rhs_val: *const f32,
        lhs_len: usize,
        rhs_len: usize,
    ) -> f32;
}
