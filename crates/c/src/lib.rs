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

    pub fn v_binary_cosine_avx512vpopcntdq(a: *const usize, b: *const usize, n: usize) -> f32;
    pub fn v_binary_dot_avx512vpopcntdq(a: *const usize, b: *const usize, n: usize) -> f32;
    pub fn v_binary_sl2_avx512vpopcntdq(a: *const usize, b: *const usize, n: usize) -> f32;
    pub fn v_binary_cnt_avx512vpopcntdq(a: *const usize, n: usize) -> f32;
    pub fn v_binary_cosine_v4(a: *const usize, b: *const usize, n: usize) -> f32;
    pub fn v_binary_dot_v4(a: *const usize, b: *const usize, n: usize) -> f32;
    pub fn v_binary_sl2_v4(a: *const usize, b: *const usize, n: usize) -> f32;
    pub fn v_binary_cnt_v4(a: *const usize, n: usize) -> f32;
    pub fn v_binary_cosine_v3(a: *const usize, b: *const usize, n: usize) -> f32;
    pub fn v_binary_dot_v3(a: *const usize, b: *const usize, n: usize) -> f32;
    pub fn v_binary_sl2_v3(a: *const usize, b: *const usize, n: usize) -> f32;
    pub fn v_binary_cnt_v3(a: *const usize, n: usize) -> f32;
}
