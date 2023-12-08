#[cfg(target_arch = "x86_64")]
#[link(name = "pgvectorsc", kind = "static")]
extern "C" {
    pub fn v_f16_cosine_axv512(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_dot_axv512(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_sl2_axv512(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_cosine_axv2(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_dot_axv2(a: *const u16, b: *const u16, n: usize) -> f32;
    pub fn v_f16_sl2_axv2(a: *const u16, b: *const u16, n: usize) -> f32;
}

// `compiler_builtin` defines `__extendhfsf2` with integer calling convention.
// However C compilers links `__extendhfsf2` with floating calling convention.
// The code should be removed once Rust offically supports `f16`.

#[no_mangle]
#[linkage = "external"]
extern "C" fn __extendhfsf2(f: f64) -> f32 {
    unsafe {
        let f: half::f16 = std::mem::transmute_copy(&f);
        f.to_f32()
    }
}
