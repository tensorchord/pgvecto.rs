/*

## codes layout for 4-bit quantizer

group i = | vector i | (total bytes = width/2)

byte:      | 0      | 1      | 2      | ... | width/2 - 1  |
bits 0..3: | code 0 | code 2 | code 4 | ... | code width-2 |
bits 4..7: | code 1 | code 3 | code 5 | ... | code width-1 |

## packed_codes layout for 4-bit quantizer

group i = | vector 32i | vector 32i+1 | vector 32i+2 | ... | vector 32i+31 | (total bytes = width * 16)

byte      | 0                | 1                | 2                | ... | 14               | 15               |
bits 0..3 | code 0,vector 0  | code 0,vector 8  | code 0,vector 1  | ... | code 0,vector 14 | code 0,vector 15 |
bits 4..7 | code 0,vector 16 | code 0,vector 24 | code 0,vector 17 | ... | code 0,vector 30 | code 0,vector 31 |

byte      | 16               | 17               | 18               | ... | 30               | 31               |
bits 0..3 | code 1,vector 0  | code 1,vector 8  | code 1,vector 1  | ... | code 1,vector 14 | code 1,vector 15 |
bits 4..7 | code 1,vector 16 | code 1,vector 24 | code 1,vector 17 | ... | code 1,vector 30 | code 1,vector 31 |

byte      | 32               | 33               | 34               | ... | 46               | 47               |
bits 0..3 | code 2,vector 0  | code 2,vector 8  | code 2,vector 1  | ... | code 2,vector 14 | code 2,vector 15 |
bits 4..7 | code 2,vector 16 | code 2,vector 24 | code 2,vector 17 | ... | code 2,vector 30 | code 2,vector 31 |

...

byte      | width*32-32              | width*32-31              | ... | width*32-1               |
bits 0..3 | code (width-1),vector 0  | code (width-1),vector 8  | ... | code (width-1),vector 15 |
bits 4..7 | code (width-1),vector 16 | code (width-1),vector 24 | ... | code (width-1),vector 31 |

*/

pub const BLOCK_SIZE: u32 = 32;

pub fn pack(width: u32, r: [Vec<u8>; 32]) -> impl Iterator<Item = u8> {
    (0..width as usize).flat_map(move |i| {
        [
            r[0][i] | (r[16][i] << 4),
            r[8][i] | (r[24][i] << 4),
            r[1][i] | (r[17][i] << 4),
            r[9][i] | (r[25][i] << 4),
            r[2][i] | (r[18][i] << 4),
            r[10][i] | (r[26][i] << 4),
            r[3][i] | (r[19][i] << 4),
            r[11][i] | (r[27][i] << 4),
            r[4][i] | (r[20][i] << 4),
            r[12][i] | (r[28][i] << 4),
            r[5][i] | (r[21][i] << 4),
            r[13][i] | (r[29][i] << 4),
            r[6][i] | (r[22][i] << 4),
            r[14][i] | (r[30][i] << 4),
            r[7][i] | (r[23][i] << 4),
            r[15][i] | (r[31][i] << 4),
        ]
        .into_iter()
    })
}

mod fast_scan_b4 {
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn fast_scan_b4_v4(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 32] {
        // bounds checking is not enforced by compiler, so check it manually
        assert_eq!(codes.len(), width as usize * 16);
        assert_eq!(lut.len(), width as usize * 16);

        unsafe {
            use std::arch::x86_64::*;

            #[inline]
            #[detect::target_cpu(enable = "v4")]
            unsafe fn combine2x2(x0x1: __m256i, y0y1: __m256i) -> __m256i {
                unsafe {
                    let x1y0 = _mm256_permute2f128_si256(x0x1, y0y1, 0x21);
                    let x0y1 = _mm256_blend_epi32(x0x1, y0y1, 0xf0);
                    _mm256_add_epi16(x1y0, x0y1)
                }
            }

            #[inline]
            #[detect::target_cpu(enable = "v4")]
            unsafe fn combine4x2(x0x1x2x3: __m512i, y0y1y2y3: __m512i) -> __m256i {
                unsafe {
                    let x0x1 = _mm512_castsi512_si256(x0x1x2x3);
                    let x2x3 = _mm512_extracti64x4_epi64(x0x1x2x3, 1);
                    let y0y1 = _mm512_castsi512_si256(y0y1y2y3);
                    let y2y3 = _mm512_extracti64x4_epi64(y0y1y2y3, 1);
                    let x01y01 = combine2x2(x0x1, y0y1);
                    let x23y23 = combine2x2(x2x3, y2y3);
                    _mm256_add_epi16(x01y01, x23y23)
                }
            }

            let mut accu_0 = _mm512_setzero_si512();
            let mut accu_1 = _mm512_setzero_si512();
            let mut accu_2 = _mm512_setzero_si512();
            let mut accu_3 = _mm512_setzero_si512();

            let mut i = 0_usize;
            while i + 4 <= width as usize {
                let c = _mm512_loadu_si512(codes.as_ptr().add(i * 16).cast());

                let mask = _mm512_set1_epi8(0xf);
                let clo = _mm512_and_si512(c, mask);
                let chi = _mm512_and_si512(_mm512_srli_epi16(c, 4), mask);

                let lut = _mm512_loadu_si512(lut.as_ptr().add(i * 16).cast());
                let res_lo = _mm512_shuffle_epi8(lut, clo);
                accu_0 = _mm512_add_epi16(accu_0, res_lo);
                accu_1 = _mm512_add_epi16(accu_1, _mm512_srli_epi16(res_lo, 8));
                let res_hi = _mm512_shuffle_epi8(lut, chi);
                accu_2 = _mm512_add_epi16(accu_2, res_hi);
                accu_3 = _mm512_add_epi16(accu_3, _mm512_srli_epi16(res_hi, 8));

                i += 4;
            }
            if i + 2 <= width as usize {
                let c = _mm256_loadu_si256(codes.as_ptr().add(i * 16).cast());

                let mask = _mm256_set1_epi8(0xf);
                let clo = _mm256_and_si256(c, mask);
                let chi = _mm256_and_si256(_mm256_srli_epi16(c, 4), mask);

                let lut = _mm256_loadu_si256(lut.as_ptr().add(i * 16).cast());
                let res_lo = _mm512_zextsi256_si512(_mm256_shuffle_epi8(lut, clo));
                accu_0 = _mm512_add_epi16(accu_0, res_lo);
                accu_1 = _mm512_add_epi16(accu_1, _mm512_srli_epi16(res_lo, 8));
                let res_hi = _mm512_zextsi256_si512(_mm256_shuffle_epi8(lut, chi));
                accu_2 = _mm512_add_epi16(accu_2, res_hi);
                accu_3 = _mm512_add_epi16(accu_3, _mm512_srli_epi16(res_hi, 8));

                i += 2;
            }
            if i < width as usize {
                let c = _mm_loadu_si128(codes.as_ptr().add(i * 16).cast());

                let mask = _mm_set1_epi8(0xf);
                let clo = _mm_and_si128(c, mask);
                let chi = _mm_and_si128(_mm_srli_epi16(c, 4), mask);

                let lut = _mm_loadu_si128(lut.as_ptr().add(i * 16).cast());
                let res_lo = _mm512_zextsi128_si512(_mm_shuffle_epi8(lut, clo));
                accu_0 = _mm512_add_epi16(accu_0, res_lo);
                accu_1 = _mm512_add_epi16(accu_1, _mm512_srli_epi16(res_lo, 8));
                let res_hi = _mm512_zextsi128_si512(_mm_shuffle_epi8(lut, chi));
                accu_2 = _mm512_add_epi16(accu_2, res_hi);
                accu_3 = _mm512_add_epi16(accu_3, _mm512_srli_epi16(res_hi, 8));

                i += 1;
            }
            debug_assert_eq!(i, width as usize);

            let mut result = [0_u16; 32];

            accu_0 = _mm512_sub_epi16(accu_0, _mm512_slli_epi16(accu_1, 8));
            _mm256_storeu_si256(
                result.as_mut_ptr().add(0).cast(),
                combine4x2(accu_0, accu_1),
            );

            accu_2 = _mm512_sub_epi16(accu_2, _mm512_slli_epi16(accu_3, 8));
            _mm256_storeu_si256(
                result.as_mut_ptr().add(16).cast(),
                combine4x2(accu_2, accu_3),
            );

            result
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn fast_scan_b4_v4_test() {
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        for _ in 0..200 {
            for width in 90..110 {
                let codes = (0..16 * width).map(|_| rand::random()).collect::<Vec<u8>>();
                let lut = (0..16 * width).map(|_| rand::random()).collect::<Vec<u8>>();
                unsafe {
                    assert_eq!(
                        fast_scan_b4_v4(width, &codes, &lut),
                        fast_scan_b4_fallback(width, &codes, &lut)
                    );
                }
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    unsafe fn fast_scan_b4_v3(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 32] {
        // bounds checking is not enforced by compiler, so check it manually
        assert_eq!(codes.len(), width as usize * 16);
        assert_eq!(lut.len(), width as usize * 16);

        unsafe {
            use std::arch::x86_64::*;

            #[inline]
            #[detect::target_cpu(enable = "v3")]
            unsafe fn combine2x2(x0x1: __m256i, y0y1: __m256i) -> __m256i {
                unsafe {
                    let x1y0 = _mm256_permute2f128_si256(x0x1, y0y1, 0x21);
                    let x0y1 = _mm256_blend_epi32(x0x1, y0y1, 0xf0);
                    _mm256_add_epi16(x1y0, x0y1)
                }
            }

            let mut accu_0 = _mm256_setzero_si256();
            let mut accu_1 = _mm256_setzero_si256();
            let mut accu_2 = _mm256_setzero_si256();
            let mut accu_3 = _mm256_setzero_si256();

            let mut i = 0_usize;
            while i + 2 <= width as usize {
                let c = _mm256_loadu_si256(codes.as_ptr().add(i * 16).cast());

                let mask = _mm256_set1_epi8(0xf);
                let clo = _mm256_and_si256(c, mask);
                let chi = _mm256_and_si256(_mm256_srli_epi16(c, 4), mask);

                let lut = _mm256_loadu_si256(lut.as_ptr().add(i * 16).cast());
                let res_lo = _mm256_shuffle_epi8(lut, clo);
                accu_0 = _mm256_add_epi16(accu_0, res_lo);
                accu_1 = _mm256_add_epi16(accu_1, _mm256_srli_epi16(res_lo, 8));
                let res_hi = _mm256_shuffle_epi8(lut, chi);
                accu_2 = _mm256_add_epi16(accu_2, res_hi);
                accu_3 = _mm256_add_epi16(accu_3, _mm256_srli_epi16(res_hi, 8));

                i += 2;
            }
            if i < width as usize {
                let c = _mm_loadu_si128(codes.as_ptr().add(i * 16).cast());

                let mask = _mm_set1_epi8(0xf);
                let clo = _mm_and_si128(c, mask);
                let chi = _mm_and_si128(_mm_srli_epi16(c, 4), mask);

                let lut = _mm_loadu_si128(lut.as_ptr().add(i * 16).cast());
                let res_lo = _mm256_zextsi128_si256(_mm_shuffle_epi8(lut, clo));
                accu_0 = _mm256_add_epi16(accu_0, res_lo);
                accu_1 = _mm256_add_epi16(accu_1, _mm256_srli_epi16(res_lo, 8));
                let res_hi = _mm256_zextsi128_si256(_mm_shuffle_epi8(lut, chi));
                accu_2 = _mm256_add_epi16(accu_2, res_hi);
                accu_3 = _mm256_add_epi16(accu_3, _mm256_srli_epi16(res_hi, 8));

                i += 1;
            }
            debug_assert_eq!(i, width as usize);

            let mut result = [0_u16; 32];

            accu_0 = _mm256_sub_epi16(accu_0, _mm256_slli_epi16(accu_1, 8));
            _mm256_storeu_si256(
                result.as_mut_ptr().add(0).cast(),
                combine2x2(accu_0, accu_1),
            );

            accu_2 = _mm256_sub_epi16(accu_2, _mm256_slli_epi16(accu_3, 8));
            _mm256_storeu_si256(
                result.as_mut_ptr().add(16).cast(),
                combine2x2(accu_2, accu_3),
            );

            result
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn fast_scan_b4_v3_test() {
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        for _ in 0..200 {
            for width in 90..110 {
                let codes = (0..16 * width).map(|_| rand::random()).collect::<Vec<u8>>();
                let lut = (0..16 * width).map(|_| rand::random()).collect::<Vec<u8>>();
                unsafe {
                    assert_eq!(
                        fast_scan_b4_v3(width, &codes, &lut),
                        fast_scan_b4_fallback(width, &codes, &lut)
                    );
                }
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v2")]
    unsafe fn fast_scan_b4_v2(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 32] {
        // bounds checking is not enforced by compiler, so check it manually
        assert_eq!(codes.len(), width as usize * 16);
        assert_eq!(lut.len(), width as usize * 16);

        unsafe {
            use std::arch::x86_64::*;

            let mut accu_0 = _mm_setzero_si128();
            let mut accu_1 = _mm_setzero_si128();
            let mut accu_2 = _mm_setzero_si128();
            let mut accu_3 = _mm_setzero_si128();

            let mut i = 0_usize;
            while i < width as usize {
                let c = _mm_loadu_si128(codes.as_ptr().add(i * 16).cast());

                let mask = _mm_set1_epi8(0xf);
                let clo = _mm_and_si128(c, mask);
                let chi = _mm_and_si128(_mm_srli_epi16(c, 4), mask);

                let lut = _mm_loadu_si128(lut.as_ptr().add(i * 16).cast());
                let res_lo = _mm_shuffle_epi8(lut, clo);
                accu_0 = _mm_add_epi16(accu_0, res_lo);
                accu_1 = _mm_add_epi16(accu_1, _mm_srli_epi16(res_lo, 8));
                let res_hi = _mm_shuffle_epi8(lut, chi);
                accu_2 = _mm_add_epi16(accu_2, res_hi);
                accu_3 = _mm_add_epi16(accu_3, _mm_srli_epi16(res_hi, 8));

                i += 1;
            }
            debug_assert_eq!(i, width as usize);

            let mut result = [0_u16; 32];

            accu_0 = _mm_sub_epi16(accu_0, _mm_slli_epi16(accu_1, 8));
            _mm_storeu_si128(result.as_mut_ptr().add(0).cast(), accu_0);
            _mm_storeu_si128(result.as_mut_ptr().add(8).cast(), accu_1);

            accu_2 = _mm_sub_epi16(accu_2, _mm_slli_epi16(accu_3, 8));
            _mm_storeu_si128(result.as_mut_ptr().add(16).cast(), accu_2);
            _mm_storeu_si128(result.as_mut_ptr().add(24).cast(), accu_3);

            result
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn fast_scan_b4_v2_test() {
        detect::init();
        if !detect::v2::detect() {
            println!("test {} ... skipped (v2)", module_path!());
            return;
        }
        for _ in 0..200 {
            for width in 90..110 {
                let codes = (0..16 * width).map(|_| rand::random()).collect::<Vec<u8>>();
                let lut = (0..16 * width).map(|_| rand::random()).collect::<Vec<u8>>();
                unsafe {
                    assert_eq!(
                        fast_scan_b4_v2(width, &codes, &lut),
                        fast_scan_b4_fallback(width, &codes, &lut)
                    );
                }
            }
        }
    }

    #[detect::multiversion(v4 = import, v3 = import, v2 = import, neon, fallback = export)]
    pub fn fast_scan_b4(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 32] {
        let width = width as usize;

        assert_eq!(codes.len(), width * 16);
        assert_eq!(lut.len(), width * 16);

        use std::array::from_fn;
        use std::ops::BitAnd;

        fn load<T: Copy, const N: usize>(slice: &[T]) -> [T; N] {
            from_fn(|i| slice[i])
        }
        fn unary<T: Copy, U: Copy, const N: usize>(op: impl Fn(T) -> U, a: [T; N]) -> [U; N] {
            from_fn(|i| op(a[i]))
        }
        fn binary<T: Copy, const N: usize>(op: impl Fn(T, T) -> T, a: [T; N], b: [T; N]) -> [T; N] {
            from_fn(|i| op(a[i], b[i]))
        }
        fn shuffle<T: Copy, const N: usize>(a: [T; N], b: [u8; N]) -> [T; N] {
            from_fn(|i| a[b[i] as usize])
        }
        fn cast(x: [u8; 16]) -> [u16; 8] {
            from_fn(|i| u16::from_le_bytes([x[i << 1 | 0], x[i << 1 | 1]]))
        }
        fn setr<T: Copy>(x: [[T; 8]; 4]) -> [T; 32] {
            from_fn(|i| x[i >> 3][i & 7])
        }

        let mut a_0 = [0u16; 8];
        let mut a_1 = [0u16; 8];
        let mut a_2 = [0u16; 8];
        let mut a_3 = [0u16; 8];

        for i in 0..width {
            let c = load(&codes[16 * i..]);

            let mask = [0xfu8; 16];
            let clo = binary(u8::bitand, c, mask);
            let chi = binary(u8::bitand, unary(|x| x >> 4, c), mask);

            let lut = load(&lut[16 * i..]);
            let res_lo = cast(shuffle(lut, clo));
            a_0 = binary(u16::wrapping_add, a_0, res_lo);
            a_1 = binary(u16::wrapping_add, a_1, unary(|x| x >> 8, res_lo));
            let res_hi = cast(shuffle(lut, chi));
            a_2 = binary(u16::wrapping_add, a_2, res_hi);
            a_3 = binary(u16::wrapping_add, a_3, unary(|x| x >> 8, res_hi));
        }

        a_0 = binary(u16::wrapping_sub, a_0, unary(|x| x.wrapping_shl(8), a_1));
        a_2 = binary(u16::wrapping_sub, a_2, unary(|x| x.wrapping_shl(8), a_3));

        setr([a_0, a_1, a_2, a_3])
    }
}

#[inline(always)]
pub fn fast_scan_b4(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 32] {
    fast_scan_b4::fast_scan_b4(width, codes, lut)
}
