/*

## codes layout for 4-bit quantizer

group i = | vector i | (total bytes = width/2)

byte:      | 0      | 1      | 2       | ... | width/4-1    |
bits 0..1: | code 0 | code 4 | code 8  | ... | code width-4 |
bits 2..3: | code 1 | code 5 | code 9  | ... | code width-3 |
bits 4..5: | code 2 | code 6 | code 10 | ... | code width-2 |
bits 6..7: | code 3 | code 7 | code 11 | ... | code width-1 |

## packed_codes layout for 4-bit quantizer

group i = | vector 64i | vector 64i+1 | vector 64i+2 | ... | vector 64i+63 | (total bytes = width * 16)

*/

pub const BLOCK_SIZE: u32 = 64;

pub fn pack(width: u32, r: [Vec<u8>; 64]) -> impl Iterator<Item = u8> {
    (0..width as usize).flat_map(move |i| {
        [
            r[0][i] | (r[16][i] << 2) | (r[32][i] << 4) | (r[48][i] << 6),
            r[8][i] | (r[24][i] << 2) | (r[40][i] << 4) | (r[56][i] << 6),
            r[1][i] | (r[17][i] << 2) | (r[33][i] << 4) | (r[49][i] << 6),
            r[9][i] | (r[25][i] << 2) | (r[41][i] << 4) | (r[57][i] << 6),
            r[2][i] | (r[18][i] << 2) | (r[34][i] << 4) | (r[50][i] << 6),
            r[10][i] | (r[26][i] << 2) | (r[42][i] << 4) | (r[58][i] << 6),
            r[3][i] | (r[19][i] << 2) | (r[35][i] << 4) | (r[51][i] << 6),
            r[11][i] | (r[27][i] << 2) | (r[43][i] << 4) | (r[59][i] << 6),
            r[4][i] | (r[20][i] << 2) | (r[36][i] << 4) | (r[52][i] << 6),
            r[12][i] | (r[28][i] << 2) | (r[44][i] << 4) | (r[60][i] << 6),
            r[5][i] | (r[21][i] << 2) | (r[37][i] << 4) | (r[53][i] << 6),
            r[13][i] | (r[29][i] << 2) | (r[45][i] << 4) | (r[61][i] << 6),
            r[6][i] | (r[22][i] << 2) | (r[38][i] << 4) | (r[54][i] << 6),
            r[14][i] | (r[30][i] << 2) | (r[46][i] << 4) | (r[62][i] << 6),
            r[7][i] | (r[23][i] << 2) | (r[39][i] << 4) | (r[55][i] << 6),
            r[15][i] | (r[31][i] << 2) | (r[47][i] << 4) | (r[63][i] << 6),
        ]
        .into_iter()
    })
}

pub fn is_supported() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if detect::v4::detect() {
            return true;
        }
        if detect::v3::detect() {
            return true;
        }
        if detect::v2::detect() {
            return true;
        }
    }
    false
}

pub fn fast_scan(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 64] {
    #[cfg(target_arch = "x86_64")]
    {
        if detect::v4::detect() {
            return unsafe { fast_scan_v4(width, codes, lut) };
        }
        if detect::v3::detect() {
            return unsafe { fast_scan_v3(width, codes, lut) };
        }
        if detect::v2::detect() {
            return unsafe { fast_scan_v2(width, codes, lut) };
        }
    }
    let _ = (width, codes, lut);
    unimplemented!()
}

#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v4")]
unsafe fn fast_scan_v4(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 64] {
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
        let mut accu_4 = _mm512_setzero_si512();
        let mut accu_5 = _mm512_setzero_si512();
        let mut accu_6 = _mm512_setzero_si512();
        let mut accu_7 = _mm512_setzero_si512();

        let mut i = 0_usize;
        while i + 4 <= width as usize {
            let c = _mm512_loadu_si512(codes.as_ptr().add(i * 16).cast());

            let mask = _mm512_set1_epi8(0x3);
            let c_a = _mm512_and_si512(c, mask);
            let c_b = _mm512_and_si512(_mm512_srli_epi16(c, 2), mask);
            let c_c = _mm512_and_si512(_mm512_srli_epi16(c, 4), mask);
            let c_d = _mm512_srli_epi16(c, 6);

            let lut = _mm512_loadu_si512(lut.as_ptr().add(i * 16).cast());
            let res_a = _mm512_shuffle_epi8(lut, c_a);
            accu_0 = _mm512_add_epi16(accu_0, res_a);
            accu_1 = _mm512_add_epi16(accu_1, _mm512_srli_epi16(res_a, 8));
            let res_b = _mm512_shuffle_epi8(lut, c_b);
            accu_2 = _mm512_add_epi16(accu_2, res_b);
            accu_3 = _mm512_add_epi16(accu_3, _mm512_srli_epi16(res_b, 8));
            let res_c = _mm512_shuffle_epi8(lut, c_c);
            accu_4 = _mm512_add_epi16(accu_4, res_c);
            accu_5 = _mm512_add_epi16(accu_5, _mm512_srli_epi16(res_c, 8));
            let res_d = _mm512_shuffle_epi8(lut, c_d);
            accu_6 = _mm512_add_epi16(accu_6, res_d);
            accu_7 = _mm512_add_epi16(accu_7, _mm512_srli_epi16(res_d, 8));

            i += 4;
        }
        if i + 2 <= width as usize {
            let c = _mm256_loadu_si256(codes.as_ptr().add(i * 16).cast());

            let mask = _mm256_set1_epi8(0x3);
            let c_a = _mm256_and_si256(c, mask);
            let c_b = _mm256_and_si256(_mm256_srli_epi16(c, 2), mask);
            let c_c = _mm256_and_si256(_mm256_srli_epi16(c, 4), mask);
            let c_d = _mm256_srli_epi16(c, 6);

            let lut = _mm256_loadu_si256(lut.as_ptr().add(i * 16).cast());
            let res_a = _mm512_zextsi256_si512(_mm256_shuffle_epi8(lut, c_a));
            accu_0 = _mm512_add_epi16(accu_0, res_a);
            accu_1 = _mm512_add_epi16(accu_1, _mm512_srli_epi16(res_a, 8));
            let res_b = _mm512_zextsi256_si512(_mm256_shuffle_epi8(lut, c_b));
            accu_2 = _mm512_add_epi16(accu_2, res_b);
            accu_3 = _mm512_add_epi16(accu_3, _mm512_srli_epi16(res_b, 8));
            let res_c = _mm512_zextsi256_si512(_mm256_shuffle_epi8(lut, c_c));
            accu_4 = _mm512_add_epi16(accu_4, res_c);
            accu_5 = _mm512_add_epi16(accu_5, _mm512_srli_epi16(res_c, 8));
            let res_d = _mm512_zextsi256_si512(_mm256_shuffle_epi8(lut, c_d));
            accu_6 = _mm512_add_epi16(accu_6, res_d);
            accu_7 = _mm512_add_epi16(accu_7, _mm512_srli_epi16(res_d, 8));

            i += 2;
        }
        if i < width as usize {
            let c = _mm_loadu_si128(codes.as_ptr().add(i * 16).cast());

            let mask = _mm_set1_epi8(0x3);
            let c_a = _mm_and_si128(c, mask);
            let c_b = _mm_and_si128(_mm_srli_epi16(c, 2), mask);
            let c_c = _mm_and_si128(_mm_srli_epi16(c, 4), mask);
            let c_d = _mm_srli_epi16(c, 6);

            let lut = _mm_loadu_si128(lut.as_ptr().add(i * 16).cast());
            let res_a = _mm512_zextsi128_si512(_mm_shuffle_epi8(lut, c_a));
            accu_0 = _mm512_add_epi16(accu_0, res_a);
            accu_1 = _mm512_add_epi16(accu_1, _mm512_srli_epi16(res_a, 8));
            let res_b = _mm512_zextsi128_si512(_mm_shuffle_epi8(lut, c_b));
            accu_2 = _mm512_add_epi16(accu_2, res_b);
            accu_3 = _mm512_add_epi16(accu_3, _mm512_srli_epi16(res_b, 8));
            let res_c = _mm512_zextsi128_si512(_mm_shuffle_epi8(lut, c_c));
            accu_4 = _mm512_add_epi16(accu_4, res_c);
            accu_5 = _mm512_add_epi16(accu_5, _mm512_srli_epi16(res_c, 8));
            let res_d = _mm512_zextsi128_si512(_mm_shuffle_epi8(lut, c_d));
            accu_6 = _mm512_add_epi16(accu_6, res_d);
            accu_7 = _mm512_add_epi16(accu_7, _mm512_srli_epi16(res_d, 8));

            i += 1;
        }
        debug_assert_eq!(i, width as usize);

        let mut result = [0_u16; 64];

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

        accu_4 = _mm512_sub_epi16(accu_4, _mm512_slli_epi16(accu_5, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(32).cast(),
            combine4x2(accu_4, accu_5),
        );

        accu_6 = _mm512_sub_epi16(accu_6, _mm512_slli_epi16(accu_7, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(48).cast(),
            combine4x2(accu_6, accu_7),
        );

        result
    }
}

#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v3")]
unsafe fn fast_scan_v3(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 64] {
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
        let mut accu_4 = _mm256_setzero_si256();
        let mut accu_5 = _mm256_setzero_si256();
        let mut accu_6 = _mm256_setzero_si256();
        let mut accu_7 = _mm256_setzero_si256();

        let mut i = 0_usize;
        while i + 2 <= width as usize {
            let c = _mm256_loadu_si256(codes.as_ptr().add(i * 16).cast());

            let mask = _mm256_set1_epi8(0x3);
            let c_a = _mm256_and_si256(c, mask);
            let c_b = _mm256_and_si256(_mm256_srli_epi16(c, 2), mask);
            let c_c = _mm256_and_si256(_mm256_srli_epi16(c, 4), mask);
            let c_d = _mm256_srli_epi16(c, 6);

            let lut = _mm256_loadu_si256(lut.as_ptr().add(i * 16).cast());
            let res_a = _mm256_shuffle_epi8(lut, c_a);
            accu_0 = _mm256_add_epi16(accu_0, res_a);
            accu_1 = _mm256_add_epi16(accu_1, _mm256_srli_epi16(res_a, 8));
            let res_b = _mm256_shuffle_epi8(lut, c_b);
            accu_2 = _mm256_add_epi16(accu_2, res_b);
            accu_3 = _mm256_add_epi16(accu_3, _mm256_srli_epi16(res_b, 8));
            let res_c = _mm256_shuffle_epi8(lut, c_c);
            accu_4 = _mm256_add_epi16(accu_4, res_c);
            accu_5 = _mm256_add_epi16(accu_5, _mm256_srli_epi16(res_c, 8));
            let res_d = _mm256_shuffle_epi8(lut, c_d);
            accu_6 = _mm256_add_epi16(accu_6, res_d);
            accu_7 = _mm256_add_epi16(accu_7, _mm256_srli_epi16(res_d, 8));

            i += 2;
        }
        if i < width as usize {
            let c = _mm_loadu_si128(codes.as_ptr().add(i * 16).cast());

            let mask = _mm_set1_epi8(0x3);
            let c_a = _mm_and_si128(c, mask);
            let c_b = _mm_and_si128(_mm_srli_epi16(c, 2), mask);
            let c_c = _mm_and_si128(_mm_srli_epi16(c, 4), mask);
            let c_d = _mm_srli_epi16(c, 6);

            let lut = _mm_loadu_si128(lut.as_ptr().add(i * 16).cast());
            let res_a = _mm256_zextsi128_si256(_mm_shuffle_epi8(lut, c_a));
            accu_0 = _mm256_add_epi16(accu_0, res_a);
            accu_1 = _mm256_add_epi16(accu_1, _mm256_srli_epi16(res_a, 8));
            let res_b = _mm256_zextsi128_si256(_mm_shuffle_epi8(lut, c_b));
            accu_2 = _mm256_add_epi16(accu_2, res_b);
            accu_3 = _mm256_add_epi16(accu_3, _mm256_srli_epi16(res_b, 8));
            let res_c = _mm256_zextsi128_si256(_mm_shuffle_epi8(lut, c_c));
            accu_4 = _mm256_add_epi16(accu_4, res_c);
            accu_5 = _mm256_add_epi16(accu_5, _mm256_srli_epi16(res_c, 8));
            let res_d = _mm256_zextsi128_si256(_mm_shuffle_epi8(lut, c_d));
            accu_6 = _mm256_add_epi16(accu_6, res_d);
            accu_7 = _mm256_add_epi16(accu_7, _mm256_srli_epi16(res_d, 8));

            i += 1;
        }
        debug_assert_eq!(i, width as usize);

        let mut result = [0_u16; 64];

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

        accu_4 = _mm256_sub_epi16(accu_4, _mm256_slli_epi16(accu_5, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(32).cast(),
            combine2x2(accu_4, accu_5),
        );

        accu_6 = _mm256_sub_epi16(accu_6, _mm256_slli_epi16(accu_7, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(48).cast(),
            combine2x2(accu_6, accu_7),
        );

        result
    }
}

#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v2")]
unsafe fn fast_scan_v2(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 64] {
    // bounds checking is not enforced by compiler, so check it manually
    assert_eq!(codes.len(), width as usize * 16);
    assert_eq!(lut.len(), width as usize * 16);

    unsafe {
        use std::arch::x86_64::*;

        let mut accu_0 = _mm_setzero_si128();
        let mut accu_1 = _mm_setzero_si128();
        let mut accu_2 = _mm_setzero_si128();
        let mut accu_3 = _mm_setzero_si128();
        let mut accu_4 = _mm_setzero_si128();
        let mut accu_5 = _mm_setzero_si128();
        let mut accu_6 = _mm_setzero_si128();
        let mut accu_7 = _mm_setzero_si128();

        let mut i = 0_usize;
        while i < width as usize {
            let c = _mm_loadu_si128(codes.as_ptr().add(i * 16).cast());

            let mask = _mm_set1_epi8(0x3);
            let c_a = _mm_and_si128(c, mask);
            let c_b = _mm_and_si128(_mm_srli_epi16(c, 2), mask);
            let c_c = _mm_and_si128(_mm_srli_epi16(c, 4), mask);
            let c_d = _mm_srli_epi16(c, 6);

            let lut = _mm_loadu_si128(lut.as_ptr().add(i * 16).cast());
            let res_a = _mm_shuffle_epi8(lut, c_a);
            accu_0 = _mm_add_epi16(accu_0, res_a);
            accu_1 = _mm_add_epi16(accu_1, _mm_srli_epi16(res_a, 8));
            let res_b = _mm_shuffle_epi8(lut, c_b);
            accu_2 = _mm_add_epi16(accu_2, res_b);
            accu_3 = _mm_add_epi16(accu_3, _mm_srli_epi16(res_b, 8));
            let res_c = _mm_shuffle_epi8(lut, c_c);
            accu_4 = _mm_add_epi16(accu_4, res_c);
            accu_5 = _mm_add_epi16(accu_5, _mm_srli_epi16(res_c, 8));
            let res_d = _mm_shuffle_epi8(lut, c_d);
            accu_6 = _mm_add_epi16(accu_6, res_d);
            accu_7 = _mm_add_epi16(accu_7, _mm_srli_epi16(res_d, 8));

            i += 1;
        }
        debug_assert_eq!(i, width as usize);

        let mut result = [0_u16; 64];

        accu_0 = _mm_sub_epi16(accu_0, _mm_slli_epi16(accu_1, 8));
        _mm_storeu_si128(result.as_mut_ptr().add(0).cast(), accu_0);
        _mm_storeu_si128(result.as_mut_ptr().add(8).cast(), accu_1);

        accu_2 = _mm_sub_epi16(accu_2, _mm_slli_epi16(accu_3, 8));
        _mm_storeu_si128(result.as_mut_ptr().add(16).cast(), accu_2);
        _mm_storeu_si128(result.as_mut_ptr().add(24).cast(), accu_3);

        accu_4 = _mm_sub_epi16(accu_4, _mm_slli_epi16(accu_5, 8));
        _mm_storeu_si128(result.as_mut_ptr().add(32).cast(), accu_4);
        _mm_storeu_si128(result.as_mut_ptr().add(40).cast(), accu_5);

        accu_6 = _mm_sub_epi16(accu_6, _mm_slli_epi16(accu_7, 8));
        _mm_storeu_si128(result.as_mut_ptr().add(48).cast(), accu_6);
        _mm_storeu_si128(result.as_mut_ptr().add(56).cast(), accu_7);

        result
    }
}

#[cfg(target_arch = "x86_64")]
#[test]
fn test_v4_v3() {
    detect::init();
    if !detect::v4::detect() || !detect::v3::detect() {
        println!("test {} ... skipped (v4, v3)", module_path!());
        return;
    }
    for _ in 0..200 {
        for width in 90..110 {
            let codes = (0..16 * width).map(|_| rand::random()).collect::<Vec<u8>>();
            let lut = (0..16 * width).map(|_| rand::random()).collect::<Vec<u8>>();
            unsafe {
                assert_eq!(
                    fast_scan_v4(width, &codes, &lut),
                    fast_scan_v3(width, &codes, &lut)
                );
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[test]
fn test_v3_v2() {
    detect::init();
    if !detect::v3::detect() || !detect::v2::detect() {
        println!("test {} ... skipped (v3, v2)", module_path!());
        return;
    }
    for _ in 0..200 {
        for width in 90..110 {
            let codes = (0..16 * width).map(|_| rand::random()).collect::<Vec<u8>>();
            let lut = (0..16 * width).map(|_| rand::random()).collect::<Vec<u8>>();
            unsafe {
                assert_eq!(
                    fast_scan_v3(width, &codes, &lut),
                    fast_scan_v2(width, &codes, &lut)
                );
            }
        }
    }
}
