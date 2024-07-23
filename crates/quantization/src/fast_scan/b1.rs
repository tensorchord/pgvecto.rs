/*

## codes layout for 4-bit quantizer

group i = | vector i | (total bytes = width/2)

byte:   | 0      | 1       | 2       | ... | width/8-1    |
bits 0: | code 0 | code 8  | code 16 | ... | code width-8 |
bits 1: | code 1 | code 9  | code 17 | ... | code width-7 |
bits 2: | code 2 | code 10 | code 18 | ... | code width-6 |
bits 3: | code 3 | code 11 | code 19 | ... | code width-5 |
bits 4: | code 4 | code 12 | code 20 | ... | code width-4 |
bits 5: | code 5 | code 13 | code 21 | ... | code width-3 |
bits 6: | code 6 | code 14 | code 22 | ... | code width-2 |
bits 7: | code 7 | code 15 | code 23 | ... | code width-1 |

## packed_codes layout for 4-bit quantizer

group i = | vector 128i | vector 128i+1 | vector 128i+2 | ... | vector 128i+127 | (total bytes = width * 16)

*/

pub const BLOCK_SIZE: u32 = 128;

pub fn pack(width: u32, r: [Vec<u8>; 128]) -> impl Iterator<Item = u8> {
    let make = move |s: usize, i: usize| {
        r[0][i]
            | (r[s + 16][i] << 1)
            | (r[s + 32][i] << 2)
            | (r[s + 48][i] << 3)
            | (r[s + 64][i] << 4)
            | (r[s + 80][i] << 5)
            | (r[s + 96][i] << 6)
            | (r[s + 112][i] << 7)
    };
    (0..width as usize).flat_map(move |i| {
        [
            make(0, i),
            make(8, i),
            make(1, i),
            make(9, i),
            make(2, i),
            make(10, i),
            make(3, i),
            make(11, i),
            make(4, i),
            make(12, i),
            make(5, i),
            make(13, i),
            make(6, i),
            make(14, i),
            make(7, i),
            make(15, i),
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

pub fn fast_scan(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 128] {
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
unsafe fn fast_scan_v4(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 128] {
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
        let mut accu_8 = _mm512_setzero_si512();
        let mut accu_9 = _mm512_setzero_si512();
        let mut accu_10 = _mm512_setzero_si512();
        let mut accu_11 = _mm512_setzero_si512();
        let mut accu_12 = _mm512_setzero_si512();
        let mut accu_13 = _mm512_setzero_si512();
        let mut accu_14 = _mm512_setzero_si512();
        let mut accu_15 = _mm512_setzero_si512();

        let mut i = 0_usize;
        while i + 4 <= width as usize {
            let c = _mm512_loadu_si512(codes.as_ptr().add(i * 16).cast());

            let mask = _mm512_set1_epi8(0x1);
            let c_a = _mm512_and_si512(c, mask);
            let c_b = _mm512_and_si512(_mm512_srli_epi16(c, 1), mask);
            let c_c = _mm512_and_si512(_mm512_srli_epi16(c, 2), mask);
            let c_d = _mm512_and_si512(_mm512_srli_epi16(c, 3), mask);
            let c_e = _mm512_and_si512(_mm512_srli_epi16(c, 4), mask);
            let c_f = _mm512_and_si512(_mm512_srli_epi16(c, 5), mask);
            let c_g = _mm512_and_si512(_mm512_srli_epi16(c, 6), mask);
            let c_h = _mm512_srli_epi16(c, 7);

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
            let res_e = _mm512_shuffle_epi8(lut, c_e);
            accu_8 = _mm512_add_epi16(accu_8, res_e);
            accu_9 = _mm512_add_epi16(accu_9, _mm512_srli_epi16(res_e, 8));
            let res_f = _mm512_shuffle_epi8(lut, c_f);
            accu_10 = _mm512_add_epi16(accu_10, res_f);
            accu_11 = _mm512_add_epi16(accu_11, _mm512_srli_epi16(res_f, 8));
            let res_g = _mm512_shuffle_epi8(lut, c_g);
            accu_12 = _mm512_add_epi16(accu_12, res_g);
            accu_13 = _mm512_add_epi16(accu_13, _mm512_srli_epi16(res_g, 8));
            let res_h = _mm512_shuffle_epi8(lut, c_h);
            accu_14 = _mm512_add_epi16(accu_14, res_h);
            accu_15 = _mm512_add_epi16(accu_15, _mm512_srli_epi16(res_h, 8));

            i += 4;
        }
        if i + 2 <= width as usize {
            let c = _mm256_loadu_si256(codes.as_ptr().add(i * 16).cast());

            let mask = _mm256_set1_epi8(0x1);
            let c_a = _mm256_and_si256(c, mask);
            let c_b = _mm256_and_si256(_mm256_srli_epi16(c, 1), mask);
            let c_c = _mm256_and_si256(_mm256_srli_epi16(c, 2), mask);
            let c_d = _mm256_and_si256(_mm256_srli_epi16(c, 3), mask);
            let c_e = _mm256_and_si256(_mm256_srli_epi16(c, 4), mask);
            let c_f = _mm256_and_si256(_mm256_srli_epi16(c, 5), mask);
            let c_g = _mm256_and_si256(_mm256_srli_epi16(c, 6), mask);
            let c_h = _mm256_srli_epi16(c, 7);

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
            let res_e = _mm512_zextsi256_si512(_mm256_shuffle_epi8(lut, c_e));
            accu_8 = _mm512_add_epi16(accu_8, res_e);
            accu_9 = _mm512_add_epi16(accu_9, _mm512_srli_epi16(res_e, 8));
            let res_f = _mm512_zextsi256_si512(_mm256_shuffle_epi8(lut, c_f));
            accu_10 = _mm512_add_epi16(accu_10, res_f);
            accu_11 = _mm512_add_epi16(accu_11, _mm512_srli_epi16(res_f, 8));
            let res_g = _mm512_zextsi256_si512(_mm256_shuffle_epi8(lut, c_g));
            accu_12 = _mm512_add_epi16(accu_12, res_g);
            accu_13 = _mm512_add_epi16(accu_13, _mm512_srli_epi16(res_g, 8));
            let res_h = _mm512_zextsi256_si512(_mm256_shuffle_epi8(lut, c_h));
            accu_14 = _mm512_add_epi16(accu_14, res_h);
            accu_15 = _mm512_add_epi16(accu_15, _mm512_srli_epi16(res_h, 8));

            i += 2;
        }
        if i < width as usize {
            let c = _mm_loadu_si128(codes.as_ptr().add(i * 16).cast());

            let mask = _mm_set1_epi8(0x1);
            let c_a = _mm_and_si128(c, mask);
            let c_b = _mm_and_si128(_mm_srli_epi16(c, 1), mask);
            let c_c = _mm_and_si128(_mm_srli_epi16(c, 2), mask);
            let c_d = _mm_and_si128(_mm_srli_epi16(c, 3), mask);
            let c_e = _mm_and_si128(_mm_srli_epi16(c, 4), mask);
            let c_f = _mm_and_si128(_mm_srli_epi16(c, 5), mask);
            let c_g = _mm_and_si128(_mm_srli_epi16(c, 6), mask);
            let c_h = _mm_srli_epi16(c, 7);

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
            let res_e = _mm512_zextsi128_si512(_mm_shuffle_epi8(lut, c_e));
            accu_8 = _mm512_add_epi16(accu_8, res_e);
            accu_9 = _mm512_add_epi16(accu_9, _mm512_srli_epi16(res_e, 8));
            let res_f = _mm512_zextsi128_si512(_mm_shuffle_epi8(lut, c_f));
            accu_10 = _mm512_add_epi16(accu_10, res_f);
            accu_11 = _mm512_add_epi16(accu_11, _mm512_srli_epi16(res_f, 8));
            let res_g = _mm512_zextsi128_si512(_mm_shuffle_epi8(lut, c_g));
            accu_12 = _mm512_add_epi16(accu_12, res_g);
            accu_13 = _mm512_add_epi16(accu_13, _mm512_srli_epi16(res_g, 8));
            let res_h = _mm512_zextsi128_si512(_mm_shuffle_epi8(lut, c_h));
            accu_14 = _mm512_add_epi16(accu_14, res_h);
            accu_15 = _mm512_add_epi16(accu_15, _mm512_srli_epi16(res_h, 8));

            i += 1;
        }
        debug_assert_eq!(i, width as usize);

        let mut result = [0_u16; 128];

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

        accu_8 = _mm512_sub_epi16(accu_8, _mm512_slli_epi16(accu_9, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(64).cast(),
            combine4x2(accu_8, accu_9),
        );

        accu_10 = _mm512_sub_epi16(accu_10, _mm512_slli_epi16(accu_11, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(80).cast(),
            combine4x2(accu_10, accu_11),
        );

        accu_12 = _mm512_sub_epi16(accu_12, _mm512_slli_epi16(accu_13, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(96).cast(),
            combine4x2(accu_12, accu_13),
        );

        accu_14 = _mm512_sub_epi16(accu_14, _mm512_slli_epi16(accu_15, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(112).cast(),
            combine4x2(accu_14, accu_15),
        );

        result
    }
}

#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v3")]
unsafe fn fast_scan_v3(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 128] {
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
        let mut accu_8 = _mm256_setzero_si256();
        let mut accu_9 = _mm256_setzero_si256();
        let mut accu_10 = _mm256_setzero_si256();
        let mut accu_11 = _mm256_setzero_si256();
        let mut accu_12 = _mm256_setzero_si256();
        let mut accu_13 = _mm256_setzero_si256();
        let mut accu_14 = _mm256_setzero_si256();
        let mut accu_15 = _mm256_setzero_si256();

        let mut i = 0_usize;
        while i + 2 <= width as usize {
            let c = _mm256_loadu_si256(codes.as_ptr().add(i * 16).cast());

            let mask = _mm256_set1_epi8(0x1);
            let c_a = _mm256_and_si256(c, mask);
            let c_b = _mm256_and_si256(_mm256_srli_epi16(c, 1), mask);
            let c_c = _mm256_and_si256(_mm256_srli_epi16(c, 2), mask);
            let c_d = _mm256_and_si256(_mm256_srli_epi16(c, 3), mask);
            let c_e = _mm256_and_si256(_mm256_srli_epi16(c, 4), mask);
            let c_f = _mm256_and_si256(_mm256_srli_epi16(c, 5), mask);
            let c_g = _mm256_and_si256(_mm256_srli_epi16(c, 6), mask);
            let c_h = _mm256_srli_epi16(c, 7);

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
            let res_e = _mm256_shuffle_epi8(lut, c_e);
            accu_8 = _mm256_add_epi16(accu_8, res_e);
            accu_9 = _mm256_add_epi16(accu_9, _mm256_srli_epi16(res_e, 8));
            let res_f = _mm256_shuffle_epi8(lut, c_f);
            accu_10 = _mm256_add_epi16(accu_10, res_f);
            accu_11 = _mm256_add_epi16(accu_11, _mm256_srli_epi16(res_f, 8));
            let res_g = _mm256_shuffle_epi8(lut, c_g);
            accu_12 = _mm256_add_epi16(accu_12, res_g);
            accu_13 = _mm256_add_epi16(accu_13, _mm256_srli_epi16(res_g, 8));
            let res_h = _mm256_shuffle_epi8(lut, c_h);
            accu_14 = _mm256_add_epi16(accu_14, res_h);
            accu_15 = _mm256_add_epi16(accu_15, _mm256_srli_epi16(res_h, 8));

            i += 2;
        }
        if i < width as usize {
            let c = _mm_loadu_si128(codes.as_ptr().add(i * 16).cast());

            let mask = _mm_set1_epi8(0x1);
            let c_a = _mm_and_si128(c, mask);
            let c_b = _mm_and_si128(_mm_srli_epi16(c, 1), mask);
            let c_c = _mm_and_si128(_mm_srli_epi16(c, 2), mask);
            let c_d = _mm_and_si128(_mm_srli_epi16(c, 3), mask);
            let c_e = _mm_and_si128(_mm_srli_epi16(c, 4), mask);
            let c_f = _mm_and_si128(_mm_srli_epi16(c, 5), mask);
            let c_g = _mm_and_si128(_mm_srli_epi16(c, 6), mask);
            let c_h = _mm_srli_epi16(c, 7);

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
            let res_e = _mm256_zextsi128_si256(_mm_shuffle_epi8(lut, c_e));
            accu_8 = _mm256_add_epi16(accu_8, res_e);
            accu_9 = _mm256_add_epi16(accu_9, _mm256_srli_epi16(res_e, 8));
            let res_f = _mm256_zextsi128_si256(_mm_shuffle_epi8(lut, c_f));
            accu_10 = _mm256_add_epi16(accu_10, res_f);
            accu_11 = _mm256_add_epi16(accu_11, _mm256_srli_epi16(res_f, 8));
            let res_g = _mm256_zextsi128_si256(_mm_shuffle_epi8(lut, c_g));
            accu_12 = _mm256_add_epi16(accu_12, res_g);
            accu_13 = _mm256_add_epi16(accu_13, _mm256_srli_epi16(res_g, 8));
            let res_h = _mm256_zextsi128_si256(_mm_shuffle_epi8(lut, c_h));
            accu_14 = _mm256_add_epi16(accu_14, res_h);
            accu_15 = _mm256_add_epi16(accu_15, _mm256_srli_epi16(res_h, 8));

            i += 1;
        }
        debug_assert_eq!(i, width as usize);

        let mut result = [0_u16; 128];

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

        accu_8 = _mm256_sub_epi16(accu_8, _mm256_slli_epi16(accu_9, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(64).cast(),
            combine2x2(accu_8, accu_9),
        );

        accu_10 = _mm256_sub_epi16(accu_10, _mm256_slli_epi16(accu_11, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(80).cast(),
            combine2x2(accu_10, accu_11),
        );

        accu_12 = _mm256_sub_epi16(accu_12, _mm256_slli_epi16(accu_13, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(96).cast(),
            combine2x2(accu_12, accu_13),
        );

        accu_14 = _mm256_sub_epi16(accu_14, _mm256_slli_epi16(accu_15, 8));
        _mm256_storeu_si256(
            result.as_mut_ptr().add(112).cast(),
            combine2x2(accu_14, accu_15),
        );

        result
    }
}

#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v2")]
unsafe fn fast_scan_v2(width: u32, codes: &[u8], lut: &[u8]) -> [u16; 128] {
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
        let mut accu_8 = _mm_setzero_si128();
        let mut accu_9 = _mm_setzero_si128();
        let mut accu_10 = _mm_setzero_si128();
        let mut accu_11 = _mm_setzero_si128();
        let mut accu_12 = _mm_setzero_si128();
        let mut accu_13 = _mm_setzero_si128();
        let mut accu_14 = _mm_setzero_si128();
        let mut accu_15 = _mm_setzero_si128();

        let mut i = 0_usize;
        while i < width as usize {
            let c = _mm_loadu_si128(codes.as_ptr().add(i * 16).cast());

            let mask = _mm_set1_epi8(0x1);
            let c_a = _mm_and_si128(c, mask);
            let c_b = _mm_and_si128(_mm_srli_epi16(c, 1), mask);
            let c_c = _mm_and_si128(_mm_srli_epi16(c, 2), mask);
            let c_d = _mm_and_si128(_mm_srli_epi16(c, 3), mask);
            let c_e = _mm_and_si128(_mm_srli_epi16(c, 4), mask);
            let c_f = _mm_and_si128(_mm_srli_epi16(c, 5), mask);
            let c_g = _mm_and_si128(_mm_srli_epi16(c, 6), mask);
            let c_h = _mm_srli_epi16(c, 7);

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
            let res_e = _mm_shuffle_epi8(lut, c_e);
            accu_8 = _mm_add_epi16(accu_8, res_e);
            accu_9 = _mm_add_epi16(accu_9, _mm_srli_epi16(res_e, 8));
            let res_f = _mm_shuffle_epi8(lut, c_f);
            accu_10 = _mm_add_epi16(accu_10, res_f);
            accu_11 = _mm_add_epi16(accu_11, _mm_srli_epi16(res_f, 8));
            let res_g = _mm_shuffle_epi8(lut, c_g);
            accu_12 = _mm_add_epi16(accu_12, res_g);
            accu_13 = _mm_add_epi16(accu_13, _mm_srli_epi16(res_g, 8));
            let res_h = _mm_shuffle_epi8(lut, c_h);
            accu_14 = _mm_add_epi16(accu_14, res_h);
            accu_15 = _mm_add_epi16(accu_15, _mm_srli_epi16(res_h, 8));

            i += 1;
        }
        debug_assert_eq!(i, width as usize);

        let mut result = [0_u16; 128];

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

        accu_8 = _mm_sub_epi16(accu_8, _mm_slli_epi16(accu_9, 8));
        _mm_storeu_si128(result.as_mut_ptr().add(64).cast(), accu_8);
        _mm_storeu_si128(result.as_mut_ptr().add(72).cast(), accu_9);

        accu_10 = _mm_sub_epi16(accu_10, _mm_slli_epi16(accu_11, 8));
        _mm_storeu_si128(result.as_mut_ptr().add(80).cast(), accu_10);
        _mm_storeu_si128(result.as_mut_ptr().add(88).cast(), accu_11);

        accu_12 = _mm_sub_epi16(accu_12, _mm_slli_epi16(accu_13, 8));
        _mm_storeu_si128(result.as_mut_ptr().add(96).cast(), accu_12);
        _mm_storeu_si128(result.as_mut_ptr().add(104).cast(), accu_13);

        accu_14 = _mm_sub_epi16(accu_14, _mm_slli_epi16(accu_15, 8));
        _mm_storeu_si128(result.as_mut_ptr().add(112).cast(), accu_14);
        _mm_storeu_si128(result.as_mut_ptr().add(120).cast(), accu_15);

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
