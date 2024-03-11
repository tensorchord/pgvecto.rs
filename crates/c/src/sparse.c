#include "sparse.h"

#if defined(__x86_64__)
#include <immintrin.h>

// wait for https://github.com/rust-lang/stdarch/pull/1000
__attribute__((target("arch=x86-64-v4,avx512vp2intersect"))) extern float
v_sparse_dot_avx512vp2intersect(uint32_t *lhs_idx, uint32_t *rhs_idx,
                                float *lhs_val, float *rhs_val, size_t lhs_len,
                                size_t rhs_len) {
  size_t lhs_pos = 0, rhs_pos = 0, lhs_loop_len = lhs_len / 16 * 16,
         rhs_loop_len = rhs_len / 16 * 16;
  __m512 xy = _mm512_setzero_ps();
  while (lhs_pos < lhs_loop_len && rhs_pos < rhs_loop_len) {
    __m512i i_l = _mm512_loadu_epi32(lhs_idx + lhs_pos);
    __m512i i_r = _mm512_loadu_epi32(rhs_idx + rhs_pos);
    __mmask16 m_l, m_r;
    _mm512_2intersect_epi32(i_l, i_r, &m_l, &m_r);
    __m512 v_l = _mm512_loadu_ps(lhs_val + lhs_pos);
    __m512 v_r = _mm512_loadu_ps(rhs_val + rhs_pos);
    __m512 cv_l = _mm512_maskz_compress_ps(m_l, v_l);
    __m512 cv_r = _mm512_maskz_compress_ps(m_r, v_r);
    xy = _mm512_fmadd_ps(cv_l, cv_r, xy);
    uint32_t l_max = lhs_idx[lhs_pos + 15], r_max = rhs_idx[rhs_pos + 15];
    if (l_max < r_max) {
      lhs_pos += 16;
    } else if (l_max > r_max) {
      rhs_pos += 16;
    } else {
      lhs_pos += 16;
      rhs_pos += 16;
    }
  }
  while (lhs_pos < lhs_len && rhs_pos < rhs_len) {
    size_t len_l = lhs_len - lhs_pos < 16 ? lhs_len - lhs_pos : 16;
    size_t len_r = rhs_len - rhs_pos < 16 ? rhs_len - rhs_pos : 16;
    __mmask16 mask_l = _bzhi_u32(0xFFFF, len_l);
    __mmask16 mask_r = _bzhi_u32(0xFFFF, len_r);
    __m512i i_l = _mm512_maskz_loadu_epi32(mask_l, lhs_idx + lhs_pos);
    __m512i i_r = _mm512_maskz_loadu_epi32(mask_r, rhs_idx + rhs_pos);
    __mmask16 m_l, m_r;
    _mm512_2intersect_epi32(i_l, i_r, &m_l, &m_r);
    __m512 v_l = _mm512_maskz_loadu_ps(mask_l, lhs_val + lhs_pos);
    __m512 v_r = _mm512_maskz_loadu_ps(mask_r, rhs_val + rhs_pos);
    __m512 cv_l = _mm512_maskz_compress_ps(m_l, v_l);
    __m512 cv_r = _mm512_maskz_compress_ps(m_r, v_r);
    xy = _mm512_fmadd_ps(cv_l, cv_r, xy);
    uint32_t l_max = lhs_idx[lhs_pos + len_l - 1],
             r_max = rhs_idx[rhs_pos + len_r - 1];
    if (l_max < r_max) {
      lhs_pos += 16;
    } else if (l_max > r_max) {
      rhs_pos += 16;
    } else {
      lhs_pos += 16;
      rhs_pos += 16;
    }
  }
  return _mm512_reduce_add_ps(xy);
}

#endif