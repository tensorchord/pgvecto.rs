#include "sparse.h"
#include <math.h>

#if defined(__x86_64__)
#include <immintrin.h>

// wait for https://github.com/rust-lang/stdarch/pull/1000
__attribute__((target("arch=x86-64-v4,avx512vp2intersect"))) extern float
v_sparse_cosine_avx512vp2intersect(uint32_t *lhs_idx, uint32_t *rhs_idx,
                                   float *lhs_val, float *rhs_val,
                                   size_t lhs_len, size_t rhs_len) {
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

  __m512 xx = _mm512_setzero_ps(), yy = _mm512_setzero_ps(), v;
  lhs_pos = rhs_pos = 0;
  while (lhs_pos < lhs_loop_len) {
    v = _mm512_loadu_ps(lhs_val + lhs_pos);
    xx = _mm512_fmadd_ps(v, v, xx);
    lhs_pos += 16;
  }
  v = _mm512_maskz_loadu_ps(_bzhi_u32(0xFFFF, lhs_len - lhs_pos),
                            lhs_val + lhs_pos);
  xx = _mm512_fmadd_ps(v, v, xx);
  while (rhs_pos < rhs_loop_len) {
    v = _mm512_loadu_ps(rhs_val + rhs_pos);
    yy = _mm512_fmadd_ps(v, v, yy);
    rhs_pos += 16;
  }
  v = _mm512_maskz_loadu_ps(_bzhi_u32(0xFFFF, rhs_len - rhs_pos),
                            rhs_val + rhs_pos);
  yy = _mm512_fmadd_ps(v, v, yy);

  float rxy = _mm512_reduce_add_ps(xy);
  float rxx = _mm512_reduce_add_ps(xx);
  float ryy = _mm512_reduce_add_ps(yy);
  return rxy / sqrt(rxx * ryy);
}

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

__attribute__((target("arch=x86-64-v4,avx512vp2intersect"))) extern float
v_sparse_sl2_avx512vp2intersect(uint32_t *lhs_idx, uint32_t *rhs_idx,
                                float *lhs_val, float *rhs_val, size_t lhs_len,
                                size_t rhs_len) {
  size_t lhs_pos = 0, rhs_pos = 0, lhs_loop_len = lhs_len / 16 * 16,
         rhs_loop_len = rhs_len / 16 * 16;
  __m512 dd = _mm512_setzero_ps();
  while (lhs_pos < lhs_loop_len && rhs_pos < rhs_loop_len) {
    __m512i i_l = _mm512_loadu_epi32(lhs_idx + lhs_pos);
    __m512i i_r = _mm512_loadu_epi32(rhs_idx + rhs_pos);
    __mmask16 m_l, m_r;
    _mm512_2intersect_epi32(i_l, i_r, &m_l, &m_r);
    __m512 v_l = _mm512_loadu_ps(lhs_val + lhs_pos);
    __m512 v_r = _mm512_loadu_ps(rhs_val + rhs_pos);
    __m512 cv_l = _mm512_maskz_compress_ps(m_l, v_l);
    __m512 cv_r = _mm512_maskz_compress_ps(m_r, v_r);
    __m512 d = _mm512_sub_ps(cv_l, cv_r);
    dd = _mm512_fmadd_ps(d, d, dd);
    dd = _mm512_fmsub_ps(cv_l, cv_l, dd);
    dd = _mm512_fmsub_ps(cv_r, cv_r, dd);
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
    __m512 d = _mm512_sub_ps(cv_l, cv_r);
    dd = _mm512_fmadd_ps(d, d, dd);
    dd = _mm512_fmsub_ps(cv_l, cv_l, dd);
    dd = _mm512_fmsub_ps(cv_r, cv_r, dd);
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

  __m512 v;
  lhs_pos = rhs_pos = 0;
  while (lhs_pos < lhs_loop_len) {
    v = _mm512_loadu_ps(lhs_val + lhs_pos);
    dd = _mm512_fmadd_ps(v, v, dd);
    lhs_pos += 16;
  }
  v = _mm512_maskz_loadu_ps(_bzhi_u32(0xFFFF, lhs_len - lhs_pos),
                            lhs_val + lhs_pos);
  dd = _mm512_fmadd_ps(v, v, dd);
  while (rhs_pos < rhs_loop_len) {
    v = _mm512_loadu_ps(rhs_val + rhs_pos);
    dd = _mm512_fmadd_ps(v, v, dd);
    rhs_pos += 16;
  }
  v = _mm512_maskz_loadu_ps(_bzhi_u32(0xFFFF, rhs_len - rhs_pos),
                            rhs_val + rhs_pos);
  dd = _mm512_fmadd_ps(v, v, dd);

  return _mm512_reduce_add_ps(dd);
}

#endif
