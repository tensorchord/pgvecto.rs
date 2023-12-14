#include "c.h"
#include <math.h>

#if defined(__x86_64__)
#include <immintrin.h>
#endif

#if defined(__x86_64__)

__attribute__((target("arch=x86-64-v4,avx512fp16"))) extern float
v_f16_cosine_avx512fp16(_Float16 *a, _Float16 *b, size_t n) {
  __m512h xy = _mm512_set1_ph(0);
  __m512h xx = _mm512_set1_ph(0);
  __m512h yy = _mm512_set1_ph(0);

  while (n >= 32) {
    __m512h x = _mm512_loadu_ph(a);
    __m512h y = _mm512_loadu_ph(b);
    a += 32, b += 32, n -= 32;
    xy = _mm512_fmadd_ph(x, y, xy);
    xx = _mm512_fmadd_ph(x, x, xx);
    yy = _mm512_fmadd_ph(y, y, yy);
  }
  if (n > 0) {
    __mmask32 mask = _bzhi_u32(0xFFFFFFFF, n);
    __m512h x = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, a));
    __m512h y = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, b));
    xy = _mm512_fmadd_ph(x, y, xy);
    xx = _mm512_fmadd_ph(x, x, xx);
    yy = _mm512_fmadd_ph(y, y, yy);
  }
  return (float)(_mm512_reduce_add_ph(xy) /
                 sqrt(_mm512_reduce_add_ph(xx) * _mm512_reduce_add_ph(yy)));
}

__attribute__((target("arch=x86-64-v4,avx512fp16"))) extern float
v_f16_dot_avx512fp16(_Float16 *a, _Float16 *b, size_t n) {
  __m512h xy = _mm512_set1_ph(0);

  while (n >= 32) {
    __m512h x = _mm512_loadu_ph(a);
    __m512h y = _mm512_loadu_ph(b);
    a += 32, b += 32, n -= 32;
    xy = _mm512_fmadd_ph(x, y, xy);
  }
  if (n > 0) {
    __mmask32 mask = _bzhi_u32(0xFFFFFFFF, n);
    __m512h x = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, a));
    __m512h y = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, b));
    xy = _mm512_fmadd_ph(x, y, xy);
  }
  return (float)_mm512_reduce_add_ph(xy);
}

__attribute__((target("arch=x86-64-v4,avx512fp16"))) extern float
v_f16_sl2_avx512fp16(_Float16 *a, _Float16 *b, size_t n) {
  __m512h dd = _mm512_set1_ph(0);

  while (n >= 32) {
    __m512h x = _mm512_loadu_ph(a);
    __m512h y = _mm512_loadu_ph(b);
    a += 32, b += 32, n -= 32;
    __m512h d = _mm512_sub_ph(x, y);
    dd = _mm512_fmadd_ph(d, d, dd);
  }
  if (n > 0) {
    __mmask32 mask = _bzhi_u32(0xFFFFFFFF, n);
    __m512h x = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, a));
    __m512h y = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, b));
    __m512h d = _mm512_sub_ph(x, y);
    dd = _mm512_fmadd_ph(d, d, dd);
  }

  return (float)_mm512_reduce_add_ph(dd);
}

__attribute__((target("arch=x86-64-v3"))) extern float
v_f16_cosine_v3(_Float16 *a, _Float16 *b, size_t n) {
  float xy = 0;
  float xx = 0;
  float yy = 0;
#pragma clang loop vectorize_width(8)
  for (size_t i = 0; i < n; i++) {
    float x = a[i];
    float y = b[i];
    xy += x * y;
    xx += x * x;
    yy += y * y;
  }
  return xy / sqrt(xx * yy);
}

__attribute__((target("arch=x86-64-v3"))) extern float
v_f16_dot_v3(_Float16 *a, _Float16 *b, size_t n) {
  float xy = 0;
#pragma clang loop vectorize_width(8)
  for (size_t i = 0; i < n; i++) {
    float x = a[i];
    float y = b[i];
    xy += x * y;
  }
  return xy;
}

__attribute__((target("arch=x86-64-v3"))) extern float
v_f16_sl2_v3(_Float16 *a, _Float16 *b, size_t n) {
  float dd = 0;
#pragma clang loop vectorize_width(8)
  for (size_t i = 0; i < n; i++) {
    float x = a[i];
    float y = b[i];
    float d = x - y;
    dd += d * d;
  }
  return dd;
}

#endif
