#include "binary.h"
#include <math.h>

#if defined(__x86_64__)
#include <immintrin.h>
#endif

#if defined(__x86_64__)

#define WIDTH (512 / 8 / sizeof(size_t))

__attribute__((target("arch=x86-64-v4,avx512vpopcntdq"))) extern float
v_binary_cosine_avx512vpopcntdq(size_t *a, size_t *b, size_t n) {
  __m512i xy = _mm512_setzero_si512();
  __m512i xx = _mm512_setzero_si512();
  __m512i yy = _mm512_setzero_si512();

  while (n >= WIDTH) {
    __m512i x = _mm512_loadu_si512(a);
    __m512i y = _mm512_loadu_si512(b);
    a += WIDTH, b += WIDTH, n -= WIDTH;
    xy = _mm512_add_epi64(xy, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
    xx = _mm512_add_epi64(xx, _mm512_popcnt_epi64(x));
    yy = _mm512_add_epi64(yy, _mm512_popcnt_epi64(y));
  }
  if (n > 0) {
    __mmask8 mask = _bzhi_u32(0xFFFF, n);
    __m512i x = _mm512_maskz_loadu_epi64(mask, a);
    __m512i y = _mm512_maskz_loadu_epi64(mask, b);
    xy = _mm512_add_epi64(xy, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
    xx = _mm512_add_epi64(xx, _mm512_popcnt_epi64(x));
    yy = _mm512_add_epi64(yy, _mm512_popcnt_epi64(y));
  }
  {
    float rxy = (float)_mm512_reduce_add_epi64(xy);
    float rxx = (float)_mm512_reduce_add_epi64(xx);
    float ryy = (float)_mm512_reduce_add_epi64(yy);
    return rxy / sqrt(rxx * ryy);
  }
}

__attribute__((target("arch=x86-64-v4,avx512vpopcntdq"))) extern float
v_binary_dot_avx512vpopcntdq(size_t *a, size_t *b, size_t n) {
  __m512i xy = _mm512_setzero_si512();

  while (n >= WIDTH) {
    __m512i x = _mm512_loadu_si512(a);
    __m512i y = _mm512_loadu_si512(b);
    a += WIDTH, b += WIDTH, n -= WIDTH;
    xy = _mm512_add_epi64(xy, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
  }
  if (n > 0) {
    __mmask8 mask = _bzhi_u32(0xFFFF, n);
    __m512i x = _mm512_maskz_loadu_epi64(mask, a);
    __m512i y = _mm512_maskz_loadu_epi64(mask, b);
    xy = _mm512_add_epi64(xy, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
  }
  return (float)_mm512_reduce_add_epi64(xy);
}

__attribute__((target("arch=x86-64-v4,avx512vpopcntdq"))) extern float
v_binary_sl2_avx512vpopcntdq(size_t *a, size_t *b, size_t n) {
  __m512i dd = _mm512_setzero_si512();

  while (n >= WIDTH) {
    __m512i x = _mm512_loadu_si512(a);
    __m512i y = _mm512_loadu_si512(b);
    a += WIDTH, b += WIDTH, n -= WIDTH;
    dd = _mm512_add_epi64(dd, _mm512_popcnt_epi64(_mm512_xor_si512(x, y)));
  }
  if (n > 0) {
    __mmask8 mask = _bzhi_u32(0xFFFF, n);
    __m512i x = _mm512_maskz_loadu_epi64(mask, a);
    __m512i y = _mm512_maskz_loadu_epi64(mask, b);
    dd = _mm512_add_epi64(dd, _mm512_popcnt_epi64(_mm512_xor_si512(x, y)));
  }
  return (float)_mm512_reduce_add_epi64(dd);
}

__attribute__((target("arch=x86-64-v4,avx512vpopcntdq"))) extern float
v_binary_cnt_avx512vpopcntdq(size_t *a, size_t n) {
  __m512i cnt = _mm512_setzero_si512();

  while (n >= WIDTH) {
    __m512i x = _mm512_loadu_si512(a);
    a += WIDTH, n -= WIDTH;
    cnt = _mm512_add_epi64(cnt, _mm512_popcnt_epi64(x));
  }
  if (n > 0) {
    __mmask8 mask = _bzhi_u32(0xFFFF, n);
    __m512i x = _mm512_maskz_loadu_epi64(mask, a);
    cnt = _mm512_add_epi64(cnt, _mm512_popcnt_epi64(x));
  }
  return (float)_mm512_reduce_add_epi64(cnt);
}

__attribute__((target("arch=x86-64-v4"))) extern float
v_binary_cosine_v4(size_t *a, size_t *b, size_t n) {
  int xy = 0;
  int xx = 0;
  int yy = 0;

#pragma clang loop vectorize_width(8)
  for (size_t i = 0; i < n; i++) {
    size_t x = a[i];
    size_t y = b[i];
    xy += __builtin_popcountl(x & y);
    xx += __builtin_popcountl(x);
    yy += __builtin_popcountl(y);
  }
  return (float)xy / sqrt((float)xx * (float)yy);
}

__attribute__((target("arch=x86-64-v4"))) extern float
v_binary_dot_v4(size_t *a, size_t *b, size_t n) {
  int xy = 0;

#pragma clang loop vectorize_width(8)
  for (size_t i = 0; i < n; i++) {
    size_t x = a[i];
    size_t y = b[i];
    xy += __builtin_popcountl(x & y);
  }
  return (float)xy;
}

__attribute__((target("arch=x86-64-v4"))) extern float
v_binary_sl2_v4(size_t *a, size_t *b, size_t n) {
  int dd = 0;

#pragma clang loop vectorize_width(8)
  for (size_t i = 0; i < n; i++) {
    size_t x = a[i];
    size_t y = b[i];
    dd += __builtin_popcountl(x ^ y);
  }
  return (float)dd;
}

__attribute__((target("arch=x86-64-v4"))) extern float
v_binary_cnt_v4(size_t *a, size_t n) {
  int cnt = 0;

#pragma clang loop vectorize_width(8)
  for (size_t i = 0; i < n; i++) {
    size_t x = a[i];
    cnt += __builtin_popcountl(x);
  }
  return (float)cnt;
}

__attribute__((target("arch=x86-64-v3"))) extern float
v_binary_cosine_v3(size_t *a, size_t *b, size_t n) {
  int xy = 0;
  int xx = 0;
  int yy = 0;

#pragma clang loop vectorize_width(8)
  for (size_t i = 0; i < n; i++) {
    size_t x = a[i];
    size_t y = b[i];
    xy += __builtin_popcountl(x & y);
    xx += __builtin_popcountl(x);
    yy += __builtin_popcountl(y);
  }
  return (float)xy / sqrt((float)xx * (float)yy);
}

__attribute__((target("arch=x86-64-v3"))) extern float
v_binary_dot_v3(size_t *a, size_t *b, size_t n) {
  int xy = 0;

#pragma clang loop vectorize_width(8)
  for (size_t i = 0; i < n; i++) {
    size_t x = a[i];
    size_t y = b[i];
    xy += __builtin_popcountl(x & y);
  }
  return (float)xy;
}

__attribute__((target("arch=x86-64-v3"))) extern float
v_binary_sl2_v3(size_t *a, size_t *b, size_t n) {
  int dd = 0;

#pragma clang loop vectorize_width(8)
  for (size_t i = 0; i < n; i++) {
    size_t x = a[i];
    size_t y = b[i];
    dd += __builtin_popcountl(x ^ y);
  }
  return (float)dd;
}

__attribute__((target("arch=x86-64-v3"))) extern float
v_binary_cnt_v3(size_t *a, size_t n) {
  int cnt = 0;

#pragma clang loop vectorize_width(8)
  for (size_t i = 0; i < n; i++) {
    size_t x = a[i];
    cnt += __builtin_popcountl(x);
  }
  return (float)cnt;
}

#undef WIDTH

#endif
