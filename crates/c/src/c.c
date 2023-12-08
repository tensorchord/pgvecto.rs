#include "c.h"

__attribute__((target("avx512fp16,avx512vl,avx512f,bmi2"))) extern float
vectors_f16_cosine_axv512(_Float16 const *restrict a,
                          _Float16 const *restrict b, size_t n) {
  _Float16 xy = 0.0;
  _Float16 x2 = 0.0;
  _Float16 y2 = 0.0;
  for (size_t i = 0; i < n; i++) {
    xy += a[i] * b[i];
    x2 += a[i] * a[i];
    y2 += b[i] * b[i];
  }
  return xy / sqrt(x2 * y2);
}

__attribute__((target("avx512fp16,avx512vl,avx512f,bmi2"))) extern float
vectors_f16_dot_axv512(_Float16 const *restrict a, _Float16 const *restrict b,
                       size_t n) {
  _Float16 result = 0.0;
  for (size_t i = 0; i < n; i++) {
    result += a[i] * b[i];
  }
  return result;
}

__attribute__((target("avx512fp16,avx512vl,avx512f,bmi2"))) extern float
vectors_f16_distance_squared_l2_axv512(_Float16 const *restrict a,
                                       _Float16 const *restrict b, size_t n) {
  _Float16 result = 0.0;
  for (size_t i = 0; i < n; i++) {
    _Float16 d = a[i] - b[i];
    result += d * d;
  }
  return result;
}
