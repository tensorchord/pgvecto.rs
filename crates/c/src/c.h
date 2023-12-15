#include <stddef.h>
#include <stdint.h>

#if defined(__x86_64__)

extern float v_f16_cosine_avx512fp16(_Float16 *, _Float16 *, size_t n);
extern float v_f16_dot_avx512fp16(_Float16 *, _Float16 *, size_t n);
extern float v_f16_sl2_avx512fp16(_Float16 *, _Float16 *, size_t n);
extern float v_f16_cosine_v4(_Float16 *, _Float16 *, size_t n);
extern float v_f16_dot_v4(_Float16 *, _Float16 *, size_t n);
extern float v_f16_sl2_v4(_Float16 *, _Float16 *, size_t n);
extern float v_f16_cosine_v3(_Float16 *, _Float16 *, size_t n);
extern float v_f16_dot_v3(_Float16 *, _Float16 *, size_t n);
extern float v_f16_sl2_v3(_Float16 *, _Float16 *, size_t n);

#endif
