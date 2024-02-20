#include <stddef.h>
#include <stdint.h>

#if defined(__x86_64__)

extern float v_binary_cosine_avx512vpopcntdq(size_t *, size_t *, size_t n);
extern float v_binary_dot_avx512vpopcntdq(size_t *, size_t *, size_t n);
extern float v_binary_sl2_avx512vpopcntdq(size_t *, size_t *, size_t n);
extern float v_binary_cnt_avx512vpopcntdq(size_t *, size_t n);
extern float v_binary_cosine_v4(size_t *, size_t *, size_t n);
extern float v_binary_dot_v4(size_t *, size_t *, size_t n);
extern float v_binary_sl2_v4(size_t *, size_t *, size_t n);
extern float v_binary_cnt_v4(size_t *, size_t n);
extern float v_binary_cosine_v3(size_t *, size_t *, size_t n);
extern float v_binary_dot_v3(size_t *, size_t *, size_t n);
extern float v_binary_sl2_v3(size_t *, size_t *, size_t n);
extern float v_binary_cnt_v3(size_t *, size_t n);

#endif
