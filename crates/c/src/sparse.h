#include <stddef.h>
#include <stdint.h>

#if defined(__x86_64__)

extern float v_sparse_dot_avx512vp2intersect(uint32_t *, uint32_t *, float *,
                                            float *, size_t, size_t);

#endif