#include <stddef.h>
#include <stdint.h>

extern float vectors_f16_cosine_axv512(_Float16 const *, _Float16 const *,
                                       size_t n);
extern float vectors_f16_dot_axv512(_Float16 const *, _Float16 const *,
                                    size_t n);
extern float vectors_f16_distance_squared_l2_axv512(_Float16 const *,
                                                    _Float16 const *, size_t n);
