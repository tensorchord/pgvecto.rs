use base::operator::*;
use base::scalar::*;
use base::vector::*;
use elkan_k_means::operator::OperatorElkanKMeans;
use num_traits::{Float, Zero};

pub trait OperatorProductQuantization: Operator {
    type PQL2: Operator<VectorOwned = Self::VectorOwned>
        + OperatorElkanKMeans
        + OperatorProductQuantization;
    fn product_quantization_distance(
        dims: u32,
        ratio: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
        rhs: &[u8],
    ) -> F32;
    fn product_quantization_distance_with_delta(
        dims: u32,
        ratio: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
        rhs: &[u8],
        delta: &[Scalar<Self>],
    ) -> F32;
    fn dense_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32;
}

impl OperatorProductQuantization for Vecf32Cos {
    type PQL2 = Vecf32L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'_>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            for j in 0..k {
                xy += lhs[j] * rhs[j];
                x2 += lhs[j] * lhs[j];
                y2 += rhs[j] * rhs[j];
            }
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'_>,
        rhs: &[u8],
        delta: &[F32],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            let del = &delta[i * ratio..][..k];
            for j in 0..k {
                xy += lhs[j] * (rhs[j] + del[j]);
                x2 += lhs[j] * lhs[j];
                y2 += (rhs[j] + del[j]) * (rhs[j] + del[j]);
            }
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    fn dense_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }
}

impl OperatorProductQuantization for Vecf32Dot {
    type PQL2 = Vecf32L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'_>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            for j in 0..k {
                xy += lhs[j] * rhs[j];
            }
        }
        xy * (-1.0)
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'_>,
        rhs: &[u8],
        delta: &[F32],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            let del = &delta[i * ratio..][..k];
            for j in 0..k {
                xy += lhs[j] * (rhs[j] + del[j]);
            }
        }
        xy * (-1.0)
    }

    fn dense_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }
}

impl OperatorProductQuantization for Vecf32L2 {
    type PQL2 = Vecf32L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'_>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            for j in 0..k {
                let d = lhs[j] - rhs[j];
                result += d * d;
            }
        }
        result
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'_>,
        rhs: &[u8],
        delta: &[F32],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            let del = &delta[i * ratio..][..k];
            for j in 0..k {
                let d = lhs[j] - (rhs[j] + del[j]);
                result += d * d;
            }
        }
        result
    }

    fn dense_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }
}

impl OperatorProductQuantization for Vecf16Cos {
    type PQL2 = Vecf16L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'_>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            for j in 0..k {
                xy += lhs[j].to_f() * rhs[j].to_f();
                x2 += lhs[j].to_f() * lhs[j].to_f();
                y2 += rhs[j].to_f() * rhs[j].to_f();
            }
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'_>,
        rhs: &[u8],
        delta: &[F16],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            let del = &delta[i * ratio..][..k];
            for j in 0..k {
                xy += lhs[j].to_f() * (rhs[j].to_f() + del[j].to_f());
                x2 += lhs[j].to_f() * lhs[j].to_f();
                y2 += (rhs[j].to_f() + del[j].to_f()) * (rhs[j].to_f() + del[j].to_f());
            }
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    fn dense_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf16::sl2(lhs, rhs)
    }
}

impl OperatorProductQuantization for Vecf16Dot {
    type PQL2 = Vecf16L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'_>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            for j in 0..k {
                xy += lhs[j].to_f() * rhs[j].to_f();
            }
        }
        xy * (-1.0)
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'_>,
        rhs: &[u8],
        delta: &[F16],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            let del = &delta[i * ratio..][..k];
            for j in 0..k {
                xy += lhs[j].to_f() * (rhs[j].to_f() + del[j].to_f());
            }
        }
        xy * (-1.0)
    }

    fn dense_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf16::sl2(lhs, rhs)
    }
}

impl OperatorProductQuantization for Vecf16L2 {
    type PQL2 = Vecf16L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'_>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            for j in 0..k {
                let d = lhs[j].to_f() - rhs[j].to_f();
                result += d * d;
            }
        }
        result
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'_>,
        rhs: &[u8],
        delta: &[F16],
    ) -> F32 {
        let lhs = lhs.slice();
        let ratio = ratio as usize;
        let width = (dims as usize).div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims as usize - i * ratio);
            let lhs = &lhs[i * ratio..][..k];
            let off = rhs[i] as usize * dims as usize;
            let rhs = &centroids[off + i * ratio..][..k];
            let del = &delta[i * ratio..][..k];
            for j in 0..k {
                let d = lhs[j].to_f() - (rhs[j].to_f() + del[j].to_f());
                result += d * d;
            }
        }
        result
    }

    fn dense_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf16::sl2(lhs, rhs)
    }
}

macro_rules! unimpl_operator_product_quantization {
    ($t:ty, $l:ty) => {
        impl OperatorProductQuantization for $t {
            fn product_quantization_distance(
                _: u32,
                _: u32,
                _: &[Scalar<Self>],
                _: Borrowed<'_, Self>,
                _: &[u8],
            ) -> F32 {
                unimplemented!()
            }

            fn product_quantization_distance_with_delta(
                _: u32,
                _: u32,
                _: &[Scalar<Self>],
                _: Borrowed<'_, Self>,
                _: &[u8],
                _: &[Scalar<Self>],
            ) -> F32 {
                unimplemented!()
            }

            type PQL2 = $l;

            fn dense_l2_distance(_: &[Scalar<Self>], _: &[Scalar<Self>]) -> F32 {
                unimplemented!()
            }
        }
    };
}

unimpl_operator_product_quantization!(BVecf32Cos, BVecf32L2);
unimpl_operator_product_quantization!(BVecf32Dot, BVecf32L2);
unimpl_operator_product_quantization!(BVecf32L2, BVecf32L2);
unimpl_operator_product_quantization!(BVecf32Jaccard, BVecf32L2);

unimpl_operator_product_quantization!(SVecf32Cos, SVecf32L2);
unimpl_operator_product_quantization!(SVecf32Dot, SVecf32L2);
unimpl_operator_product_quantization!(SVecf32L2, SVecf32L2);

unimpl_operator_product_quantization!(Veci8Cos, Veci8L2);
unimpl_operator_product_quantization!(Veci8Dot, Veci8L2);
unimpl_operator_product_quantization!(Veci8L2, Veci8L2);
