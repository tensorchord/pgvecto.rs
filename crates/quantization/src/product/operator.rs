use base::operator::*;
use base::scalar::*;
use base::vector::*;
use elkan_k_means::operator::OperatorElkanKMeans;
use num_traits::{Float, Zero};

pub trait OperatorProductQuantization: Operator {
    type ProductQuantizationL2: Operator<VectorOwned = Self::VectorOwned>
        + OperatorElkanKMeans
        + OperatorProductQuantization;
    fn product_quantization_distance(
        dims: u32,
        ratio: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
        rhs: &[u8],
    ) -> F32;
    fn product_quantization_distance2(
        dims: u32,
        ratio: u32,
        centroids: &[Scalar<Self>],
        lhs: &[u8],
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
    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32;
    fn product_quantization_dense_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32;
}

impl OperatorProductQuantization for BVecf32Cos {
    type ProductQuantizationL2 = BVecf32L2;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance2(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance_with_delta(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(_: &[Scalar<Self>], _: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}

impl OperatorProductQuantization for BVecf32Dot {
    type ProductQuantizationL2 = BVecf32L2;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance2(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance_with_delta(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(_: &[Scalar<Self>], _: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}

impl OperatorProductQuantization for BVecf32Jaccard {
    type ProductQuantizationL2 = BVecf32L2;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance2(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance_with_delta(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(_: &[Scalar<Self>], _: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}

impl OperatorProductQuantization for BVecf32L2 {
    type ProductQuantizationL2 = BVecf32L2;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance2(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance_with_delta(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(_: &[Scalar<Self>], _: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}

impl OperatorProductQuantization for SVecf32Cos {
    type ProductQuantizationL2 = SVecf32L2;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance2(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance_with_delta(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(_: &[Scalar<Self>], _: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}

impl OperatorProductQuantization for SVecf32Dot {
    type ProductQuantizationL2 = SVecf32L2;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance2(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance_with_delta(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(_: &[Scalar<Self>], _: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}

impl OperatorProductQuantization for SVecf32L2 {
    type ProductQuantizationL2 = SVecf32L2;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: SVecf32Borrowed<'_>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance2(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance_with_delta(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: SVecf32Borrowed<'_>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(_: &[Scalar<Self>], _: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}

impl OperatorProductQuantization for Vecf16Cos {
    type ProductQuantizationL2 = Vecf16L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let (_xy, _x2, _y2) = vecf16::xy_x2_y2(lhs, rhs);
            xy += _xy;
            x2 += _x2;
            y2 += _y2;
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance2(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhsp = lhs[i as usize] as usize * dims as usize;
            let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let (_xy, _x2, _y2) = vecf16::xy_x2_y2(lhs, rhs);
            xy += _xy;
            x2 += _x2;
            y2 += _y2;
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
        delta: &[F16],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let del = &delta[(i * ratio) as usize..][..k as usize];
            let (_xy, _x2, _y2) = vecf16::xy_x2_y2_delta(lhs, rhs, del);
            xy += _xy;
            x2 += _x2;
            y2 += _y2;
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf16::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        F32(1.0) - vecf16::cosine(lhs, rhs)
    }
}

impl OperatorProductQuantization for Vecf16Dot {
    type ProductQuantizationL2 = Vecf16L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let _xy = vecf16::dot(lhs, rhs);
            xy += _xy;
        }
        xy * (-1.0)
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance2(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhsp = lhs[i as usize] as usize * dims as usize;
            let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let _xy = vecf16::dot(lhs, rhs);
            xy += _xy;
        }
        xy * (-1.0)
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
        delta: &[F16],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let del = &delta[(i * ratio) as usize..][..k as usize];
            let _xy = vecf16::dot_delta(lhs, rhs, del);
            xy += _xy;
        }
        xy * (-1.0)
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf16::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf16::dot(lhs, rhs) * (-1.0)
    }
}

impl OperatorProductQuantization for Vecf16L2 {
    type ProductQuantizationL2 = Vecf16L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            result += vecf16::sl2(lhs, rhs);
        }
        result
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance2(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhsp = lhs[i as usize] as usize * dims as usize;
            let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            result += vecf16::sl2(lhs, rhs);
        }
        result
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
        delta: &[F16],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let del = &delta[(i * ratio) as usize..][..k as usize];
            result += vecf16::distance_squared_l2_delta(lhs, rhs, del);
        }
        result
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf16::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf16::sl2(lhs, rhs)
    }
}

impl OperatorProductQuantization for Vecf32Cos {
    type ProductQuantizationL2 = Vecf32L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let (_xy, _x2, _y2) = vecf32::xy_x2_y2(lhs, rhs);
            xy += _xy;
            x2 += _x2;
            y2 += _y2;
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance2(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhsp = lhs[i as usize] as usize * dims as usize;
            let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let (_xy, _x2, _y2) = vecf32::xy_x2_y2(lhs, rhs);
            xy += _xy;
            x2 += _x2;
            y2 += _y2;
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'a>,
        rhs: &[u8],
        delta: &[F32],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let del = &delta[(i * ratio) as usize..][..k as usize];
            let (_xy, _x2, _y2) = vecf32::xy_x2_y2_delta(lhs, rhs, del);
            xy += _xy;
            x2 += _x2;
            y2 += _y2;
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        F32(1.0) - vecf32::cosine(lhs, rhs)
    }
}

impl OperatorProductQuantization for Vecf32Dot {
    type ProductQuantizationL2 = Vecf32L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let _xy = vecf32::dot(lhs, rhs);
            xy += _xy;
        }
        xy * (-1.0)
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance2(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhsp = lhs[i as usize] as usize * dims as usize;
            let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let _xy = vecf32::dot(lhs, rhs);
            xy += _xy;
        }
        xy * (-1.0)
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'a>,
        rhs: &[u8],
        delta: &[F32],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let del = &delta[(i * ratio) as usize..][..k as usize];
            let _xy = vecf32::dot_delta(lhs, rhs, del);
            xy += _xy;
        }
        xy * (-1.0)
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs) * (-1.0)
    }
}

impl OperatorProductQuantization for Vecf32L2 {
    type ProductQuantizationL2 = Vecf32L2;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            result += vecf32::sl2(lhs, rhs);
        }
        result
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance2(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhsp = lhs[i as usize] as usize * dims as usize;
            let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            result += vecf32::sl2(lhs, rhs);
        }
        result
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn product_quantization_distance_with_delta<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F32],
        lhs: Vecf32Borrowed<'a>,
        rhs: &[u8],
        delta: &[F32],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let del = &delta[(i * ratio) as usize..][..k as usize];
            result += vecf32::distance_squared_l2_delta(lhs, rhs, del);
        }
        result
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs)
    }
}

impl OperatorProductQuantization for Veci8Cos {
    type ProductQuantizationL2 = Veci8Cos;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_distance2(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_distance_with_delta(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_l2_distance(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
    fn product_quantization_dense_distance(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}

impl OperatorProductQuantization for Veci8Dot {
    type ProductQuantizationL2 = Veci8Dot;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_distance2(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_distance_with_delta(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_l2_distance(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
    fn product_quantization_dense_distance(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}

impl OperatorProductQuantization for Veci8L2 {
    type ProductQuantizationL2 = Veci8L2;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_distance2(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_distance_with_delta(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_l2_distance(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
    fn product_quantization_dense_distance(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}
