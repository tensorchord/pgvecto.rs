use base::operator::*;
use base::scalar::*;
use base::vector::*;
use num_traits::{Float, Zero};

pub trait OperatorScalarQuantization: Operator {
    fn scalar_quantization_distance(
        dims: u16,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
        rhs: &[u8],
    ) -> F32;
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32;
}

impl OperatorScalarQuantization for BVecf32Cos {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[F32],
        _min: &[F32],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
}

impl OperatorScalarQuantization for BVecf32Dot {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[F32],
        _min: &[F32],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
}

impl OperatorScalarQuantization for BVecf32Jaccard {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[F32],
        _min: &[F32],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
}

impl OperatorScalarQuantization for BVecf32L2 {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[F32],
        _min: &[F32],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
}

impl OperatorScalarQuantization for SVecf32Cos {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[F32],
        _min: &[F32],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
}

impl OperatorScalarQuantization for SVecf32Dot {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
}

impl OperatorScalarQuantization for SVecf32L2 {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: SVecf32Borrowed<'_>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
}

impl OperatorScalarQuantization for Vecf16Cos {
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance<'a>(
        dims: u16,
        max: &[F16],
        min: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            xy += _x * _y;
            x2 += _x * _x;
            y2 += _y * _y;
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[F16],
        min: &[F16],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            xy += _x * _y;
            x2 += _x * _x;
            y2 += _y * _y;
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }
}

impl OperatorScalarQuantization for Vecf16Dot {
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance<'a>(
        dims: u16,
        max: &[F16],
        min: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let mut xy = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            xy += _x * _y;
        }
        xy * (-1.0)
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[F16],
        min: &[F16],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let mut xy = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            xy += _x * _y;
        }
        xy * (-1.0)
    }
}

impl OperatorScalarQuantization for Vecf16L2 {
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance<'a>(
        dims: u16,
        max: &[F16],
        min: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let mut result = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i].to_f();
            let _y = (F32(rhs[i] as f32) / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            result += (_x - _y) * (_x - _y);
        }
        result
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[F16],
        min: &[F16],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let mut result = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            result += (_x - _y) * (_x - _y);
        }
        result
    }
}

impl OperatorScalarQuantization for Vecf32Cos {
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance<'a>(
        dims: u16,
        max: &[F32],
        min: &[F32],
        lhs: Vecf32Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i];
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            xy += _x * _y;
            x2 += _x * _x;
            y2 += _y * _y;
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[F32],
        min: &[F32],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            xy += _x * _y;
            x2 += _x * _x;
            y2 += _y * _y;
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }
}

impl OperatorScalarQuantization for Vecf32Dot {
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance<'a>(
        dims: u16,
        max: &[F32],
        min: &[F32],
        lhs: Vecf32Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let mut xy = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i];
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            xy += _x * _y;
        }
        xy * (-1.0)
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[F32],
        min: &[F32],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let mut xy = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            xy += _x * _y;
        }
        xy * (-1.0)
    }
}

impl OperatorScalarQuantization for Vecf32L2 {
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance<'a>(
        dims: u16,
        max: &[F32],
        min: &[F32],
        lhs: Vecf32Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let mut result = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i];
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            result += (_x - _y) * (_x - _y);
        }
        result
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[F32],
        min: &[F32],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let mut result = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            result += (_x - _y) * (_x - _y);
        }
        result
    }
}

impl OperatorScalarQuantization for Veci8Cos {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
}

impl OperatorScalarQuantization for Veci8Dot {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
}

impl OperatorScalarQuantization for Veci8L2 {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
}
