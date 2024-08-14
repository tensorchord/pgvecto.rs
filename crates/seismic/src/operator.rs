use base::operator::*;
use base::vector::SVecf32Borrowed;
use quantization::operator::OperatorQuantization;
use storage::OperatorStorage;

pub trait OperatorSeismic: OperatorQuantization + OperatorStorage {
    fn cast_svec(vec: Borrowed<'_, Self>) -> SVecf32Borrowed;

    fn prefetch(storage: &Self::Storage, i: u32);
}

impl OperatorSeismic for SVecf32Dot {
    fn cast_svec(vec: Borrowed<'_, Self>) -> SVecf32Borrowed {
        vec
    }

    fn prefetch(storage: &Self::Storage, i: u32) {
        storage.prefetch(i)
    }
}

macro_rules! unimpl_operator_seismic {
    ($t:ty) => {
        impl OperatorSeismic for $t {
            fn cast_svec(_: Borrowed<'_, Self>) -> SVecf32Borrowed {
                unimplemented!()
            }

            fn prefetch(_: &Self::Storage, _: u32) {
                unimplemented!()
            }
        }
    };
}

unimpl_operator_seismic!(SVecf32L2);
unimpl_operator_seismic!(BVectorDot);
unimpl_operator_seismic!(BVectorJaccard);
unimpl_operator_seismic!(BVectorHamming);
unimpl_operator_seismic!(Vecf32Dot);
unimpl_operator_seismic!(Vecf32L2);
unimpl_operator_seismic!(Vecf16Dot);
unimpl_operator_seismic!(Vecf16L2);
