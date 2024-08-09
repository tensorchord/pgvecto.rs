use base::{
    scalar::F32,
    vector::{SVecf32Borrowed, SVecf32Owned, VectorBorrowed},
};
use qwt::{DArray, SelectBin};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct QuantizedSummary {
    dims: u32,
    mins: Box<[F32]>,
    quants: Box<[F32]>, // quantization step
    summary_ids: Box<[u16]>,
    codes: Box<[u8]>,
    offsets: DArray<false>,
}

impl QuantizedSummary {
    pub fn create(dims: u32, vectors: &[SVecf32Owned]) -> Self {
        let mut summary_ids = vec![Vec::new(); dims as usize];
        let mut codes = vec![Vec::new(); dims as usize];
        let mut mins = Vec::with_capacity(vectors.len());
        let mut quants = Vec::with_capacity(vectors.len());

        for (id, vec) in vectors.iter().enumerate() {
            let indexes = vec.indexes();
            let values = vec.values();

            let (min, max) = values
                .iter()
                .fold((values[0], values[0]), |(min, max), &value| {
                    (min.min(value), max.max(value))
                });
            let quant = (max - min) / 256.;
            let quant_codes = values
                .iter()
                .map(|&value| (((value - min) / quant).0 as u32).clamp(0, 255) as u8);

            mins.push(min);
            quants.push(quant);
            for (&idx, code) in indexes.iter().zip(quant_codes) {
                summary_ids[idx as usize].push(id as u16);
                codes[idx as usize].push(code);
            }
        }

        let offsets = std::iter::once(0)
            .chain(summary_ids.iter().map(|ids| ids.len()).scan(0, |state, x| {
                *state += x + 1;
                Some(*state)
            }))
            .collect();
        let summary_ids = summary_ids.into_iter().flatten().collect();
        let codes = codes.into_iter().flatten().collect();

        QuantizedSummary {
            dims,
            mins: mins.into_boxed_slice(),
            quants: quants.into_boxed_slice(),
            summary_ids,
            codes,
            offsets,
        }
    }

    pub fn len(&self) -> usize {
        self.mins.len()
    }

    pub fn matmul(&self, query: SVecf32Borrowed) -> Vec<F32> {
        assert!(query.dims() == self.dims);
        let mut results = vec![F32(0.); self.len()];

        for (&qi, &qv) in query.indexes().iter().zip(query.values()) {
            let start = self.offsets.select1(qi as usize).unwrap() - qi as usize;
            let end = self.offsets.select1(qi as usize + 1).unwrap() - qi as usize - 1;
            let current_summary_ids = &self.summary_ids[start..end];
            let current_codes = &self.codes[start..end];

            for (&sid, &v) in current_summary_ids.iter().zip(current_codes) {
                let val = F32(v as f32) * self.quants[sid as usize] + self.mins[sid as usize];
                results[sid as usize] += val * qv;
            }
        }

        results
    }
}
