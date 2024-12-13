pub fn reduce_sum_of_xy(s: &[u8], t: &[u8]) -> u32 {
    reduce_sum_of_xy::reduce_sum_of_xy(s, t)
}

mod reduce_sum_of_xy {
    #[crate::simd::multiversion("v4", "v3", "v2", "v8.3a:sve", "v8.3a")]
    pub fn reduce_sum_of_xy(s: &[u8], t: &[u8]) -> u32 {
        assert_eq!(s.len(), t.len());
        let n = s.len();
        let mut result = 0;
        for i in 0..n {
            let (s, t) = (s[i], t[i]);
            result += ((s & 15) as u32) * ((t & 15) as u32);
            result += ((s >> 4) as u32) * ((t >> 4) as u32);
        }
        result
    }
}
