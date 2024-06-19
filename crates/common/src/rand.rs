use rand::Rng;

pub fn sample_u32<R>(rng: &mut R, length: u32, amount: u32) -> Vec<u32>
where
    R: Rng + ?Sized,
{
    match rand::seq::index::sample(rng, length as usize, amount as usize) {
        rand::seq::index::IndexVec::U32(x) => x,
        _ => unreachable!(),
    }
}
