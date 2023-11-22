pub type Payload = u64;

pub trait Filter {
    fn check(&mut self, payload: Payload) -> bool;
}

impl<F> Filter for F
where
    F: FnMut(Payload) -> bool,
{
    fn check(&mut self, payload: Payload) -> bool {
        self(payload)
    }
}
