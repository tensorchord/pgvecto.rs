pub struct RefPeekable<I: Iterator> {
    peeked: Option<I::Item>,
    iter: I,
}

impl<I: Iterator> RefPeekable<I> {
    pub fn new(mut iter: I) -> RefPeekable<I> {
        RefPeekable {
            peeked: iter.next(),
            iter,
        }
    }
    pub fn peek(&self) -> Option<&I::Item> {
        self.peeked.as_ref()
    }
}

impl<I: Iterator> Iterator for RefPeekable<I> {
    type Item = I::Item;

    #[inline]
    fn next(&mut self) -> Option<I::Item> {
        let result = self.peeked.take();
        self.peeked = self.iter.next();
        result
    }
}
