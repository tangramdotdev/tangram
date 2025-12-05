pub struct AndThen<I, F> {
	iter: I,
	f: F,
}

impl<I, F> AndThen<I, F> {
	pub fn new(iter: I, f: F) -> Self {
		Self { iter, f }
	}
}

impl<I, T, E, U, F> Iterator for AndThen<I, F>
where
	I: Iterator<Item = Result<T, E>>,
	F: FnMut(T) -> Result<U, E>,
{
	type Item = Result<U, E>;

	fn next(&mut self) -> Option<Self::Item> {
		self.iter.next().map(|r| r.and_then(&mut self.f))
	}
}
