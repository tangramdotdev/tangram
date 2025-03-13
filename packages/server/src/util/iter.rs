pub trait Ext: Iterator {
	fn owned_chunks(self, size: usize) -> Chunks<Self>
	where
		Self: Sized,
	{
		Chunks::new(self, size)
	}
}

impl<I> Ext for I where I: Iterator {}

pub struct Chunks<I>
where
	I: Iterator,
{
	iter: I,
	size: usize,
}

impl<I> Chunks<I>
where
	I: Iterator,
{
	fn new(iter: I, size: usize) -> Self {
		assert!(size > 0, "chunk size must be greater than 0");
		Chunks { iter, size }
	}
}

impl<I> Iterator for Chunks<I>
where
	I: Iterator,
{
	type Item = Vec<I::Item>;

	fn next(&mut self) -> Option<Self::Item> {
		let mut chunk = Vec::with_capacity(self.size);
		for _ in 0..self.size {
			match self.iter.next() {
				Some(item) => chunk.push(item),
				None => {
					return if chunk.is_empty() { None } else { Some(chunk) };
				},
			}
		}
		Some(chunk)
	}
}
