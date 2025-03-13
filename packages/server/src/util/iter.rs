pub trait Ext: Iterator {
	fn batches(self, size: usize) -> Batches<Self>
	where
		Self: Sized,
	{
		Batches::new(self, size)
	}
}

impl<I> Ext for I where I: Iterator {}

pub struct Batches<I>
where
	I: Iterator,
{
	iter: I,
	size: usize,
}

impl<I> Batches<I>
where
	I: Iterator,
{
	fn new(iter: I, size: usize) -> Self {
		assert!(size > 0, "batch size must be greater than 0");
		Batches { iter, size }
	}
}

impl<I> Iterator for Batches<I>
where
	I: Iterator,
{
	type Item = Vec<I::Item>;

	fn next(&mut self) -> Option<Self::Item> {
		let mut batch = Vec::with_capacity(self.size);
		for _ in 0..self.size {
			match self.iter.next() {
				Some(item) => batch.push(item),
				None => {
					return if batch.is_empty() { None } else { Some(batch) };
				},
			}
		}
		Some(batch)
	}
}
