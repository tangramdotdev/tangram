use {self::batches::Batches, tangram_either::Either};

mod batches;

pub trait Ext: Iterator {
	fn batches(self, size: usize) -> Batches<Self>
	where
		Self: Sized,
	{
		Batches::new(self, size)
	}

	fn boxed<'a>(self) -> Box<dyn Iterator<Item = Self::Item> + 'a>
	where
		Self: Sized + 'a,
	{
		Box::new(self)
	}

	fn left_iterator<B>(self) -> Either<Self, B>
	where
		B: Iterator<Item = Self::Item>,
		Self: Sized,
	{
		Either::Left(self)
	}

	fn right_iterator<A>(self) -> Either<A, Self>
	where
		A: Iterator<Item = Self::Item>,
		Self: Sized,
	{
		Either::Right(self)
	}
}

impl<T> Ext for T where T: Iterator + ?Sized {}
