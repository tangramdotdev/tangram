use tangram_either::Either;

pub trait IteratorExt: Iterator {
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

	fn boxed<'a>(self) -> Box<dyn Iterator<Item = Self::Item> + 'a>
	where
		Self: Sized + 'a,
	{
		Box::new(self)
	}
}

impl<T> IteratorExt for T where T: Iterator + ?Sized {}
