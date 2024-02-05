#[allow(clippy::module_name_repetitions)]
pub trait IterExt: Iterator {
	fn map_err<F, T, E1, E2>(self, mut f: F) -> impl Iterator<Item = Result<T, E2>>
	where
		Self: Iterator<Item = Result<T, E1>> + Sized,
		F: FnMut(E1) -> E2,
	{
		self.map(move |result| match result {
			Ok(value) => Ok(value),
			Err(error) => Err(f(error)),
		})
	}

	fn and_then<F, T1, T2, E>(self, mut f: F) -> impl Iterator<Item = Result<T2, E>>
	where
		Self: Iterator<Item = Result<T1, E>> + Sized,
		F: FnMut(T1) -> Result<T2, E>,
	{
		self.map(move |result| match result {
			Ok(value) => f(value),
			Err(error) => Err(error),
		})
	}
}

impl<T> IterExt for T where T: Iterator + ?Sized {}
