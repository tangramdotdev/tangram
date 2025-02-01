use std::{ops::Deref, sync::Arc};

pub trait Ext: Sized {
	fn map<F, U>(self, f: F) -> Map<Self, F, U>
	where
		F: Fn(&Self) -> &U,
	{
		Map::new(self, f)
	}
}

impl<T> Ext for Arc<T> {
	fn map<F, U>(self, f: F) -> Map<Self, F, U>
	where
		F: Fn(&Self) -> &U,
	{
		Map::new(self, f)
	}
}

pub struct Map<T, F, U>
where
	F: Fn(&T) -> &U,
{
	value: T,
	f: F,
}

impl<T, F, U> Map<T, F, U>
where
	F: Fn(&T) -> &U,
{
	pub fn new(value: T, f: F) -> Self {
		Self { value, f }
	}
}

impl<T, F, U> Deref for Map<T, F, U>
where
	F: Fn(&T) -> &U,
{
	type Target = U;

	fn deref(&self) -> &Self::Target {
		(self.f)(&self.value)
	}
}

impl<T, F, U> AsRef<U> for Map<T, F, U>
where
	F: Fn(&T) -> &U,
{
	fn as_ref(&self) -> &U {
		(self.f)(&self.value)
	}
}
