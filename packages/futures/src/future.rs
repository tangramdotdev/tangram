use crate::attach::Attach;

pub trait Ext: Future {
	fn attach<T>(self, value: T) -> Attach<Self, T>
	where
		Self: Sized,
	{
		Attach::new(self, value)
	}
}

impl<F> Ext for F where F: Future {}
