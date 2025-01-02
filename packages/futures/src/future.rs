use crate::attach::Attach;
use futures::Future;

pub trait Ext: Future {
	fn attach<T>(self, value: T) -> Attach<Self, T>
	where
		Self: Sized,
	{
		Attach::new(self, value)
	}
}
