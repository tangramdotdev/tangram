use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Grant: Send + Sync + 'static {
	fn create_grant(&self, arg: tg::grant::create::Arg) -> BoxFuture<'_, tg::Result<tg::Grant>>;

	fn delete_grant(&self, arg: tg::grant::delete::Arg) -> BoxFuture<'_, tg::Result<Option<()>>>;
}

impl<T> Grant for T
where
	T: tg::handle::Grant,
{
	fn create_grant(&self, arg: tg::grant::create::Arg) -> BoxFuture<'_, tg::Result<tg::Grant>> {
		self.create_grant(arg).boxed()
	}

	fn delete_grant(&self, arg: tg::grant::delete::Arg) -> BoxFuture<'_, tg::Result<Option<()>>> {
		self.delete_grant(arg).boxed()
	}
}
