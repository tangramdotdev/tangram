use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Sandbox: Send + Sync + 'static {
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::sandbox::create::Output>>;

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::sandbox::list::Output>>;

	fn delete_sandbox<'a>(&'a self, id: &'a tg::sandbox::Id) -> BoxFuture<'a, tg::Result<()>>;
}

impl<T> Sandbox for T
where
	T: tg::handle::Sandbox,
{
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::sandbox::create::Output>> {
		self.create_sandbox(arg).boxed()
	}

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::sandbox::list::Output>> {
		self.list_sandboxes(arg).boxed()
	}

	fn delete_sandbox<'a>(&'a self, id: &'a tg::sandbox::Id) -> BoxFuture<'a, tg::Result<()>> {
		self.delete_sandbox(id).boxed()
	}
}
