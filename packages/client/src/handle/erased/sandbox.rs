use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Sandbox: Send + Sync + 'static {
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::sandbox::create::Output>>;

	fn delete_sandbox<'a>(&'a self, id: &'a tg::sandbox::Id) -> BoxFuture<'a, tg::Result<()>>;

	fn sandbox_spawn<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::spawn::Arg,
	) -> BoxFuture<'a, tg::Result<tg::sandbox::spawn::Output>>;

	fn try_sandbox_wait_future<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::sandbox::wait::Output>>>>>,
	>;
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

	fn delete_sandbox<'a>(&'a self, id: &'a tg::sandbox::Id) -> BoxFuture<'a, tg::Result<()>> {
		self.delete_sandbox(id).boxed()
	}

	fn sandbox_spawn<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::spawn::Arg,
	) -> BoxFuture<'a, tg::Result<tg::sandbox::spawn::Output>> {
		self.sandbox_spawn(id, arg).boxed()
	}

	fn try_sandbox_wait_future<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::sandbox::wait::Output>>>>>,
	> {
		self.try_sandbox_wait_future(id, arg)
			.map_ok(|option| option.map(futures::FutureExt::boxed))
			.boxed()
	}
}
