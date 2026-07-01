use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*, stream::BoxStream},
};

pub trait Sandbox: Send + Sync + 'static {
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::sandbox::create::Output>>;

	fn try_get_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::sandbox::get::Output>>>;

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::sandbox::list::Output>>;

	fn try_destroy_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> BoxFuture<'a, tg::Result<Option<bool>>>;

	fn try_heartbeat_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::sandbox::heartbeat::Output>>>;

	fn try_get_sandbox_status_stream<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>>>;
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

	fn try_get_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::sandbox::get::Output>>> {
		self.try_get_sandbox(id, arg).boxed()
	}

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::sandbox::list::Output>> {
		self.list_sandboxes(arg).boxed()
	}

	fn try_destroy_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> BoxFuture<'a, tg::Result<Option<bool>>> {
		self.try_destroy_sandbox(id, arg).boxed()
	}

	fn try_heartbeat_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::sandbox::heartbeat::Output>>> {
		self.try_heartbeat_sandbox(id, arg).boxed()
	}

	fn try_get_sandbox_status_stream<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>>>
	{
		async move {
			Ok(self
				.try_get_sandbox_status_stream(id, arg)
				.await?
				.map(futures::StreamExt::boxed))
		}
		.boxed()
	}
}
