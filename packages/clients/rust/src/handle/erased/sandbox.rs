use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
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

	fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::sandbox::queue::Output>>>;

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::sandbox::list::Output>>;

	fn try_delete_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_finish_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
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
	) -> BoxFuture<
		'a,
		tg::Result<
			Option<
				std::pin::Pin<
					Box<
						dyn futures::Stream<Item = tg::Result<tg::sandbox::status::Event>>
							+ Send
							+ 'static,
					>,
				>,
			>,
		>,
	>;

	fn try_dequeue_sandbox_process<'a>(
		&'a self,
		sandbox: &'a tg::sandbox::Id,
		arg: tg::sandbox::process::queue::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::sandbox::process::queue::Output>>>;
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

	fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::sandbox::queue::Output>>> {
		self.try_dequeue_sandbox(arg).boxed()
	}

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::sandbox::list::Output>> {
		self.list_sandboxes(arg).boxed()
	}

	fn try_delete_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_sandbox(id).boxed()
	}

	fn try_finish_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> BoxFuture<'a, tg::Result<Option<bool>>> {
		self.try_finish_sandbox(id, arg).boxed()
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
	) -> BoxFuture<
		'a,
		tg::Result<
			Option<
				std::pin::Pin<
					Box<
						dyn futures::Stream<Item = tg::Result<tg::sandbox::status::Event>>
							+ Send
							+ 'static,
					>,
				>,
			>,
		>,
	> {
		async move {
			Ok(self
				.try_get_sandbox_status_stream(id, arg)
				.await?
				.map(|stream| Box::pin(stream) as _))
		}
		.boxed()
	}

	fn try_dequeue_sandbox_process<'a>(
		&'a self,
		sandbox: &'a tg::sandbox::Id,
		arg: tg::sandbox::process::queue::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::sandbox::process::queue::Output>>> {
		self.try_dequeue_sandbox_process(sandbox, arg).boxed()
	}
}
