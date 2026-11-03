use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*, stream::BoxStream},
	std::ops::ControlFlow,
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

	fn try_get_sandbox_processes_stream<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>>,
	>;

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::sandbox::list::Output>>;

	fn try_destroy_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> BoxFuture<'a, tg::Result<Option<bool>>>;

	fn try_stop_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::stop::Arg,
	) -> BoxFuture<'a, tg::Result<Option<bool>>>;

	fn try_get_sandbox_status_stream<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>>>;

	fn try_wait_sandbox_future<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::sandbox::outcome::Data>>>>>,
	>;

	fn get_sandbox_control_stream<'a>(
		&'a self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				(
					tg::sandbox::control::Header,
					BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
				),
				tg::Error,
			>,
		>,
	>;
}

impl<T> Sandbox for T
where
	T: tg::instance::Sandbox,
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

	fn try_get_sandbox_processes_stream<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>>,
	> {
		async move {
			Ok(self
				.try_get_sandbox_processes_stream(id, arg)
				.await?
				.map(futures::StreamExt::boxed))
		}
		.boxed()
	}

	fn try_wait_sandbox_future<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::sandbox::outcome::Data>>>>>,
	> {
		self.try_wait_sandbox_future(id, arg)
			.map_ok(|option| option.map(futures::FutureExt::boxed))
			.boxed()
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

	fn try_stop_sandbox<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		arg: tg::sandbox::stop::Arg,
	) -> BoxFuture<'a, tg::Result<Option<bool>>> {
		self.try_stop_sandbox(id, arg).boxed()
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

	fn get_sandbox_control_stream<'a>(
		&'a self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<
				(
					tg::sandbox::control::Header,
					BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
				),
				tg::Error,
			>,
		>,
	> {
		self.get_sandbox_control_stream(arg, stream)
			.map_ok(|flow| flow.map_break(|(header, stream)| (header, stream.boxed())))
			.boxed()
	}
}
