use {
	crate::prelude::*,
	futures::{Stream, stream::BoxStream},
};

impl tg::handle::Sandbox for tg::Session {
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::create::Output>> {
		self.create_sandbox(arg)
	}

	fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::get::Output>>> {
		self.try_get_sandbox(id, arg)
	}

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::list::Output>> {
		self.list_sandboxes(arg)
	}

	fn try_destroy_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> impl Future<Output = tg::Result<Option<bool>>> {
		self.try_destroy_sandbox(id, arg)
	}

	fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_sandbox_status_stream(id, arg)
	}

	fn get_sandbox_control_stream(
		&self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<(
			tg::sandbox::control::Output,
			impl Stream<Item = tg::Result<tg::sandbox::control::ServerMessage>> + Send + 'static,
		)>,
	> {
		self.get_sandbox_control_stream(arg, stream)
	}
}
