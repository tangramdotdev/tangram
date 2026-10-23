use {
	crate::Session,
	futures::{Stream, stream::BoxStream},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl tg::instance::Sandbox for Session {
	async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		self.server
			.components
			.sandbox
			.create_sandbox(self, arg)
			.await
	}

	async fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		self.server
			.components
			.sandbox
			.try_get_sandbox(self, id, arg)
			.await
	}

	async fn try_get_sandbox_processes_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::sandbox::processes::get::Event>> + Send + 'static>,
	> {
		self.server
			.components
			.sandbox
			.try_get_sandbox_processes_stream(self, id, arg)
			.await
	}

	async fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		self.server
			.components
			.sandbox
			.list_sandboxes(self, arg)
			.await
	}

	async fn try_destroy_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> tg::Result<Option<bool>> {
		self.server
			.components
			.sandbox
			.try_destroy_sandbox(self, id, arg)
			.await
	}

	async fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static>,
	> {
		self.server
			.components
			.sandbox
			.try_get_sandbox_status_stream(self, id, arg)
			.await
	}

	async fn get_sandbox_control_stream(
		&self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::sandbox::control::Header,
				impl Stream<Item = tg::Result<tg::sandbox::control::ServerMessage>> + Send + 'static,
			),
			tg::Error,
		>,
	> {
		self.server
			.components
			.sandbox
			.get_sandbox_control_stream(self, arg, stream)
			.await
	}
}
