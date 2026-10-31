use {
	crate::Server,
	futures::{FutureExt as _, Stream, stream::BoxStream},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl tg::instance::Sandbox for Server {
	async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		self.session(&self.context).create_sandbox(arg).await
	}

	async fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		self.session(&self.context)
			.try_get_sandbox(id, arg)
			.boxed()
			.await
	}

	async fn try_get_sandbox_processes_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::sandbox::processes::get::Event>> + Send + 'static>,
	> {
		let session = self.session(&self.context);
		self.components
			.sandbox
			.try_get_sandbox_processes_stream(&session, id, arg)
			.await
	}

	async fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		self.session(&self.context).list_sandboxes(arg).await
	}

	async fn try_destroy_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> tg::Result<Option<bool>> {
		self.session(&self.context)
			.try_destroy_sandbox(id, arg)
			.boxed()
			.await
	}

	async fn try_stop_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::stop::Arg,
	) -> tg::Result<Option<bool>> {
		self.session(&self.context)
			.try_stop_sandbox(id, arg)
			.boxed()
			.await
	}

	async fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static>,
	> {
		let session = self.session(&self.context);
		self.components
			.sandbox
			.try_get_sandbox_status_stream(&session, id, arg)
			.await
	}

	async fn try_wait_sandbox_future(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> tg::Result<
		Option<
			impl Future<Output = tg::Result<Option<tg::sandbox::outcome::Data>>> + Send + 'static,
		>,
	> {
		let session = self.session(&self.context);
		self.components
			.sandbox
			.try_wait_sandbox_future(&session, id, arg)
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
		let session = self.session(&self.context);
		self.components
			.sandbox
			.get_sandbox_control_stream(&session, arg, stream)
			.await
	}
}
