use {
	crate::Server,
	futures::{FutureExt as _, Stream, stream::BoxStream},
	tangram_client::prelude::*,
};

impl tg::handle::Sandbox for Server {
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

	async fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static>,
	> {
		self.session(&self.context)
			.try_get_sandbox_status_stream(id, arg)
			.await
	}

	async fn get_sandbox_control_stream(
		&self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> tg::Result<(
		tg::sandbox::control::Output,
		impl Stream<Item = tg::Result<tg::sandbox::control::ServerMessage>> + Send + 'static,
	)> {
		self.session(&self.context)
			.get_sandbox_control_stream_with_context(arg, stream)
			.await
	}
}
