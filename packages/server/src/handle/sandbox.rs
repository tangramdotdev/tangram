use {
	crate::{Context, Server, Shared},
	futures::Stream,
	tangram_client::prelude::*,
};

impl tg::handle::Sandbox for Shared {
	async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		self.0.create_sandbox(arg).await
	}

	async fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		self.0.try_get_sandbox(id, arg).await
	}

	async fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		self.0.try_dequeue_sandbox(arg).await
	}

	async fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		self.0.list_sandboxes(arg).await
	}

	async fn delete_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<()> {
		self.0.delete_sandbox(id).await
	}

	async fn finish_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> tg::Result<()> {
		self.0.finish_sandbox(id, arg).await
	}

	async fn heartbeat_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> tg::Result<tg::sandbox::heartbeat::Output> {
		self.0.heartbeat_sandbox(id, arg).await
	}

	async fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static>,
	> {
		self.0.try_get_sandbox_status_stream(id, arg).await
	}
}

impl tg::handle::Sandbox for Server {
	async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		self.create_sandbox_with_context(&Context::default(), arg)
			.await
	}

	async fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		self.try_get_sandbox_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		self.try_dequeue_sandbox_with_context(&Context::default(), arg)
			.await
	}

	async fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		self.list_sandboxes_with_context(&Context::default(), arg)
			.await
	}

	async fn delete_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<()> {
		self.delete_sandbox_with_context(&Context::default(), id)
			.await
	}

	async fn finish_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> tg::Result<()> {
		self.finish_sandbox_with_context(&Context::default(), id, arg)
			.await
	}

	async fn heartbeat_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> tg::Result<tg::sandbox::heartbeat::Output> {
		self.heartbeat_sandbox_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static>,
	> {
		self.try_get_sandbox_status_stream_with_context(&Context::default(), id, arg)
			.await
	}
}
