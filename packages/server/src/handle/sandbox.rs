use {crate::Handle, futures::Stream, tangram_client::prelude::*};

impl tg::handle::Sandbox for Handle {
	async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		self.create_sandbox(arg).await
	}

	async fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		self.try_get_sandbox(id, arg).await
	}

	async fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		self.try_dequeue_sandbox(arg).await
	}

	async fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		self.list_sandboxes(arg).await
	}

	async fn try_delete_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<Option<()>> {
		self.try_delete_sandbox(id).await
	}

	async fn try_finish_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> tg::Result<Option<bool>> {
		self.try_finish_sandbox(id, arg).await
	}

	async fn try_heartbeat_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> tg::Result<Option<tg::sandbox::heartbeat::Output>> {
		self.try_heartbeat_sandbox(id, arg).await
	}

	async fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static>,
	> {
		self.try_get_sandbox_status_stream(id, arg).await
	}

	async fn try_dequeue_sandbox_process(
		&self,
		sandbox: &tg::sandbox::Id,
		arg: tg::sandbox::process::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		self.try_dequeue_sandbox_process(sandbox, arg).await
	}
}
