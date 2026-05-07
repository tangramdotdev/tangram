use {crate::prelude::*, futures::Stream};

pub trait Sandbox: Clone + Unpin + Send + Sync + 'static {
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::create::Output>> + Send;

	fn get_sandbox(
		&self,
		id: &tg::sandbox::Id,
	) -> impl Future<Output = tg::Result<tg::sandbox::get::Output>> + Send {
		let arg = tg::sandbox::get::Arg::default();
		async move {
			self.try_get_sandbox(id, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the sandbox"))
		}
	}

	fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::get::Output>>> + Send;

	fn dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::queue::Output>> + Send {
		async move {
			self.try_dequeue_sandbox(arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the sandbox"))
		}
	}

	fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::queue::Output>>> + Send;

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::list::Output>> + Send;

	fn delete_sandbox(&self, id: &tg::sandbox::Id) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_delete_sandbox(id)
				.await?
				.ok_or_else(|| tg::error!("failed to find the sandbox"))
		}
	}

	fn try_delete_sandbox(
		&self,
		id: &tg::sandbox::Id,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

	fn finish_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			match self.try_finish_sandbox(id, arg).await? {
				Some(true) => Ok(()),
				Some(false) => Err(tg::error!("the sandbox was already finished")),
				None => Err(tg::error!("failed to find the sandbox")),
			}
		}
	}

	fn try_finish_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> impl Future<Output = tg::Result<Option<bool>>> + Send;

	fn heartbeat_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::heartbeat::Output>> + Send {
		async move {
			self.try_heartbeat_sandbox(id, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the sandbox"))
		}
	}

	fn try_heartbeat_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::heartbeat::Output>>> + Send;

	fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static>,
		>,
	> + Send;

	fn dequeue_sandbox_process(
		&self,
		sandbox: &tg::sandbox::Id,
		arg: tg::sandbox::process::queue::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::process::queue::Output>> + Send {
		async move {
			self.try_dequeue_sandbox_process(sandbox, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the sandbox"))
		}
	}

	fn try_dequeue_sandbox_process(
		&self,
		sandbox: &tg::sandbox::Id,
		arg: tg::sandbox::process::queue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::process::queue::Output>>> + Send;
}

impl tg::handle::Sandbox for tg::Client {
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
		self.session(&self.context).try_get_sandbox(id, arg).await
	}

	async fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		self.session(&self.context).try_dequeue_sandbox(arg).await
	}

	async fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		self.session(&self.context).list_sandboxes(arg).await
	}

	async fn try_delete_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<Option<()>> {
		self.session(&self.context).try_delete_sandbox(id).await
	}

	async fn try_finish_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> tg::Result<Option<bool>> {
		self.session(&self.context)
			.try_finish_sandbox(id, arg)
			.await
	}

	async fn try_heartbeat_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> tg::Result<Option<tg::sandbox::heartbeat::Output>> {
		self.session(&self.context)
			.try_heartbeat_sandbox(id, arg)
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

	async fn try_dequeue_sandbox_process(
		&self,
		sandbox: &tg::sandbox::Id,
		arg: tg::sandbox::process::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		self.session(&self.context)
			.try_dequeue_sandbox_process(sandbox, arg)
			.await
	}
}
