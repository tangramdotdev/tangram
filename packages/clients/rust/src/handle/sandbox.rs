use {crate::prelude::*, futures::Stream};

pub trait Sandbox: Clone + Unpin + Send + Sync + 'static {
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::create::Output>> + Send;

	fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::get::Output>>> + Send;

	fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::queue::Output>>> + Send;

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::list::Output>> + Send;

	fn delete_sandbox(&self, id: &tg::sandbox::Id) -> impl Future<Output = tg::Result<()>> + Send;

	fn finish_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn heartbeat_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::heartbeat::Output>> + Send;

	fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static>,
		>,
	> + Send;

	fn try_dequeue_sandbox_process(
		&self,
		sandbox: &tg::sandbox::Id,
		arg: tg::sandbox::process::queue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::process::queue::Output>>> + Send;
}

impl tg::handle::Sandbox for tg::Client {
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

	fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::queue::Output>>> {
		self.try_dequeue_sandbox(arg)
	}

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::list::Output>> {
		self.list_sandboxes(arg)
	}

	fn delete_sandbox(&self, id: &tg::sandbox::Id) -> impl Future<Output = tg::Result<()>> {
		self.delete_sandbox(id)
	}

	fn finish_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.finish_sandbox(id, arg)
	}

	fn heartbeat_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::heartbeat::Output>> {
		self.heartbeat_sandbox(id, arg)
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

	fn try_dequeue_sandbox_process(
		&self,
		sandbox: &tg::sandbox::Id,
		arg: tg::sandbox::process::queue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::process::queue::Output>>> {
		self.try_dequeue_sandbox_process(sandbox, arg)
	}
}
