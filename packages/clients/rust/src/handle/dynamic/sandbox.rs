use {
	super::Handle,
	crate::prelude::*,
	futures::{future::BoxFuture, stream::BoxStream},
};

impl tg::handle::Sandbox for Handle {
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::create::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.create_sandbox(arg)) }
	}

	fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::get::Output>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_sandbox(id, arg)) }
	}

	fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::queue::Output>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_dequeue_sandbox(arg)) }
	}

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::list::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.list_sandboxes(arg)) }
	}

	fn delete_sandbox(&self, id: &tg::sandbox::Id) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.delete_sandbox(id)) }
	}

	fn finish_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.finish_sandbox(id, arg)) }
	}

	fn heartbeat_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::heartbeat::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.heartbeat_sandbox(id, arg)) }
	}

	fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl futures::Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static,
			>,
		>,
	> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_get_sandbox_status_stream(id, arg),
			)
		}
	}
}
