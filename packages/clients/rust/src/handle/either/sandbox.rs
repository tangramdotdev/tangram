use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Sandbox for tg::Either<L, R>
where
	L: tg::handle::Sandbox,
	R: tg::handle::Sandbox,
{
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::create::Output>> {
		match self {
			tg::Either::Left(s) => s.create_sandbox(arg).left_future(),
			tg::Either::Right(s) => s.create_sandbox(arg).right_future(),
		}
	}

	fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::get::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_get_sandbox(id, arg).left_future(),
			tg::Either::Right(s) => s.try_get_sandbox(id, arg).right_future(),
		}
	}

	fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::queue::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_dequeue_sandbox(arg).left_future(),
			tg::Either::Right(s) => s.try_dequeue_sandbox(arg).right_future(),
		}
	}

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_sandboxes(arg).left_future(),
			tg::Either::Right(s) => s.list_sandboxes(arg).right_future(),
		}
	}

	fn delete_sandbox(&self, id: &tg::sandbox::Id) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.delete_sandbox(id).left_future(),
			tg::Either::Right(s) => s.delete_sandbox(id).right_future(),
		}
	}

	fn finish_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.finish_sandbox(id, arg).left_future(),
			tg::Either::Right(s) => s.finish_sandbox(id, arg).right_future(),
		}
	}

	fn heartbeat_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::heartbeat::Output>> {
		match self {
			tg::Either::Left(s) => s.heartbeat_sandbox(id, arg).left_future(),
			tg::Either::Right(s) => s.heartbeat_sandbox(id, arg).right_future(),
		}
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
		match self {
			tg::Either::Left(s) => s
				.try_get_sandbox_status_stream(id, arg.clone())
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			tg::Either::Right(s) => s
				.try_get_sandbox_status_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}
}
