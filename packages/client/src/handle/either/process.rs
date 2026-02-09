use {
	crate::prelude::*,
	futures::{FutureExt as _, Stream, TryFutureExt as _},
};

impl<L, R> tg::handle::Process for tg::Either<L, R>
where
	L: tg::handle::Process,
	R: tg::handle::Process,
{
	fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> impl Future<Output = tg::Result<tg::process::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_processes(arg).left_future(),
			tg::Either::Right(s) => s.list_processes(arg).right_future(),
		}
	}

	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::Metadata>>> {
		match self {
			tg::Either::Left(s) => s.try_get_process_metadata(id, arg).left_future(),
			tg::Either::Right(s) => s.try_get_process_metadata(id, arg).right_future(),
		}
	}

	fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_get_process(id, arg).left_future(),
			tg::Either::Right(s) => s.try_get_process(id, arg).right_future(),
		}
	}

	fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.put_process(id, arg).left_future(),
			tg::Either::Right(s) => s.put_process(id, arg).right_future(),
		}
	}

	fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static,
			>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_get_process_children_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			tg::Either::Right(s) => s
				.try_get_process_children_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_get_process_log_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			tg::Either::Right(s) => s
				.try_get_process_log_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + 'static,
			>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_get_process_signal_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			tg::Either::Right(s) => s
				.try_get_process_signal_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_get_process_status_stream(id, arg.clone())
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			tg::Either::Right(s) => s
				.try_get_process_status_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.cancel_process(id, arg).left_future(),
			tg::Either::Right(s) => s.cancel_process(id, arg).right_future(),
		}
	}

	fn try_dequeue_process(
		&self,
		arg: tg::process::queue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::queue::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_dequeue_process(arg).left_future(),
			tg::Either::Right(s) => s.try_dequeue_process(arg).right_future(),
		}
	}

	fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.finish_process(id, arg).left_future(),
			tg::Either::Right(s) => s.finish_process(id, arg).right_future(),
		}
	}

	fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::process::heartbeat::Output>> {
		match self {
			tg::Either::Left(s) => s.heartbeat_process(id, arg).left_future(),
			tg::Either::Right(s) => s.heartbeat_process(id, arg).right_future(),
		}
	}

	fn post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.post_process_log(id, arg).left_future(),
			tg::Either::Right(s) => s.post_process_log(id, arg).right_future(),
		}
	}

	fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.signal_process(id, arg).left_future(),
			tg::Either::Right(s) => s.signal_process(id, arg).right_future(),
		}
	}

	fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>
			+ Send
			+ 'static,
		>,
	> + Send {
		match self {
			tg::Either::Left(s) => s
				.try_spawn_process(arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.try_spawn_process(arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn start_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::start::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.start_process(id, arg).left_future(),
			tg::Either::Right(s) => s.start_process(id, arg).right_future(),
		}
	}

	fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.touch_process(id, arg).left_future(),
			tg::Either::Right(s) => s.touch_process(id, arg).right_future(),
		}
	}

	fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
			>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_wait_process_future(id, arg.clone())
				.map_ok(|option| option.map(futures::FutureExt::left_future))
				.left_future(),
			tg::Either::Right(s) => s
				.try_wait_process_future(id, arg)
				.map_ok(|option| option.map(futures::FutureExt::right_future))
				.right_future(),
		}
	}
}
