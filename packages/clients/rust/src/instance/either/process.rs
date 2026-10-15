use {
	crate::prelude::*,
	futures::{FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, stream::BoxStream},
};

impl<L, R> tg::instance::Process for tg::Either<L, R>
where
	L: tg::instance::Process,
	R: tg::instance::Process,
{
	async fn try_get_process_connect_stream(
		&self,
		arg: tg::process::connect::Arg,
		input: BoxStream<'static, tg::Result<tg::process::connect::ClientMessage>>,
	) -> tg::Result<
		Option<(
			tg::process::connect::Header,
			BoxStream<'static, tg::Result<tg::process::connect::ServerMessage>>,
		)>,
	> {
		match self {
			tg::Either::Left(instance) => instance.try_get_process_connect_stream(arg, input).await,
			tg::Either::Right(instance) => {
				instance.try_get_process_connect_stream(arg, input).await
			},
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

	fn try_get_process_availability(
		&self,
		id: &tg::process::Id,
		arg: tg::process::availability::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::Availability>>> {
		match self {
			tg::Either::Left(s) => s.try_get_process_availability(id, arg).left_future(),
			tg::Either::Right(s) => s.try_get_process_availability(id, arg).right_future(),
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
	) -> impl Future<Output = tg::Result<tg::process::put::Output>> {
		match self {
			tg::Either::Left(s) => s.put_process(id, arg).left_future(),
			tg::Either::Right(s) => s.put_process(id, arg).right_future(),
		}
	}

	fn try_cancel_process(
		&self,
		arg: tg::process::cancel::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::cancel::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_cancel_process(arg).left_future(),
			tg::Either::Right(s) => s.try_cancel_process(arg).right_future(),
		}
	}

	fn try_get_process_control_stream(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<
			Option<(
				tg::process::control::Header,
				impl Stream<Item = tg::Result<tg::process::control::ServerMessage>> + Send + 'static,
			)>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_get_process_control_stream(arg, stream)
				.map_ok(|option| option.map(|(header, stream)| (header, stream.left_stream())))
				.left_future(),
			tg::Either::Right(s) => s
				.try_get_process_control_stream(arg, stream)
				.map_ok(|option| option.map(|(header, stream)| (header, stream.right_stream())))
				.right_future(),
		}
	}

	fn try_signal_process(
		&self,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.try_signal_process(arg).left_future(),
			tg::Either::Right(s) => s.try_signal_process(arg).right_future(),
		}
	}

	fn try_get_process_status_stream(
		&self,
		arg: tg::process::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_get_process_status_stream(arg.clone())
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			tg::Either::Right(s) => s
				.try_get_process_status_stream(arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn try_get_process_children_stream(
		&self,
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
				.try_get_process_children_stream(arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			tg::Either::Right(s) => s
				.try_get_process_children_stream(arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn try_set_process_tty_size(
		&self,
		arg: tg::process::tty::size::put::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.try_set_process_tty_size(arg).left_future(),
			tg::Either::Right(s) => s.try_set_process_tty_size(arg).right_future(),
		}
	}

	fn try_read_process_stdio(
		&self,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::stdio::read::ServerMessage>> + Send + 'static,
			>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_read_process_stdio(arg, input)
				.map_ok(|option| option.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.try_read_process_stdio(arg, input)
				.map_ok(|option| option.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn try_write_process_stdio(
		&self,
		arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::stdio::write::Output>> + Send + 'static,
			>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_write_process_stdio(arg, input)
				.map_ok(|option| option.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.try_write_process_stdio(arg, input)
				.map_ok(|option| option.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn try_touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.try_touch_process(id, arg).left_future(),
			tg::Either::Right(s) => s.try_touch_process(id, arg).right_future(),
		}
	}

	fn try_wait_process_future(
		&self,
		arg: tg::process::wait::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Future<Output = tg::Result<Option<tg::process::outcome::Data>>> + Send + 'static,
			>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_wait_process_future(arg.clone())
				.map_ok(|option| option.map(futures::FutureExt::left_future))
				.left_future(),
			tg::Either::Right(s) => s
				.try_wait_process_future(arg)
				.map_ok(|option| option.map(futures::FutureExt::right_future))
				.right_future(),
		}
	}
}
