use {
	super::Handle,
	crate::prelude::*,
	futures::{Stream, future::BoxFuture, stream::BoxStream},
};

impl tg::handle::Process for Handle {
	fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> impl Future<Output = tg::Result<tg::process::list::Output>> {
		self.0.list_processes(arg)
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
		self.0.try_spawn_process(arg)
	}

	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::Metadata>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_process_metadata(id, arg))
		}
	}

	fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_process(id, arg)) }
	}

	fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.put_process(id, arg)) }
	}

	fn try_cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_cancel_process(id, arg)) }
	}

	fn try_signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_signal_process(id, arg)) }
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
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_get_process_signal_stream(id, arg),
			)
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
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_get_process_status_stream(id, arg),
			)
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
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_get_process_children_stream(id, arg),
			)
		}
	}

	fn try_get_process_tty_size_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::tty::size::get::Event>> + Send + 'static,
			>,
		>,
	> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_get_process_tty_size_stream(id, arg),
			)
		}
	}

	fn try_set_process_tty_size(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_set_process_tty_size(id, arg))
		}
	}

	fn try_read_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static,
			>,
		>,
	> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_read_process_stdio(id, arg),
			)
		}
	}

	fn try_write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::stdio::write::Event>> + Send + 'static,
			>,
		>,
	> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_write_process_stdio(id, arg, stream),
			)
		}
	}

	fn try_touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_touch_process(id, arg)) }
	}

	fn try_finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<Option<bool>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_finish_process(id, arg)) }
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
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxFuture<_>>>>>(
				self.0.try_wait_process_future(id, arg),
			)
		}
	}
}
