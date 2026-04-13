use {
	crate::prelude::*,
	futures::{Stream, StreamExt as _, stream::BoxStream},
};

pub trait Process: Clone + Unpin + Send + Sync + 'static {
	fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> impl Future<Output = tg::Result<tg::process::list::Output>> + Send;

	fn spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::process::spawn::Output>>>
			+ Send
			+ 'static,
		>,
	> {
		async move {
			let stream = self.try_spawn_process(arg).await?;
			let stream = stream.map(|event_result| {
				event_result.and_then(|event| {
					event
						.try_map_output(|item| item.ok_or_else(|| tg::error!("expected a process")))
				})
			});
			Ok(stream)
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
	> + Send;

	fn get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<tg::process::Metadata>> + Send {
		let arg = tg::process::metadata::Arg::default();
		async move {
			self.try_get_process_metadata(id, arg)
				.await?
				.ok_or_else(|| tg::error!(?id, "failed to find the process"))
		}
	}

	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::Metadata>>> + Send;

	fn get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<tg::process::get::Output>> + Send {
		let arg = tg::process::get::Arg::default();
		async move {
			self.try_get_process(id, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the process"))
		}
	}

	fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> + Send;

	fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_cancel_process(id, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the process"))
		}
	}

	fn try_cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

	fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_signal_process(id, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the process"))
		}
	}

	fn try_signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

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
	> + Send;

	fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> + Send;

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
	> + Send;

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
	> + Send;

	fn set_process_tty_size(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_set_process_tty_size(id, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the process"))
		}
	}

	fn try_set_process_tty_size(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

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
	> + Send;

	fn write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::process::stdio::write::Event>> + Send + 'static,
		>,
	> + Send {
		async move {
			self.try_write_process_stdio(id, arg, stream)
				.await?
				.ok_or_else(|| tg::error!("failed to find the process"))
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
	> + Send;

	fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_touch_process(id, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the process"))
		}
	}

	fn try_touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

	fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_finish_process(id, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the process"))
		}
	}

	fn try_finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

	fn wait_process_future(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
		>,
	> + Send {
		async move {
			self.try_wait_process_future(id, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the process"))
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
	> + Send;
}

impl tg::handle::Process for tg::Client {
	fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> impl Future<Output = tg::Result<tg::process::list::Output>> {
		self.list_processes(arg)
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
		self.try_spawn_process(arg)
	}

	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::Metadata>>> {
		self.try_get_process_metadata(id, arg)
	}

	fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::get::Output>>> {
		self.try_get_process(id, arg)
	}

	fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_process(id, arg)
	}

	fn try_cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_cancel_process(id, arg)
	}

	fn try_signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_post_process_signal(id, arg)
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
		self.try_get_process_signal_stream(id, arg)
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
		self.try_get_process_status_stream(id, arg)
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
		self.try_get_process_children_stream(id, arg)
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
		self.try_get_process_tty_size_stream(id, arg)
	}

	fn try_set_process_tty_size(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_set_process_tty_size(id, arg)
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
		self.try_read_process_stdio(id, arg)
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
		self.try_write_process_stdio(id, arg, stream)
	}

	fn try_touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_touch_process(id, arg)
	}

	fn try_finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_finish_process(id, arg)
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
		self.try_wait_process_future(id, arg)
	}
}
