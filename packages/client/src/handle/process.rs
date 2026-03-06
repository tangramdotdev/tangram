use {crate::prelude::*, futures::Stream, tokio::io::AsyncRead};

pub trait Process: Clone + Unpin + Send + Sync + 'static {
	fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> impl Future<Output = tg::Result<tg::process::list::Output>> + Send;

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

	fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::Metadata>>> + Send;

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
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_dequeue_process(
		&self,
		arg: tg::process::queue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::queue::Output>>> + Send;

	fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

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

	fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
		>,
	> + Send;

	fn post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_process_pty_size_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::pty::size::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::pty::size::get::Event>> + Send + 'static,
			>,
		>,
	> + Send;

	fn set_process_pty_size(
		&self,
		id: &tg::process::Id,
		arg: tg::process::pty::size::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_read_process_stdin(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<Option<impl AsyncRead + Send + 'static>>> + Send;

	fn write_process_stdin(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_read_process_stdout(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<Option<impl AsyncRead + Send + 'static>>> + Send;

	fn write_process_stdout(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_read_process_stderr(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<Option<impl AsyncRead + Send + 'static>>> + Send;

	fn write_process_stderr(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn close_process_stdin(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn close_process_stdout(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn close_process_stderr(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::process::heartbeat::Output>> + Send;

	fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

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

	fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.cancel_process(id, arg)
	}

	fn try_dequeue_process(
		&self,
		arg: tg::process::queue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::queue::Output>>> {
		self.try_dequeue_process(arg)
	}

	fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.post_process_signal(id, arg)
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

	fn try_get_process_log_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::log::get::Event>> + Send + 'static>,
		>,
	> {
		self.try_get_process_log_stream(id, arg)
	}

	fn post_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.post_process_log(id, arg)
	}

	fn try_get_process_pty_size_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::pty::size::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::pty::size::get::Event>> + Send + 'static,
			>,
		>,
	> {
		self.try_get_process_pty_size_stream(id, arg)
	}

	fn set_process_pty_size(
		&self,
		id: &tg::process::Id,
		arg: tg::process::pty::size::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.set_process_pty_size(id, arg)
	}

	fn try_read_process_stdin(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<Option<impl AsyncRead + Send + 'static>>> {
		self.try_read_process_stdin(id, arg)
	}

	fn write_process_stdin(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		self.write_process_stdin(id, arg, reader)
	}

	fn try_read_process_stdout(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<Option<impl AsyncRead + Send + 'static>>> {
		self.try_read_process_stdout(id, arg)
	}

	fn write_process_stdout(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		self.write_process_stdout(id, arg, reader)
	}

	fn try_read_process_stderr(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<Option<impl AsyncRead + Send + 'static>>> {
		self.try_read_process_stderr(id, arg)
	}

	fn write_process_stderr(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		self.write_process_stderr(id, arg, reader)
	}

	fn close_process_stdin(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.close_process_stdin(id, arg)
	}

	fn close_process_stdout(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.close_process_stdout(id, arg)
	}

	fn close_process_stderr(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.close_process_stderr(id, arg)
	}

	fn heartbeat_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::process::heartbeat::Output>> {
		self.heartbeat_process(id, arg)
	}

	fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.touch_process(id, arg)
	}

	fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.finish_process(id, arg)
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
