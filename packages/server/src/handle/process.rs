use {
	crate::{Context, Server, Shared},
	futures::{Stream, stream::BoxStream},
	tangram_client::prelude::*,
};

impl tg::handle::Process for Shared {
	async fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		self.0.list_processes(arg).await
	}

	async fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>
		+ Send
		+ 'static,
	> {
		self.0.try_spawn_process(arg).await
	}

	async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		self.0.try_get_process_metadata(id, arg).await
	}

	async fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		self.0.try_get_process(id, arg).await
	}

	async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
		self.0.put_process(id, arg).await
	}

	async fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<()> {
		self.0.cancel_process(id, arg).await
	}

	async fn try_dequeue_process(
		&self,
		sandbox: &tg::sandbox::Id,
		arg: tg::process::queue::Arg,
	) -> tg::Result<Option<tg::process::queue::Output>> {
		self.0.try_dequeue_process(sandbox, arg).await
	}

	async fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<()> {
		self.0
			.post_process_signal_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + 'static>,
	> {
		self.0.try_get_process_signal_stream(id, arg).await
	}

	async fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
	> {
		self.0.try_get_process_status_stream(id, arg).await
	}

	async fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static>,
	> {
		self.0.try_get_process_children_stream(id, arg).await
	}

	async fn try_get_process_tty_size_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::tty::size::get::Event>> + Send + 'static>,
	> {
		self.0
			.try_get_process_tty_size_stream_with_context(&Context::default(), id, arg)
			.await
	}

	async fn set_process_tty_size(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> tg::Result<()> {
		self.0
			.try_set_process_tty_size_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_read_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static>,
	> {
		self.0
			.try_read_process_stdio_with_context(&Context::default(), id, arg)
			.await
	}

	async fn write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::stdio::write::Event>> + Send + 'static>
	{
		self.0
			.write_process_stdio_with_context(&Context::default(), id, arg, stream, None)
			.await
	}

	async fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> tg::Result<()> {
		self.0.touch_process(id, arg).await
	}

	async fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
		self.0.finish_process(id, arg).await
	}

	async fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<
		Option<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
		>,
	> {
		self.0.try_wait_process_future(id, arg).await
	}
}

impl tg::handle::Process for Server {
	async fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		self.list_processes_with_context(&Context::default(), arg)
			.await
	}

	async fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>
		+ Send
		+ 'static,
	> {
		self.try_spawn_process_with_context(&Context::default(), arg)
			.await
	}

	async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		self.try_get_process_metadata_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		self.try_get_process_with_context(&Context::default(), id, arg)
			.await
	}

	async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
		self.put_process_with_context(&Context::default(), id, arg)
			.await
	}

	async fn cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<()> {
		self.cancel_process_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_dequeue_process(
		&self,
		sandbox: &tg::sandbox::Id,
		arg: tg::process::queue::Arg,
	) -> tg::Result<Option<tg::process::queue::Output>> {
		self.try_dequeue_process_with_context(&Context::default(), sandbox, arg)
			.await
	}

	async fn signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<()> {
		self.post_process_signal_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + 'static>,
	> {
		self.try_get_process_signal_stream_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
	> {
		self.try_get_process_status_stream_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static>,
	> {
		self.try_get_process_children_stream_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_process_tty_size_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::tty::size::get::Event>> + Send + 'static>,
	> {
		self.try_get_process_tty_size_stream_with_context(&Context::default(), id, arg)
			.await
	}

	async fn set_process_tty_size(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> tg::Result<()> {
		self.try_set_process_tty_size_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_read_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static>,
	> {
		self.try_read_process_stdio_with_context(&Context::default(), id, arg)
			.await
	}

	async fn write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::process::stdio::write::Event>> + Send + 'static>
	{
		self.write_process_stdio_with_context(&Context::default(), id, arg, stream, None)
			.await
	}

	async fn touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> tg::Result<()> {
		self.touch_process_with_context(&Context::default(), id, arg)
			.await
	}

	async fn finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<()> {
		self.finish_process_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_wait_process_future(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<
		Option<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
		>,
	> {
		self.try_wait_process_future_with_context(&Context::default(), id, arg)
			.await
	}
}
