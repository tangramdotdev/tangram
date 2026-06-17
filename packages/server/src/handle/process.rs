use {
	crate::Server,
	futures::{Stream, stream::BoxStream},
	tangram_client::prelude::*,
};

impl tg::handle::Process for Server {
	async fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		self.session(&self.context).list_processes(arg).await
	}

	async fn try_spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::process::spawn::Output>>>>
		+ Send
		+ 'static,
	> {
		self.session(&self.context).try_spawn_process(arg).await
	}

	async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		self.session(&self.context)
			.try_get_process_metadata(id, arg)
			.await
	}

	async fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		self.session(&self.context).try_get_process(id, arg).await
	}

	async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<tg::process::put::Output> {
		self.session(&self.context).put_process(id, arg).await
	}

	async fn try_cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_cancel_process(id, arg)
			.await
	}

	async fn try_get_process_control_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ResponseEvent>>,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::control::RequestEvent>> + Send + 'static>,
	> {
		self.session(&self.context)
			.try_get_process_control_stream_with_context(id, arg, stream)
			.await
	}

	async fn try_signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_post_process_signal(id, arg)
			.await
	}

	async fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
	> {
		self.session(&self.context)
			.try_get_process_status_stream(id, arg)
			.await
	}

	async fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static>,
	> {
		self.session(&self.context)
			.try_get_process_children_stream(id, arg)
			.await
	}

	async fn try_set_process_tty_size(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_set_process_tty_size(id, arg)
			.await
	}

	async fn try_read_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static>,
	> {
		self.session(&self.context)
			.try_read_process_stdio(id, arg)
			.await
	}

	async fn try_write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::stdio::write::Event>> + Send + 'static>,
	> {
		self.session(&self.context)
			.try_write_process_stdio(id, arg, stream)
			.await
	}

	async fn try_touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context).try_touch_process(id, arg).await
	}

	async fn try_finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<Option<bool>> {
		self.session(&self.context)
			.try_finish_process(id, arg)
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
		self.session(&self.context)
			.try_wait_process_future(id, arg)
			.await
	}
}
