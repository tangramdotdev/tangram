use {
	crate::Session,
	futures::{Stream, stream::BoxStream},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl tg::instance::Process for Session {
	async fn try_get_process_attach_stream(
		&self,
		arg: tg::process::attach::Arg,
		input: BoxStream<'static, tg::Result<tg::process::attach::ClientMessage>>,
	) -> tg::Result<
		ControlFlow<
			Option<(
				tg::process::attach::Header,
				BoxStream<'static, tg::Result<tg::process::attach::ServerMessage>>,
			)>,
			tg::Error,
		>,
	> {
		self.server
			.components
			.process
			.try_get_process_attach_stream(self, arg, input)
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
		self.server
			.components
			.process
			.try_spawn_process(self, arg)
			.await
	}

	async fn try_get_process_metadata(
		&self,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		self.server
			.components
			.process
			.try_get_process_metadata(self, id, arg)
			.await
	}

	async fn try_get_process_availability(
		&self,
		id: &tg::process::Id,
		arg: tg::process::availability::Arg,
	) -> tg::Result<Option<tg::process::Availability>> {
		self.server
			.components
			.process
			.try_get_process_availability(self, id, arg)
			.await
	}

	async fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		self.server
			.components
			.process
			.list_processes(self, arg)
			.await
	}

	async fn try_get_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::get::Arg,
	) -> tg::Result<Option<tg::process::get::Output>> {
		self.server
			.components
			.process
			.try_get_process(self, id, arg)
			.await
	}

	async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<tg::process::put::Output> {
		self.server
			.components
			.process
			.put_process(self, id, arg)
			.await
	}

	async fn try_cancel_process(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		self.server
			.components
			.process
			.try_cancel_process(self, process, arg)
			.await
	}

	async fn try_get_process_control_stream(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> tg::Result<
		ControlFlow<
			Option<(
				tg::process::control::Header,
				impl Stream<Item = tg::Result<tg::process::control::ServerMessage>> + Send + 'static,
			)>,
			tg::Error,
		>,
	> {
		self.server
			.components
			.process
			.try_get_process_control_stream(self, arg, stream)
			.await
	}

	async fn try_signal_process(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.process
			.try_signal_process(self, process, arg)
			.await
	}

	async fn try_get_process_status_stream(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
	> {
		self.server
			.components
			.process
			.try_get_process_status_stream(self, process, arg)
			.await
	}

	async fn try_get_process_children_stream(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static>,
	> {
		self.server
			.components
			.process
			.try_get_process_children_stream(self, process, arg)
			.await
	}

	async fn try_set_process_tty_size(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::tty::size::put::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.process
			.try_set_process_tty_size(self, process, arg)
			.await
	}

	async fn try_read_process_stdio(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) -> tg::Result<
		ControlFlow<
			Option<
				impl Stream<Item = tg::Result<tg::process::stdio::read::ServerMessage>> + Send + 'static,
			>,
			tg::Error,
		>,
	> {
		self.server
			.components
			.process
			.try_read_process_stdio(self, process, arg, input)
			.await
	}

	async fn try_write_process_stdio(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
	) -> tg::Result<
		ControlFlow<
			Option<
				impl Stream<Item = tg::Result<tg::process::stdio::write::Output>> + Send + 'static,
			>,
			tg::Error,
		>,
	> {
		self.server
			.components
			.process
			.try_write_process_stdio(self, process, arg, input)
			.await
	}

	async fn try_touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.process
			.try_touch_process(self, id, arg)
			.await
	}

	async fn try_wait_process_future(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::wait::Arg,
	) -> tg::Result<
		Option<
			impl Future<Output = tg::Result<Option<tg::process::outcome::Data>>> + Send + 'static,
		>,
	> {
		self.server
			.components
			.process
			.try_wait_process_future(self, process, arg)
			.await
	}
}
