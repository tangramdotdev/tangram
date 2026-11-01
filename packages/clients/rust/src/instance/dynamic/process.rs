use {
	super::Instance,
	crate::prelude::*,
	futures::{FutureExt as _, Stream, future::BoxFuture, stream::BoxStream},
	std::ops::ControlFlow,
};

impl Instance {
	async fn try_cancel_process_inner(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<tg::process::cancel::Output>> {
		if let Some(connection) = process
			.as_ref()
			.right()
			.and_then(|id| self.try_connection(id))
		{
			let output = connection
				.request(tg::process::attach::ClientRequestArg::Cancel(arg))
				.await?;
			let tg::process::attach::ServerResponseOutput::Cancel(output) = output else {
				return Err(tg::error!("expected a cancel response"));
			};
			return Ok(Some(output));
		}
		self.0.try_cancel_process(process, arg).await
	}

	async fn try_signal_process_inner(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::signal::post::Arg,
	) -> tg::Result<Option<()>> {
		if let Some(connection) = process
			.as_ref()
			.right()
			.and_then(|id| self.try_connection(id))
		{
			let output = connection
				.request(tg::process::attach::ClientRequestArg::Signal(arg))
				.await?;
			if !matches!(output, tg::process::attach::ServerResponseOutput::Signal) {
				return Err(tg::error!("expected a signal response"));
			}
			return Ok(Some(()));
		}
		self.0.try_signal_process(process, arg).await
	}

	async fn try_set_process_tty_size_inner(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::tty::size::put::Arg,
	) -> tg::Result<Option<()>> {
		if let Some(connection) = process
			.as_ref()
			.right()
			.and_then(|id| self.try_connection(id))
		{
			let output = connection
				.request(tg::process::attach::ClientRequestArg::Tty(arg))
				.await?;
			if !matches!(output, tg::process::attach::ServerResponseOutput::Tty) {
				return Err(tg::error!("expected a tty response"));
			}
			return Ok(Some(()));
		}
		self.0.try_set_process_tty_size(process, arg).await
	}

	async fn try_read_process_stdio_inner(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) -> tg::Result<
		ControlFlow<
			Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>,
			tg::Error,
		>,
	> {
		self.0.try_read_process_stdio(process, arg, input).await
	}

	async fn try_write_process_stdio_inner(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
	) -> tg::Result<
		ControlFlow<
			Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Output>>>,
			tg::Error,
		>,
	> {
		self.0.try_write_process_stdio(process, arg, input).await
	}

	async fn try_wait_process_future_inner(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::wait::Arg,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::outcome::Data>>>>> {
		if let Some(connection) = process
			.as_ref()
			.right()
			.and_then(|id| self.try_connection(id))
		{
			let connection = connection.clone();
			let future = async move { connection.wait().await.map(Some) }.boxed();
			return Ok(Some(future));
		}
		self.0.try_wait_process_future(process, arg).await
	}
}

impl tg::instance::Process for Instance {
	fn try_get_process_attach_stream(
		&self,
		arg: tg::process::attach::Arg,
		input: BoxStream<'static, tg::Result<tg::process::attach::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<
			ControlFlow<
				Option<(
					tg::process::attach::Header,
					BoxStream<'static, tg::Result<tg::process::attach::ServerMessage>>,
				)>,
				tg::Error,
			>,
		>,
	> + Send {
		self.0.try_get_process_attach_stream(arg, input)
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

	fn try_get_process_availability(
		&self,
		id: &tg::process::Id,
		arg: tg::process::availability::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::Availability>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_process_availability(id, arg))
		}
	}

	fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> impl Future<Output = tg::Result<tg::process::list::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.list_processes(arg)) }
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
	) -> impl Future<Output = tg::Result<tg::process::put::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.put_process(id, arg)) }
	}

	fn try_cancel_process(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::cancel::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::cancel::Output>>> {
		self.try_cancel_process_inner(process, arg)
	}

	fn try_get_process_control_stream(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<
			ControlFlow<
				Option<(
					tg::process::control::Header,
					impl Stream<Item = tg::Result<tg::process::control::ServerMessage>> + Send + 'static,
				)>,
				tg::Error,
			>,
		>,
	> {
		unsafe {
			std::mem::transmute::<
				_,
				BoxFuture<'_, tg::Result<ControlFlow<Option<(_, BoxStream<_>)>, tg::Error>>>,
			>(self.0.try_get_process_control_stream(arg, stream))
		}
	}

	fn try_signal_process(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_signal_process_inner(process, arg)
	}

	fn try_get_process_status_stream(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static>,
		>,
	> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_get_process_status_stream(process, arg),
			)
		}
	}

	fn try_get_process_children_stream(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
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
				self.0.try_get_process_children_stream(process, arg),
			)
		}
	}

	fn try_set_process_tty_size(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::tty::size::put::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_set_process_tty_size_inner(process, arg)
	}

	fn try_read_process_stdio(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<
			ControlFlow<
				Option<
					impl Stream<Item = tg::Result<tg::process::stdio::read::ServerMessage>>
					+ Send
					+ 'static,
				>,
				tg::Error,
			>,
		>,
	> {
		self.try_read_process_stdio_inner(process, arg, input)
	}

	fn try_write_process_stdio(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
	) -> impl Future<
		Output = tg::Result<
			ControlFlow<
				Option<
					impl Stream<Item = tg::Result<tg::process::stdio::write::Output>> + Send + 'static,
				>,
				tg::Error,
			>,
		>,
	> {
		self.try_write_process_stdio_inner(process, arg, input)
	}

	fn try_touch_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::touch::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_touch_process(id, arg)) }
	}

	fn try_wait_process_future(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::wait::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Future<Output = tg::Result<Option<tg::process::outcome::Data>>> + Send + 'static,
			>,
		>,
	> {
		self.try_wait_process_future_inner(process, arg)
	}
}
