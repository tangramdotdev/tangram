use {
	super::Handle,
	crate::prelude::*,
	futures::{Stream, future::BoxFuture, stream::BoxStream},
};

impl tg::handle::Process for Handle {
	fn connect_process(
		&self,
		input: BoxStream<'static, tg::Result<tg::process::connect::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<BoxStream<'static, tg::Result<tg::process::connect::ServerMessage>>>,
	> + Send {
		self.0.connect_process(input)
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
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::process::cancel::Output>>> {
		async move {
			if let Some(connection) = self.connection(id) {
				let output = connection
					.request(tg::process::connect::ClientRequestArg::Cancel(arg.into()))
					.await?;
				let tg::process::connect::ServerResponseOutput::Cancel(output) = output else {
					return Err(tg::error!("expected a cancel response"));
				};
				return Ok(Some(output.0));
			}
			self.0.try_cancel_process(id, arg).await
		}
	}

	fn try_get_process_control_stream(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<
			Option<(
				tg::process::control::Output,
				impl Stream<Item = tg::Result<tg::process::control::ServerMessage>> + Send + 'static,
			)>,
		>,
	> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<(_, BoxStream<_>)>>>>(
				self.0.try_get_process_control_stream(arg, stream),
			)
		}
	}

	fn try_signal_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::post::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		async move {
			if let Some(connection) = self.connection(id) {
				let output = connection
					.request(tg::process::connect::ClientRequestArg::Signal(arg.into()))
					.await?;
				if !matches!(output, tg::process::connect::ServerResponseOutput::Signal) {
					return Err(tg::error!("expected a signal response"));
				}
				return Ok(Some(()));
			}
			self.0.try_signal_process(id, arg).await
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

	fn try_set_process_tty_size(
		&self,
		id: &tg::process::Id,
		arg: tg::process::tty::size::put::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		async move {
			if let Some(connection) = self.connection(id) {
				let output = connection
					.request(tg::process::connect::ClientRequestArg::Tty(arg.into()))
					.await?;
				if !matches!(output, tg::process::connect::ServerResponseOutput::Tty) {
					return Err(tg::error!("expected a tty response"));
				}
				return Ok(Some(()));
			}
			self.0.try_set_process_tty_size(id, arg).await
		}
	}

	fn try_read_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::stdio::read::ServerMessage>> + Send + 'static,
			>,
		>,
	> {
		async move {
			if let Some(connection) = self.connection(id) {
				return connection.read(arg, input).await.map(Some);
			}
			self.0.try_read_process_stdio(id, arg, input).await
		}
	}

	fn try_write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::stdio::write::ServerMessage>>
				+ Send
				+ 'static,
			>,
		>,
	> {
		async move {
			if let Some(connection) = self.connection(id) {
				return connection.write(arg, input).await.map(Some);
			}
			self.0.try_write_process_stdio(id, arg, input).await
		}
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
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
			>,
		>,
	> {
		async move {
			if let Some(connection) = self.connection(id) {
				let connection = connection.clone();
				return Ok(Some(futures::FutureExt::boxed(async move {
					connection.wait().await.map(Some)
				})));
			}
			self.0.try_wait_process_future(id, arg).await
		}
	}
}
