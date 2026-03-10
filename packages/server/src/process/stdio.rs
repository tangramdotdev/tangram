use {
	crate::{Context, Server},
	bytes::Bytes,
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future, stream,
		stream::BoxStream,
	},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::{
		stream::Ext as _,
		task::{Stop, Task},
	},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_messenger::prelude::*,
};

impl Server {
	pub async fn try_read_process_stdin_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Event>>>> {
		self.try_read_process_stdio_with_context(
			context,
			id,
			arg,
			tg::process::stdio::Stream::Stdin,
		)
		.await
	}

	pub async fn write_process_stdin_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::Event>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::OutputEvent>>> {
		self.write_process_stdio_with_context(
			context,
			id,
			arg,
			tg::process::stdio::Stream::Stdin,
			stream,
		)
		.await
	}

	pub async fn try_read_process_stdout_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Event>>>> {
		self.try_read_process_stdio_with_context(
			context,
			id,
			arg,
			tg::process::stdio::Stream::Stdout,
		)
		.await
	}

	pub async fn write_process_stdout_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::Event>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::OutputEvent>>> {
		self.write_process_stdio_with_context(
			context,
			id,
			arg,
			tg::process::stdio::Stream::Stdout,
			stream,
		)
		.await
	}

	pub async fn try_read_process_stderr_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Event>>>> {
		self.try_read_process_stdio_with_context(
			context,
			id,
			arg,
			tg::process::stdio::Stream::Stderr,
		)
		.await
	}

	pub async fn write_process_stderr_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::stdio::Event>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::OutputEvent>>> {
		self.write_process_stdio_with_context(
			context,
			id,
			arg,
			tg::process::stdio::Stream::Stderr,
			stream,
		)
		.await
	}

	pub async fn close_process_stdin_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<()> {
		self.close_process_stdio_with_context(context, id, arg, tg::process::stdio::Stream::Stdin)
			.await
	}

	pub async fn close_process_stdout_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<()> {
		self.close_process_stdio_with_context(context, id, arg, tg::process::stdio::Stream::Stdout)
			.await
	}

	pub async fn close_process_stderr_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<()> {
		self.close_process_stdio_with_context(context, id, arg, tg::process::stdio::Stream::Stderr)
			.await
	}

	pub async fn close_process_stdio_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<()> {
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the client"))?;
			return client.close_process_stdio(id, arg, stream).await;
		}
		let subject = format!("processes.{id}.{stream}");
		let _future = self
			.messenger
			.stream_publish("stdio".into(), subject, Bytes::new())
			.await
			.map_err(|source| tg::error!(!source, "failed to send close message"))?;
		Ok(())
	}

	pub async fn try_read_process_stdio_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Event>>>> {
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(event_stream) = self
				.try_read_process_stdio_local(id, stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to read local process stdio"))?
		{
			return Ok(Some(event_stream));
		}
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(event_stream) = self
			.try_read_process_stdio_remote(id, stream, &remotes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read remote process stdio"))?
		{
			return Ok(Some(event_stream));
		}
		Ok(None)
	}

	async fn try_read_process_stdio_local(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Event>>>> {
		if !self
			.get_process_exists_local(id)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the process exists"))?
		{
			return Ok(None);
		}

		let stream_ = self
			.messenger
			.get_stream("stdio".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stdio stream"))?;
		let subject = format!("processes.{id}.{stream}");
		let consumer_config = tangram_messenger::ConsumerConfig {
			deliver_policy: tangram_messenger::DeliverPolicy::All,
			ack_policy: tangram_messenger::AckPolicy::None,
			durable_name: None,
			filter_subject: Some(subject.clone()),
		};
		let consumer = stream_
			.create_consumer(None, consumer_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to create a stdio consumer"))?;
		let (sender, receiver) = async_channel::unbounded::<tg::Result<Bytes>>();
		let task = Task::spawn(|_| async move {
			let stream = consumer
				.subscribe::<Bytes>()
				.await
				.map_err(|source| tg::error!(!source, "failed to subscribe to stdio"))?;
			let mut stream = pin!(stream.map(|result| {
				result
					.map(|message| message.payload)
					.map_err(|source| tg::error!(!source, "failed to read stdio"))
			}));
			while let Some(result) = stream.next().await {
				if sender.send(result).await.is_err() {
					break;
				}
			}
			Ok::<_, tg::Error>(())
		});
		let event_stream = receiver
			.take_while(|result| future::ready(!result.as_ref().is_ok_and(Bytes::is_empty)))
			.map(|result| {
				result.map(|bytes| {
					tg::process::stdio::Event::Chunk(tg::process::stdio::Chunk { bytes })
				})
			})
			.attach(task)
			.boxed();
		Ok(Some(event_stream))
	}

	async fn try_read_process_stdio_remote(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		remotes: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Event>>>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				let arg = tg::process::stdio::Arg::default();
				let stream = match stream {
					tg::process::stdio::Stream::Stdin => client
						.try_read_process_stdin(id, arg)
						.await
						.map_err(
							|source| tg::error!(!source, %remote, "failed to read the process stdin"),
						)?
						.map(futures::StreamExt::boxed),
					tg::process::stdio::Stream::Stdout => client
						.try_read_process_stdout(id, arg)
						.await
						.map_err(
							|source| tg::error!(!source, %remote, "failed to read the process stdout"),
						)?
						.map(futures::StreamExt::boxed),
					tg::process::stdio::Stream::Stderr => client
						.try_read_process_stderr(id, arg)
						.await
						.map_err(
							|source| tg::error!(!source, %remote, "failed to read the process stderr"),
						)?
						.map(futures::StreamExt::boxed),
				};
				Ok::<_, tg::Error>(stream)
			}
			.boxed()
		});
		let Ok((stream, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(stream)
	}

	pub async fn write_process_stdio_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stdio_stream: tg::process::stdio::Stream,
		input: BoxStream<'static, tg::Result<tg::process::stdio::Event>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::OutputEvent>>> {
		if Self::local(arg.local, arg.remotes.as_ref())
			&& self
				.get_process_exists_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to check if the process exists"))?
		{
			return self
				.write_process_stdio_local(id, stdio_stream, input)
				.await;
		}

		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			return self
				.write_process_stdio_remote(id, stdio_stream, input, &remote)
				.await;
		}

		Err(tg::error!("not found"))
	}

	async fn write_process_stdio_local(
		&self,
		id: &tg::process::Id,
		stdio_stream: tg::process::stdio::Stream,
		input: BoxStream<'static, tg::Result<tg::process::stdio::Event>>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::OutputEvent>>> {
		if !self
			.get_process_exists_local(id)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the process exists"))?
		{
			return Err(tg::error!("not found"));
		}

		let server = self.clone();
		let id = id.clone();
		let output_stream = stream::unfold(
			(server, id, stdio_stream, Some(input)),
			|(server, id, stdio_stream, input)| async move {
				let mut input = input?;
				let retry_options = tangram_futures::retry::Options {
					max_retries: u64::MAX,
					..Default::default()
				};
				let subject = format!("processes.{id}.{stdio_stream}");
				loop {
					let result = input.next().await?;
					let event = match result {
						Ok(event) => event,
						Err(source) => {
							return Some((
								Err(tg::error!(!source, "failed to read stdio event")),
								(server, id, stdio_stream, None),
							));
						},
					};
					match event {
						tg::process::stdio::Event::Chunk(chunk) => {
							if chunk.bytes.is_empty() {
								continue;
							}
							let retries = tangram_futures::retry_stream(retry_options.clone());
							let mut retries = pin!(retries);
							while let Some(()) = retries.next().await {
								let publish = server
									.messenger
									.stream_publish(
										"stdio".to_owned(),
										subject.clone(),
										chunk.bytes.clone(),
									)
									.and_then(|result| result)
									.await;
								match publish {
									Ok(_) => break,
									Err(
										tangram_messenger::Error::MaxMessages
										| tangram_messenger::Error::MaxBytes
										| tangram_messenger::Error::PublishFailed,
									) => (),
									Err(source) => {
										return Some((
											Err(tg::error!(!source, "failed to publish stdio")),
											(server, id, stdio_stream, None),
										));
									},
								}
							}
						},
						tg::process::stdio::Event::End => {
							return Some((
								Ok(tg::process::stdio::OutputEvent::End),
								(server, id, stdio_stream, None),
							));
						},
					}
				}
			},
		)
		.boxed();
		Ok(output_stream)
	}

	async fn write_process_stdio_remote(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		input: BoxStream<'static, tg::Result<tg::process::stdio::Event>>,
		remote: &str,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::OutputEvent>>> {
		let client = self
			.get_remote_client(remote.to_owned())
			.await
			.map_err(|source| tg::error!(!source, %remote, "failed to get the remote client"))?;
		let arg = tg::process::stdio::Arg::default();
		let stream = match stream {
			tg::process::stdio::Stream::Stdin => client
				.write_process_stdin(id, arg, input)
				.await
				.map_err(|source| tg::error!(!source, "failed to write process stdin"))?
				.boxed(),
			tg::process::stdio::Stream::Stdout => client
				.write_process_stdout(id, arg, input)
				.await
				.map_err(|source| tg::error!(!source, "failed to write process stdout"))?
				.boxed(),
			tg::process::stdio::Stream::Stderr => client
				.write_process_stderr(id, arg, input)
				.await
				.map_err(|source| tg::error!(!source, "failed to write process sterr"))?
				.boxed(),
		};
		Ok(stream)
	}

	pub(crate) async fn handle_post_process_stdin_read_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		self.handle_post_process_stdio_read_request(
			request,
			context,
			id,
			tg::process::stdio::Stream::Stdin,
		)
		.await
	}

	pub(crate) async fn handle_post_process_stdin_write_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		self.handle_post_process_stdio_write_request(
			request,
			context,
			id,
			tg::process::stdio::Stream::Stdin,
		)
		.await
	}

	pub(crate) async fn handle_post_process_stdout_read_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		self.handle_post_process_stdio_read_request(
			request,
			context,
			id,
			tg::process::stdio::Stream::Stdout,
		)
		.await
	}

	pub(crate) async fn handle_post_process_stdout_write_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		self.handle_post_process_stdio_write_request(
			request,
			context,
			id,
			tg::process::stdio::Stream::Stdout,
		)
		.await
	}

	pub(crate) async fn handle_post_process_stderr_read_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		self.handle_post_process_stdio_read_request(
			request,
			context,
			id,
			tg::process::stdio::Stream::Stderr,
		)
		.await
	}

	pub(crate) async fn handle_post_process_stderr_write_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		self.handle_post_process_stdio_write_request(
			request,
			context,
			id,
			tg::process::stdio::Stream::Stderr,
		)
		.await
	}

	pub(crate) async fn handle_post_process_stdin_close_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		self.handle_post_process_stdio_close_request(
			request,
			context,
			id,
			tg::process::stdio::Stream::Stdin,
		)
		.await
	}

	pub(crate) async fn handle_post_process_stdout_close_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		self.handle_post_process_stdio_close_request(
			request,
			context,
			id,
			tg::process::stdio::Stream::Stdout,
		)
		.await
	}

	pub(crate) async fn handle_post_process_stderr_close_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		self.handle_post_process_stdio_close_request(
			request,
			context,
			id,
			tg::process::stdio::Stream::Stderr,
		)
		.await
	}

	pub(crate) async fn handle_post_process_stdio_close_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<http::Response<BoxBody>> {
		let id = id
			.parse::<tg::process::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		self.close_process_stdio_with_context(context, &id, arg, stream)
			.await?;

		let response = http::Response::builder().body(BoxBody::empty()).unwrap();
		Ok(response)
	}

	pub(crate) async fn handle_post_process_stdio_read_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
		stdio_stream: tg::process::stdio::Stream,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let id = id
			.parse::<tg::process::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		let Some(event_stream) = self
			.try_read_process_stdio_with_context(context, &id, arg, stdio_stream)
			.await?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let event_stream = event_stream.take_until(stop);

		// Create the body.
		let content_type = mime::TEXT_EVENT_STREAM;
		let sse_stream = event_stream.map(|result| match result {
			Ok(event) => event.try_into(),
			Err(error) => error.try_into(),
		});
		let body = BoxBody::with_sse_stream(sse_stream);

		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();
		Ok(response)
	}

	pub(crate) async fn handle_post_process_stdio_write_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
		stdio_stream: tg::process::stdio::Stream,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let id = id
			.parse::<tg::process::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Parse the input SSE stream. Stop it when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop_signal = stop.clone();
		let input_stream = request
			.sse()
			.map_err(|source| tg::error!(!source, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			})
			.boxed();

		let output_stream = self
			.write_process_stdio_with_context(context, &id, arg, stdio_stream, input_stream)
			.await?;

		// Emit OutputEvent::Stop if the server stop signal fires before the output stream ends.
		let stop_future = async move { stop_signal.wait().await };
		let output_stream = output_stream
			.take_until(stop_future)
			.chain(stream::once(future::ready(Ok(
				tg::process::stdio::OutputEvent::Stop,
			))))
			.boxed();

		// Create the response body.
		let content_type = mime::TEXT_EVENT_STREAM;
		let sse_stream = output_stream.map(|result| match result {
			Ok(event) => event.try_into(),
			Err(error) => error.try_into(),
		});
		let body = BoxBody::with_sse_stream(sse_stream);

		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();
		Ok(response)
	}
}
