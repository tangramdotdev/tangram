use {
	crate::{Context, Server},
	bytes::Bytes,
	futures::{FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::{BoxAsyncRead, stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_messenger::prelude::*,
	tokio::io::AsyncRead,
	tokio_util::io::{ReaderStream, StreamReader},
};

impl Server {
	pub async fn try_read_process_stdin_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<Option<impl AsyncRead + Send + 'static + use<>>> {
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
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<()> {
		self.write_process_stdio_with_context(
			context,
			id,
			arg,
			tg::process::stdio::Stream::Stdin,
			reader,
		)
		.await
	}

	pub async fn try_read_process_stdout_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<Option<impl AsyncRead + Send + 'static + use<>>> {
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
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<()> {
		self.write_process_stdio_with_context(
			context,
			id,
			arg,
			tg::process::stdio::Stream::Stdout,
			reader,
		)
		.await
	}

	pub async fn try_read_process_stderr_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
	) -> tg::Result<Option<impl AsyncRead + Send + 'static + use<>>> {
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
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<()> {
		self.write_process_stdio_with_context(
			context,
			id,
			arg,
			tg::process::stdio::Stream::Stderr,
			reader,
		)
		.await
	}

	pub async fn try_read_process_stdio_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<Option<impl AsyncRead + Send + 'static + use<>>> {
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(reader) = self
				.try_read_process_stdio_local(id, stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to read local process stdio"))?
		{
			return Ok(Some(reader));
		}

		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(reader) = self
			.try_read_process_stdio_remote(id, stream, &remotes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read remote process stdio"))?
		{
			return Ok(Some(reader));
		}

		Ok(None)
	}

	async fn try_read_process_stdio_local(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<Option<BoxAsyncRead<'static>>> {
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
			filter_subject: Some(subject),
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
		let stream = receiver.attach(task).map_err(std::io::Error::other);
		let reader = StreamReader::new(stream);
		Ok(Some(Box::pin(reader)))
	}

	async fn try_read_process_stdio_remote(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		remotes: &[String],
	) -> tg::Result<Option<BoxAsyncRead<'static>>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				let arg = tg::process::stdio::Arg {
					local: None,
					remotes: None,
				};
				let reader = match stream {
					tg::process::stdio::Stream::Stdin => client
						.try_read_process_stdin(id, arg)
						.await
						.map_err(
							|source| tg::error!(!source, %id, %remote, "failed to read process stdin"),
						)?
						.map(|reader| Box::pin(reader) as BoxAsyncRead<'static>),
					tg::process::stdio::Stream::Stdout => client
						.try_read_process_stdout(id, arg)
						.await
						.map_err(
							|source| tg::error!(!source, %id, %remote, "failed to read process stdout"),
						)?
						.map(|reader| Box::pin(reader) as BoxAsyncRead<'static>),
					tg::process::stdio::Stream::Stderr => client
						.try_read_process_stderr(id, arg)
						.await
						.map_err(
							|source| tg::error!(!source, %id, %remote, "failed to read process stderr"),
						)?
						.map(|reader| Box::pin(reader) as BoxAsyncRead<'static>),
				};
				reader.ok_or_else(|| tg::error!(%id, %remote, "failed to find the process"))
			}
			.boxed()
		});
		let Ok((reader, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(Some(reader))
	}

	pub async fn write_process_stdio_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::Arg,
		stream: tg::process::stdio::Stream,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<()> {
		if Self::local(arg.local, arg.remotes.as_ref())
			&& self
				.get_process_exists_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to check if the process exists"))?
		{
			return self.write_process_stdio_local(id, stream, reader).await;
		}

		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			return self
				.write_process_stdio_remote(id, stream, reader, &remote)
				.await;
		}

		Err(tg::error!("not found"))
	}

	async fn write_process_stdio_local(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<()> {
		if !self
			.get_process_exists_local(id)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the process exists"))?
		{
			return Err(tg::error!("not found"));
		}

		let retry_options = tangram_futures::retry::Options {
			max_retries: u64::MAX,
			..Default::default()
		};
		let stream_ = ReaderStream::new(reader)
			.map_err(|source| tg::error!(!source, "failed to read from stdio"));
		let mut stream_ = pin!(stream_);
		let subject = format!("processes.{id}.{stream}");
		while let Some(bytes) = stream_
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to read stdio chunk"))?
		{
			let retries = tangram_futures::retry_stream(retry_options.clone());
			let mut retries = pin!(retries);
			while let Some(()) = retries.next().await {
				let publish = self
					.messenger
					.stream_publish("stdio".to_owned(), subject.clone(), bytes.clone())
					.and_then(|result| result)
					.await;
				match publish {
					Ok(_) => break,
					Err(error)
						if matches!(
							error,
							tangram_messenger::Error::MaxMessages
								| tangram_messenger::Error::MaxBytes
								| tangram_messenger::Error::PublishFailed
						) =>
					{
						continue;
					},
					Err(source) => {
						return Err(tg::error!(!source, "failed to publish stdio"));
					},
				}
			}
		}

		Ok(())
	}

	async fn write_process_stdio_remote(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		reader: impl AsyncRead + Send + 'static,
		remote: &str,
	) -> tg::Result<()> {
		let client = self
			.get_remote_client(remote.to_owned())
			.await
			.map_err(|source| tg::error!(!source, %remote, "failed to get the remote client"))?;
		let arg = tg::process::stdio::Arg {
			local: None,
			remotes: None,
		};
		match stream {
			tg::process::stdio::Stream::Stdin => {
				client.write_process_stdin(id, arg, reader).await.map_err(
					|source| tg::error!(!source, %id, %remote, "failed to write process stdin"),
				)?;
			},
			tg::process::stdio::Stream::Stdout => {
				client.write_process_stdout(id, arg, reader).await.map_err(
					|source| tg::error!(!source, %id, %remote, "failed to write process stdout"),
				)?;
			},
			tg::process::stdio::Stream::Stderr => {
				client.write_process_stderr(id, arg, reader).await.map_err(
					|source| tg::error!(!source, %id, %remote, "failed to write process stderr"),
				)?;
			},
		}
		Ok(())
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

	pub(crate) async fn handle_post_process_stdio_read_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::OCTET_STREAM)) => (),
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

		let Some(reader) = self
			.try_read_process_stdio_with_context(context, &id, arg, stream)
			.await?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_OCTET_STREAM.to_string(),
			)
			.body(BoxBody::with_reader(reader))
			.unwrap();
		Ok(response)
	}

	pub(crate) async fn handle_post_process_stdio_write_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
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

		let reader = request.reader();
		self.write_process_stdio_with_context(context, &id, arg, stream, reader)
			.await?;

		let response = http::Response::builder().body(BoxBody::empty()).unwrap();
		Ok(response)
	}
}
