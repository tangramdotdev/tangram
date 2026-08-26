use {
	crate::Session,
	futures::{
		StreamExt as _, TryStreamExt as _,
		stream::{self, BoxStream, FuturesUnordered},
	},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, io::SeekFrom, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::{IntervalStream, ReceiverStream},
};

const READ_CHUNK_SIZE: usize = 32 * 1024;

enum Source {
	Log(BTreeSet<tg::process::stdio::Stream>),
	Null,
	Pipe(BTreeSet<tg::process::stdio::Stream>),
}

impl Session {
	pub async fn try_read_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		let Some(source) = self.try_read_process_stdio_source(id, arg.clone()).await? else {
			return Ok(None);
		};
		let stream = self.read_process_stdio_protocol(arg, input, source);

		Ok(Some(stream))
	}

	async fn try_read_process_stdio_source(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		if let Some(local) = &locations.local {
			if local.current
				&& let Some(stream) = self
					.try_read_process_stdio_local(id, arg.clone())
					.await
					.map_err(|error| tg::error!(!error, "failed to read local process stdio"))?
			{
				return Ok(Some(stream));
			}
			if let Some(stream) = self
				.try_read_process_stdio_regions(id, arg.clone(), &local.regions)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to read process stdio from another region")
				})? {
				return Ok(Some(stream));
			}
		}
		if let Some(stream) = self
			.try_read_process_stdio_remotes(id, arg, &locations.remotes)
			.await
			.map_err(|error| tg::error!(!error, "failed to read process stdio from a remote"))?
		{
			return Ok(Some(stream));
		}

		Ok(None)
	}

	fn read_process_stdio_protocol(
		&self,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
		output: BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>,
	) -> BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>> {
		let (sender, receiver) = tokio::sync::mpsc::channel(4);
		let stopper = self.context.stopper.clone();
		let task = Task::spawn(move |_| async move {
			let future = Self::read_process_stdio_protocol_task(arg, input, output, &sender);
			let result = match stopper {
				Some(stopper) => {
					tokio::select! {
						result = future => result,
						() = stopper.wait() => {
							let message = tg::process::stdio::read::ServerMessage::Notification(
								tg::process::stdio::read::ServerNotification::Stop,
							);
							sender.send(Ok(message)).await.ok();

							Ok(())
						},
					}
				},
				None => future.await,
			};
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}

			Ok::<_, tg::Error>(())
		});

		ReceiverStream::new(receiver).attach(task).boxed()
	}

	async fn read_process_stdio_protocol_task(
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
		output: BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::process::stdio::read::ServerMessage>>,
	) -> tg::Result<()> {
		let combined = arg.streams.len() > 1;
		let forward = arg.length.is_none_or(|length| length >= 0);
		let mut input = pin!(input);
		let mut output = pin!(output);
		while let Some(chunk) = output.try_next().await? {
			let start = if combined {
				chunk.combined_position
			} else {
				chunk.stream_position
			};
			let end = start
				.checked_add(chunk.bytes.len().to_u64().unwrap())
				.ok_or_else(|| tg::error!("the stdio position is too large"))?;
			let expected = if forward { end } else { start };
			let message = tg::process::stdio::read::ServerMessage::Notification(
				tg::process::stdio::read::ServerNotification::Chunk(chunk),
			);
			if sender.send(Ok(message)).await.is_err() {
				return Ok(());
			}
			loop {
				let message = input.try_next().await?.ok_or_else(|| {
					tg::error!("the stdio read stream ended before the chunk was read")
				})?;
				match message {
					tg::process::stdio::read::ClientMessage::Notification(
						tg::process::stdio::read::ClientNotification::Read { position },
					) if forward && position >= expected || !forward && position <= expected => break,
					tg::process::stdio::read::ClientMessage::Notification(_) => (),
					tg::process::stdio::read::ClientMessage::Response(_) => {
						return Err(tg::error!("received an unexpected stdio read response"));
					},
				}
			}
		}
		let message = tg::process::stdio::read::ServerMessage::Request(
			tg::process::stdio::read::ServerRequest::End,
		);
		if sender.send(Ok(message)).await.is_err() {
			return Ok(());
		}
		loop {
			let message = input
				.try_next()
				.await?
				.ok_or_else(|| tg::error!("the stdio read stream ended before the end response"))?;
			match message {
				tg::process::stdio::read::ClientMessage::Notification(_) => (),
				tg::process::stdio::read::ClientMessage::Response(
					tg::process::stdio::read::ClientResponse::End,
				) => break,
			}
		}

		Ok(())
	}

	async fn try_read_process_stdio_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>>> {
		let output = self
			.try_get_process_local(id, false, false, arg.tokens.local())
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process"))?;
		let Some(output) = output else {
			return Ok(None);
		};
		let source = Self::get_process_stdio_source(&output.data, &arg)?;
		self.authorize_process_stdio_read(id, &source, arg.tokens.local())
			.await?;
		let stream = match source {
			Source::Log(streams) => {
				self.try_read_process_stdio_log_local(id, arg, streams)
					.await?
			},
			Source::Null => stream::empty().boxed(),
			Source::Pipe(streams) => self.try_read_process_stdio_pipe_local(id, &arg, streams),
		};

		Ok(Some(stream))
	}

	async fn authorize_process_stdio_read(
		&self,
		id: &tg::process::Id,
		source: &Source,
		token: Option<&tg::authorization::Token>,
	) -> tg::Result<()> {
		let Source::Pipe(streams) = source else {
			return Ok(());
		};
		let stdin = streams.contains(&tg::process::stdio::Stream::Stdin);
		let output = streams
			.iter()
			.any(|stream| !matches!(stream, tg::process::stdio::Stream::Stdin));
		match (stdin, output) {
			(true, false) => {
				if !matches!(
					&self.context.principal,
					tg::Principal::Process(process) if process == id
				) {
					return Err(tg::error!("unauthorized"));
				}

				Ok(())
			},
			(false, _) => {
				let permission = tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Parent,
				);
				let resource = tg::Referent::with_node_and_token(id.clone(), token.cloned());
				let authorized = self.authorize(resource, permission).await?;
				if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
					return Err(tg::error!("unauthorized"));
				}

				Ok(())
			},
			(true, true) => Err(tg::error!(
				"cannot read stdin and stdout or stderr in a single request"
			)),
		}
	}

	async fn try_read_process_stdio_log_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		streams: BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>> {
		let (sender, receiver) = async_channel::unbounded();
		let session = self.clone();
		let id = id.clone();
		let task = Task::spawn(move |_| async move {
			let result = session
				.try_read_process_stdio_log_local_task(&id, arg, streams, sender.clone())
				.await;
			if let Err(error) = result {
				sender.try_send(Err(error)).ok();
			}
		});

		Ok(receiver.attach(task).boxed())
	}

	async fn try_read_process_stdio_log_local_task(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::stdio::read::Arg,
		streams: BTreeSet<tg::process::stdio::Stream>,
		sender: async_channel::Sender<tg::Result<tg::process::stdio::Chunk>>,
	) -> tg::Result<()> {
		let mut wakeups = if arg.timeout == Some(Duration::ZERO) {
			None
		} else {
			let subject = format!("processes.{id}.log");
			let log_wakeups = self
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ())
				.boxed();
			let subject = format!("processes.{id}.status");
			let status_wakeups = self
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ())
				.boxed();
			let interval = IntervalStream::new(tokio::time::interval(
				self.server.config.process.stdio_wakeup_interval,
			))
			.skip(1)
			.map(|_| ())
			.boxed();
			let wakeups = stream::select_all([log_wakeups, status_wakeups, interval]);
			let wakeups = match arg.timeout {
				Some(timeout) => wakeups.take_until(tokio::time::sleep(timeout)).boxed(),
				None => wakeups.boxed(),
			};

			Some(wakeups)
		};
		'outer: loop {
			let status = self
				.get_process_status_local(id)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the process status"))?;
			let mut stream = self
				.process_log_stream(id, arg.position, arg.length, arg.size, streams.clone())
				.await
				.map_err(|error| tg::error!(!error, "failed to create the log stream"))?;
			while let Some(chunk) = stream.try_next().await? {
				let position = if streams.len() > 1 {
					chunk.combined_position
				} else {
					chunk.stream_position
				};
				let forward = arg.length.is_none_or(|length| length >= 0);
				arg.position.replace(SeekFrom::Start(if forward {
					position + chunk.bytes.len().to_u64().unwrap()
				} else {
					position
				}));
				if let Some(length) = &mut arg.length {
					if *length >= 0 {
						*length -= chunk.bytes.len().to_i64().unwrap().min(*length);
					} else {
						*length += chunk.bytes.len().to_i64().unwrap().min(length.abs());
					}
				}
				if sender.send(Ok(chunk)).await.is_err() {
					break 'outer;
				}
				if arg.length.is_some_and(|length| length == 0) {
					break;
				}
			}
			let reached_start = arg.length.is_some_and(|length| length < 0)
				&& matches!(arg.position, Some(SeekFrom::Start(0)));
			if status.is_finished() || arg.length == Some(0) || reached_start {
				break;
			}
			let Some(wakeups) = &mut wakeups else {
				break;
			};
			if wakeups.next().await.is_none() {
				break;
			}
		}

		Ok(())
	}

	fn try_read_process_stdio_pipe_local(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::stdio::read::Arg,
		streams: BTreeSet<tg::process::stdio::Stream>,
	) -> BoxStream<'static, tg::Result<tg::process::stdio::Chunk>> {
		let session = self.clone();
		let id = id.clone();
		let position = match arg.position {
			None => 0,
			Some(SeekFrom::Start(position)) => position,
			Some(SeekFrom::Current(_) | SeekFrom::End(_)) => unreachable!(),
		};
		let state = (session, id, streams, position);
		stream::try_unfold(state, |(session, id, streams, position)| async move {
			for stream in &streams {
				crate::checkpoint!(
					session.server,
					"process.stdio.read.request",
					process = %id,
					stream = %stream,
				)
				.await;
			}
			let request = tg::process::control::ServerRequestArg::Read(
				tg::process::control::ReadServerRequestArg {
					length: READ_CHUNK_SIZE,
					position,
					streams: streams.iter().copied().collect(),
				},
			);
			let retry = tangram_futures::retry::Options {
				max_retries: u64::MAX,
				..Default::default()
			};
			let timeout = if session
				.server
				.config
				.roles
				.contains(&crate::config::Role::Runner)
			{
				session.server.config.runner.stdio_drain_timeout
			} else {
				Duration::from_secs(10)
			};
			let options = crate::control::Options { retry, timeout };
			let response = session
				.send_process_control_request(&id, request, options)
				.await??;
			let response = response
				.try_unwrap_read()
				.map_err(|_| tg::error!("expected a read response"))?;
			let Some(chunk) = response.chunk else {
				return Ok(None);
			};
			let start = if streams.len() > 1 {
				chunk.combined_position
			} else {
				chunk.stream_position
			};
			if start > position {
				return Err(tg::error!(
					expected = %position,
					actual = %start,
					"encountered a gap in the process stdio stream"
				));
			}
			let position = start
				.checked_add(chunk.bytes.len().to_u64().unwrap())
				.ok_or_else(|| tg::error!("the stdio position is too large"))?;
			let state = (session, id, streams, position);

			Ok(Some((chunk, state)))
		})
		.boxed()
	}

	async fn try_read_process_stdio_regions(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		regions: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_read_process_stdio_region(id, arg.clone(), region))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(stream)) => {
					result = Ok(Some(stream));
					break;
				},
				Ok(None) => (),
				Err(error) => result = Err(error),
			}
		}

		result
	}

	async fn try_read_process_stdio_region(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		region: &str,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>>> {
		let client = self.get_region_session_for_process(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let tokens = arg.tokens.for_location(&location);
		let arg = tg::process::stdio::read::Arg {
			location: Some(location.into()),
			tokens,
			..arg
		};
		let stream = client
			.try_read_process_stdio_all(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to read the process stdio"),
			)?
			.map(futures::StreamExt::boxed);

		Ok(stream)
	}

	async fn try_read_process_stdio_remotes(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_read_process_stdio_remote(id, arg.clone(), remote))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(stream)) => {
					result = Ok(Some(stream));
					break;
				},
				Ok(None) => (),
				Err(error) => result = Err(error),
			}
		}

		result
	}

	async fn try_read_process_stdio_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>>> {
		let client = self
			.get_remote_session_for_process(&remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let tokens = arg.tokens.for_location(&location);
		let arg = tg::process::stdio::read::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			tokens,
			..arg
		};
		let stream = client
			.try_read_process_stdio_all(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to read the process stdio"),
			)?
			.map(futures::StreamExt::boxed);

		Ok(stream)
	}

	fn get_process_stdio_source(
		data: &tg::process::Data,
		arg: &tg::process::stdio::read::Arg,
	) -> tg::Result<Source> {
		let mut log_streams = BTreeSet::new();
		let mut pipe_streams = BTreeSet::new();
		for stream in &arg.streams {
			let stdio = match stream {
				tg::process::stdio::Stream::Stderr => &data.stderr,
				tg::process::stdio::Stream::Stdin => &data.stdin,
				tg::process::stdio::Stream::Stdout => &data.stdout,
			};
			match stdio {
				tg::process::Stdio::Log => {
					if matches!(stream, tg::process::stdio::Stream::Stdin) {
						return Err(tg::error!("invalid stdio stream"));
					}
					log_streams.insert(*stream);
				},
				tg::process::Stdio::Null => (),
				tg::process::Stdio::Pipe | tg::process::Stdio::Tty => {
					pipe_streams.insert(*stream);
				},
				tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit => {
					return Err(tg::error!("invalid stdio"));
				},
			}
		}
		if !log_streams.is_empty() && !pipe_streams.is_empty() {
			return Err(tg::error!(
				"cannot read logged and piped stdio in a single request"
			));
		}
		if !pipe_streams.is_empty() {
			if arg.length.is_some() || arg.size.is_some() {
				return Err(tg::error!(
					"length and size are only valid for logged stdio"
				));
			}
			if matches!(arg.position, Some(SeekFrom::Current(_) | SeekFrom::End(_))) {
				return Err(tg::error!("piped stdio only supports an absolute position"));
			}
			if pipe_streams.contains(&tg::process::stdio::Stream::Stdout)
				&& pipe_streams.contains(&tg::process::stdio::Stream::Stderr)
				&& matches!(data.stdout, tg::process::Stdio::Tty)
				&& matches!(data.stderr, tg::process::Stdio::Tty)
			{
				pipe_streams.remove(&tg::process::stdio::Stream::Stderr);
			}

			return Ok(Source::Pipe(pipe_streams));
		}
		if log_streams.is_empty() {
			return Ok(Source::Null);
		}

		Ok(Source::Log(log_streams))
	}

	pub(crate) async fn try_read_process_stdio_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the content type header"))?;
		let output_encoding = super::Encoding::from_accept(accept.as_ref())?;
		let input_encoding = content_type
			.as_ref()
			.ok_or_else(|| tg::error!("missing the content type"))?
			.try_into()?;
		let id = id
			.parse::<tg::process::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;
		let arg: tg::process::stdio::read::Arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let max_frame_size = self.server.config.sync.max_frame_size;
		let input = super::decode(request, input_encoding, max_frame_size);
		let Some(output) = self.try_read_process_stdio(&id, arg, input).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		let body = super::encode(output, output_encoding, max_frame_size);
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, output_encoding.content_type())
			.body(body)
			.unwrap();

		Ok(response)
	}
}
