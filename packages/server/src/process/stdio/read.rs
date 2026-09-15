use {
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		stream::{self, BoxStream, FuturesUnordered},
	},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, io::SeekFrom, time::Duration},
	tangram_cache::Cache as _,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

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
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
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

	pub(in crate::process) fn read_process_stdio_protocol(
		&self,
		_arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
		output: BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>,
	) -> BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>> {
		let stream = tg::process::stdio::flow::read(input, output);
		match self.context.stopper.clone() {
			Some(stopper) => stream
				.take_until(async move { stopper.wait().await })
				.boxed(),
			None => stream,
		}
	}

	pub(in crate::process) async fn try_read_process_stdio_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
		let output = self
			.try_get_process_local(id, false, false, arg.tokens.local_authorization())
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process"))?;
		let Some(output) = output else {
			return Ok(None);
		};
		let source = Self::get_process_stdio_source(&output.data, &arg)?;
		if matches!(source, Source::Pipe(_))
			&& output
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
		{
			return Ok(None);
		}
		self.authorize_process_stdio_read(id, &source, arg.tokens.local_authorization())
			.await?;
		let mut arg = arg;
		if arg.size == Some(0) {
			return Err(tg::error!("expected a nonzero stdio chunk size"));
		}
		arg.size = Some(
			arg.size
				.unwrap_or(tg::process::stdio::flow::CHUNK_SIZE as u64)
				.min(tg::process::stdio::flow::CHUNK_SIZE as u64),
		);
		let stream = match source {
			Source::Log(streams) => {
				self.try_read_process_stdio_log_local(id, arg, streams)
					.await?
			},
			Source::Null => stream::once(futures::future::ok(
				tg::process::stdio::read::ServerMessage::Response(
					tg::process::stdio::read::Output::End(tg::process::stdio::End {
						combined_position: 0,
						stream_positions: arg.streams.iter().map(|stream| (*stream, 0)).collect(),
					}),
				),
			))
			.boxed(),
			Source::Pipe(streams) => self.try_read_process_stdio_pipe_local(id, &arg, streams),
		};

		Ok(Some(stream))
	}

	async fn authorize_process_stdio_read(
		&self,
		id: &tg::process::Id,
		source: &Source,
		tokens: &[tg::authorization::Token],
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
				let resource =
					tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
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
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>> {
		let (sender, receiver) = async_channel::bounded(1);
		let session = self.clone();
		let id = id.clone();
		let task = Task::spawn(move |_| async move {
			let result = session
				.try_read_process_stdio_log_local_task(&id, arg, streams, sender.clone())
				.boxed()
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});

		Ok(receiver.attach(task).boxed())
	}

	async fn try_read_process_stdio_log_local_task(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::stdio::read::Arg,
		streams: BTreeSet<tg::process::stdio::Stream>,
		sender: async_channel::Sender<tg::Result<tg::process::stdio::read::ServerMessage>>,
	) -> tg::Result<()> {
		let mut wakeups = if arg.timeout == Some(Duration::ZERO) {
			None
		} else {
			let mut wakeups: Vec<BoxStream<'static, ()>> = Vec::new();
			let subject = format!("processes.{id}.log");
			let log_wakeups = self
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ())
				.boxed();
			wakeups.push(log_wakeups);
			let subject = format!("processes.{id}.status");
			let status_wakeups = self
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ())
				.boxed();
			wakeups.push(status_wakeups);
			for &stream in &streams {
				let subject = format!("processes.{id}.{stream}.close");
				let close_wakeups = self
					.server
					.messenger
					.subscribe::<()>(subject)
					.await
					.map_err(|error| tg::error!(!error, "failed to subscribe"))?
					.map(|_| ())
					.boxed();
				wakeups.push(close_wakeups);
			}
			let interval = IntervalStream::new(tokio::time::interval(
				self.server.config.process.stdio_wakeup_interval,
			))
			.skip(1)
			.map(|_| ())
			.boxed();
			wakeups.push(interval);
			let wakeups = stream::select_all(wakeups);
			let wakeups = match arg.timeout {
				Some(timeout) => wakeups.take_until(tokio::time::sleep(timeout)).boxed(),
				None => wakeups.boxed(),
			};

			Some(wakeups)
		};
		let mut positioned = false;
		let mut outcome = None;
		'outer: loop {
			let indexed = self
				.get_process_from_index(id)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the process"))?;
			let data = indexed
				.data
				.ok_or_else(|| tg::error!(%id, "missing the process data"))?;
			// Read the completion marker before draining so every committed chunk is visible.
			let end =
				if data.log.is_some() {
					None
				} else if data.status.is_finished() && data.started_at.is_none() {
					Some(tg::process::stdio::End {
						combined_position: 0,
						stream_positions: [
							(tg::process::stdio::Stream::Stderr, 0),
							(tg::process::stdio::Stream::Stdout, 0),
						]
						.into(),
					})
				} else {
					self.server.cache.try_get_log_end(id).await?.map(|end| {
						tg::process::stdio::End {
							combined_position: end.position,
							stream_positions: [
								(tg::process::stdio::Stream::Stderr, end.stderr_position),
								(tg::process::stdio::Stream::Stdout, end.stdout_position),
							]
							.into(),
						}
					})
				};
			let previous = (arg.position, arg.length);
			let (end, mut stream) = self
				.process_log_stream(id, &mut arg, end, streams.clone())
				.await
				.map_err(|error| tg::error!(!error, "failed to create the log stream"))?;
			// Report the resolved window before its chunks so reconnecting readers do not infer it from a changing EOF.
			if !positioned || previous != (arg.position, arg.length) {
				let Some(SeekFrom::Start(position)) = arg.position else {
					unreachable!()
				};
				let notification = tg::process::stdio::read::Event::Position {
					length: arg.length,
					position,
				};
				if sender
					.send(Ok(tg::process::stdio::read::ServerMessage::Notification(
						notification,
					)))
					.await
					.is_err()
				{
					break;
				}
				positioned = true;
			}
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
						*length = length
							.saturating_add_unsigned(chunk.bytes.len().to_u64().unwrap())
							.min(0);
					}
				}
				let notification = tg::process::stdio::read::Event::Chunk(chunk);
				if sender
					.send(Ok(tg::process::stdio::read::ServerMessage::Notification(
						notification,
					)))
					.await
					.is_err()
				{
					break 'outer;
				}
				if arg.length.is_some_and(|length| length == 0) {
					break;
				}
			}
			let reached_start = arg.length.is_some_and(|length| length < 0)
				&& matches!(arg.position, Some(SeekFrom::Start(0)));
			if arg.length == Some(0) || reached_start {
				outcome = Some(tg::process::stdio::read::Output::Limit {
					position: match arg.position {
						Some(SeekFrom::Start(position)) => position,
						_ => unreachable!(),
					},
				});
				break;
			}
			if let Some(end) = end {
				// A reverse read must reach its limit or the start before completing.
				if arg.length.is_some_and(|length| length < 0) {
					return Err(tg::error!("encountered a gap in the process log"));
				}
				outcome = Some(tg::process::stdio::read::Output::End(end));
				break;
			}
			let Some(wakeups) = &mut wakeups else {
				break;
			};
			let Some(()) = wakeups.next().await else {
				break;
			};
		}

		let outcome = outcome.unwrap_or(tg::process::stdio::read::Output::Timeout {
			position: match arg.position {
				Some(SeekFrom::Start(position)) => position,
				_ => 0,
			},
		});
		let message = tg::process::stdio::read::ServerMessage::Response(outcome);
		sender.send(Ok(message)).await.ok();

		Ok(())
	}

	fn try_read_process_stdio_pipe_local(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::stdio::read::Arg,
		streams: BTreeSet<tg::process::stdio::Stream>,
	) -> BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>> {
		let arg = tg::process::stdio::read::Arg {
			streams: streams.into_iter().collect(),
			..arg.clone()
		};
		self.send_process_control_read(id, arg)
	}

	async fn try_read_process_stdio_regions(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		regions: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
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
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
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
			.try_read_process_stdio_all_inner(id, arg)
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
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
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
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
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
			.try_read_process_stdio_all_inner(id, arg)
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
			if arg.length.is_some_and(|length| length < 0) {
				return Err(tg::error!("piped stdio only supports forward reads"));
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
		let tangram_content_type = tg::process::stdio::TANGRAM_CONTENT_TYPE;
		let output_encoding = super::Encoding::from_accept(accept.as_ref(), tangram_content_type)?;
		let input_encoding = super::Encoding::from_content_type(
			content_type
				.as_ref()
				.ok_or_else(|| tg::error!("missing the content type"))?,
			tangram_content_type,
		)?;
		let id = id
			.parse::<tg::process::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;
		let (arg, request) = request
			.arg::<tg::process::stdio::read::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let max_frame_size = self.server.config.sync.max_frame_size;
		let input = super::decode(request, input_encoding, max_frame_size);
		let Some(output) = self.try_read_process_stdio(&id, arg, input).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		let content_type = output_encoding.content_type(tangram_content_type);
		let body = super::encode(output, output_encoding, max_frame_size);
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}
}
