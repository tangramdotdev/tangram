use {
	crate::{Context, Server},
	bytes::Bytes,
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, io::SeekFrom, pin::pin, time::Duration},
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
	tangram_store::Store as _,
	tokio_stream::wrappers::{IntervalStream, ReceiverStream},
	tokio_util::task::AbortOnDropHandle,
};

enum ReadBacking {
	Empty,
	Log(Option<tg::process::stdio::Stream>),
	Messenger(BTreeSet<tg::process::stdio::Stream>),
}

enum WriteBacking {
	Log,
	Messenger,
	Null,
}

impl Server {
	pub async fn try_read_process_stdio_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(event_stream) = self
				.try_read_process_stdio_local(id, arg.clone())
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
			.try_read_process_stdio_remote(id, arg, &remotes)
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
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>> {
		let output = self
			.try_get_process_local(id, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?;
		let Some(output) = output else {
			return Ok(None);
		};

		let backing = classify_read_process_stdio(&output.data, &arg)?;
		let stream = match backing {
			ReadBacking::Empty => {
				stream::once(future::ok(tg::process::stdio::read::Event::End)).boxed()
			},
			ReadBacking::Log(stream) => {
				self.try_read_process_stdio_log_local(id, arg, stream)
					.await?
			},
			ReadBacking::Messenger(streams) => {
				self.try_read_process_stdio_messenger_local(id, &streams)
					.await?
			},
		};
		Ok(Some(stream))
	}

	async fn try_read_process_stdio_log_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		stream: Option<tg::process::stdio::Stream>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>> {
		let (sender, receiver) = async_channel::unbounded();
		let server = self.clone();
		let id = id.clone();
		let task = Task::spawn(move |_| async move {
			let result = server
				.try_read_process_stdio_log_local_task(&id, arg, stream, sender.clone())
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
		stream: Option<tg::process::stdio::Stream>,
		sender: async_channel::Sender<tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<()> {
		let subject = format!("processes.{id}.log");
		let log = self
			.messenger
			.subscribe::<()>(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();

		let subject = format!("processes.{id}.status");
		let status = self
			.messenger
			.subscribe::<()>(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();

		let interval = IntervalStream::new(tokio::time::interval(Duration::from_mins(1)))
			.map(|_| ())
			.boxed();

		let mut events = stream::select_all([log, status, interval]).boxed();
		'outer: loop {
			let status = self
				.get_current_process_status_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the process status"))?;

			let mut stream = self
				.process_log_stream(id, arg.position, arg.length, arg.size, stream)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the log stream"))?;
			while let Some(chunk) = stream.next().await {
				if let Ok(chunk) = &chunk {
					let position = chunk
						.position
						.ok_or_else(|| tg::error!("expected the chunk position"))?;
					let forward = arg.length.is_none_or(|length| length >= 0);
					arg.position.replace(SeekFrom::Start(if forward {
						position + chunk.bytes.len().to_u64().unwrap()
					} else {
						position.saturating_sub(1)
					}));
					if let Some(length) = &mut arg.length {
						if *length >= 0 {
							*length -= chunk.bytes.len().to_i64().unwrap().min(*length);
						} else {
							*length += chunk.bytes.len().to_i64().unwrap().min(length.abs());
						}
					}
				}
				let event = chunk.map(tg::process::stdio::read::Event::Chunk);
				if sender.send(event).await.is_err() {
					break 'outer;
				}

				if arg.length.is_some_and(|length| length == 0) {
					break;
				}
			}

			let reached_start = arg.length.is_some_and(|length| length < 0)
				&& matches!(arg.position, Some(SeekFrom::Start(0)));
			if status.is_finished() || arg.length.is_some_and(|length| length == 0) || reached_start
			{
				sender
					.send(Ok(tg::process::stdio::read::Event::End))
					.await
					.ok();
				break;
			}

			events.next().await;
		}

		Ok(())
	}

	pub(crate) async fn try_read_process_stdio_messenger_local(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>> {
		let stream = self
			.messenger
			.get_stream("stdio".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stdio stream"))?;
		let consumer_config = tangram_messenger::ConsumerConfig {
			deliver_policy: tangram_messenger::DeliverPolicy::All,
			ack_policy: tangram_messenger::AckPolicy::None,
			durable_name: None,
			filter_subjects: streams
				.iter()
				.map(|stream| format!("processes.{id}.{stream}"))
				.collect(),
		};
		let consumer = stream
			.create_consumer(None, consumer_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to create a stdio consumer"))?;
		let (sender, receiver) =
			async_channel::unbounded::<tg::Result<tg::process::stdio::read::Event>>();
		let streams = streams.clone();
		let task = Task::spawn(move |_| async move {
			let stream = consumer
				.subscribe::<Bytes>()
				.await
				.map_err(|source| tg::error!(!source, "failed to subscribe to stdio"))?;
			let mut stream = pin!(stream.map(|result| {
				result
					.map(|message| (message.subject, message.payload))
					.map_err(|source| tg::error!(!source, "failed to read stdio"))
			}));
			let mut ended = BTreeSet::new();
			while let Some(result) = stream.next().await {
				let event = match result {
					Ok((subject, bytes)) => (
						subject,
						serde_json::from_slice::<tg::process::stdio::read::Event>(&bytes).map_err(
							|source| tg::error!(!source, "failed to deserialize the stdio event"),
						)?,
					),
					Err(error) => {
						sender.send(Err(error)).await.ok();
						break;
					},
				};
				match event {
					(_, tg::process::stdio::read::Event::Chunk(chunk)) => {
						if sender
							.send(Ok(tg::process::stdio::read::Event::Chunk(chunk)))
							.await
							.is_err()
						{
							break;
						}
					},
					(subject, tg::process::stdio::read::Event::End) => {
						let stream = subject
							.rsplit('.')
							.next()
							.ok_or_else(|| tg::error!(%subject, "failed to parse the subject"))?
							.parse::<tg::process::stdio::Stream>()
							.map_err(
								|source| tg::error!(!source, %subject, "failed to parse the subject"),
							)?;
						ended.insert(stream);
						if ended == streams {
							if sender
								.send(Ok(tg::process::stdio::read::Event::End))
								.await
								.is_err()
							{
								break;
							}
							break;
						}
					},
				}
			}
			Ok::<_, tg::Error>(())
		});
		Ok(receiver.attach(task).boxed())
	}

	async fn try_read_process_stdio_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		remotes: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let arg = tg::process::stdio::read::Arg {
			local: None,
			remotes: None,
			..arg
		};
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			let arg = arg.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				let stream = client
					.try_read_process_stdio_all(id, arg)
					.await
					.map_err(
						|source| tg::error!(!source, %remote, "failed to read the process stdio"),
					)?
					.map(futures::StreamExt::boxed);
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
		arg: tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
		stop: Option<Stop>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		let (sender, receiver) = tokio::sync::mpsc::channel(4);

		let handle = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				let future = if Self::local(arg.local, arg.remotes.as_ref())
					&& server
						.get_process_exists_local(&id)
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to check if the process exists")
						})? {
					server
						.write_process_stdio_local_task(&id, &arg.streams, input)
						.left_future()
				} else if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
					server
						.write_process_stdio_remote_task(&id, &arg.streams, input, remote)
						.right_future()
				} else {
					return Err(tg::error!("not found"));
				};

				let result = if let Some(stop) = stop {
					let future = pin!(future);
					let stop = pin!(stop.wait());
					match future::select(future, stop).await {
						future::Either::Left((result, _)) => result,
						future::Either::Right(((), future)) => {
							sender
								.send(Ok(tg::process::stdio::write::Event::Stop))
								.await
								.ok();
							future.await
						},
					}
				} else {
					future.await
				};

				if let Err(error) = result {
					sender.send(Err(error)).await.ok();
				}
				sender
					.send(Ok(tg::process::stdio::write::Event::End))
					.await
					.ok();
				Ok::<_, tg::Error>(())
			}
		}));
		let stream = ReceiverStream::new(receiver).attach(handle).boxed();
		Ok(stream)
	}

	async fn write_process_stdio_local_task(
		&self,
		id: &tg::process::Id,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<()> {
		let output = self
			.try_get_process_local(id, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?
			.ok_or_else(|| tg::error!("not found"))?;
		let data = output.data;
		let started_at = data.started_at;
		let streams = streams.iter().copied().collect::<BTreeSet<_>>();
		let mut input = pin!(input);
		while let Some(result) = input.next().await {
			let event = match result {
				Ok(event) => event,
				Err(source) => {
					return Err(tg::error!(!source, "failed to read a stdio event"));
				},
			};
			match event {
				tg::process::stdio::read::Event::Chunk(mut chunk) => {
					if chunk.bytes.is_empty() {
						continue;
					}
					if !streams.contains(&chunk.stream) {
						return Err(tg::error!(
							stream = %chunk.stream,
							"received an unexpected stdio stream"
						));
					}
					chunk.position = None;
					match classify_write_process_stdio_stream(&data, chunk.stream)? {
						WriteBacking::Log => {
							let started_at = started_at
								.ok_or_else(|| tg::error!("expected the process to be started"))?;
							if data.status != tg::process::Status::Started {
								return Err(tg::error!("failed to find the process"));
							}
							let timestamp =
								time::OffsetDateTime::now_utc().unix_timestamp() - started_at;
							let arg = tangram_store::PutProcessLogArg {
								bytes: chunk.bytes,
								process: id.clone(),
								stream: validate_log_stream(chunk.stream)?,
								timestamp,
							};
							self.store
								.put_process_log(arg)
								.await
								.map_err(|source| tg::error!(!source, "failed to store the log"))?;
							tokio::spawn({
								let server = self.clone();
								let id = id.clone();
								async move {
									server
										.messenger
										.publish(format!("processes.{id}.log"), ())
										.await
										.inspect_err(|error| {
											tracing::error!(%error, "failed to publish");
										})
										.ok();
								}
							});
						},
						WriteBacking::Messenger => {
							let subject = format!("processes.{id}.{}", chunk.stream);
							let payload =
								serde_json::to_vec(&tg::process::stdio::read::Event::Chunk(chunk))
									.map(Bytes::from)
									.map_err(|source| {
										tg::error!(!source, "failed to serialize the stdio event")
									})?;
							self.messenger
								.stream_publish("stdio".to_owned(), subject, payload)
								.and_then(|result| result)
								.await
								.map_err(|source| tg::error!(!source, "failed to publish stdio"))?;
						},
						WriteBacking::Null => (),
					}
				},
				tg::process::stdio::read::Event::End => {
					let payload = serde_json::to_vec(&tg::process::stdio::read::Event::End)
						.map(Bytes::from)
						.map_err(|source| {
							tg::error!(!source, "failed to serialize the stdio event")
						})?;
					for stream in &streams {
						if !matches!(
							classify_write_process_stdio_stream(&data, *stream)?,
							WriteBacking::Messenger
						) {
							continue;
						}
						let subject = format!("processes.{id}.{stream}");
						self.messenger
							.stream_publish("stdio".to_owned(), subject, payload.clone())
							.and_then(|result| result)
							.await
							.map_err(|source| tg::error!(!source, "failed to publish stdio"))?;
					}
					return Ok(());
				},
			}
		}
		Ok(())
	}

	async fn write_process_stdio_remote_task(
		&self,
		id: &tg::process::Id,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
		remote: String,
	) -> tg::Result<()> {
		let client = self
			.get_remote_client(remote.clone())
			.await
			.map_err(|source| tg::error!(!source, %remote, "failed to get the remote client"))?;
		let arg = tg::process::stdio::write::Arg {
			streams: streams.to_vec(),
			..Default::default()
		};
		let output = client
			.write_process_stdio(id, arg, input)
			.await
			.map_err(|source| tg::error!(!source, "failed to write stdio"))?;
		let mut output = pin!(output);
		while output.try_next().await?.is_some() {}
		Ok(())
	}

	pub(crate) async fn handle_post_process_stdio_read_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
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
		let arg: tg::process::stdio::read::Arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		let Some(event_stream) = self
			.try_read_process_stdio_with_context(context, &id, arg)
			.await?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let event_stream = event_stream.take_until(stop);

		let content_type = mime::TEXT_EVENT_STREAM;
		let stream = event_stream.map(|result| match result {
			Ok(event) => event.try_into(),
			Err(error) => error.try_into(),
		});
		let body = BoxBody::with_sse_stream(stream);

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
		let arg: tg::process::stdio::write::Arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let input = request
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

		let output = self
			.write_process_stdio_with_context(context, &id, arg, input, Some(stop.clone()))
			.await?;

		let content_type = mime::TEXT_EVENT_STREAM;
		let stream = output.map(|result| match result {
			Ok(event) => event.try_into(),
			Err(error) => error.try_into(),
		});
		let body = BoxBody::with_sse_stream(stream);

		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();
		Ok(response)
	}
}

fn classify_read_process_stdio(
	data: &tg::process::Data,
	arg: &tg::process::stdio::read::Arg,
) -> tg::Result<ReadBacking> {
	let mut log_streams = BTreeSet::new();
	let mut messenger_streams = BTreeSet::new();
	for stream in &arg.streams {
		let stdio = process_stdio(data, *stream);
		match stream {
			tg::process::stdio::Stream::Stdin => {
				return Err(tg::error!("reading stdin is invalid"));
			},
			tg::process::stdio::Stream::Stdout | tg::process::stdio::Stream::Stderr => (),
		}
		match stdio {
			tg::process::Stdio::Log => {
				log_streams.insert(validate_log_stream(*stream)?);
			},
			tg::process::Stdio::Null => (),
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => {
				messenger_streams.insert(*stream);
			},
			tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit => {
				return Err(tg::error!("invalid stdio"));
			},
		}
	}
	if !log_streams.is_empty() && !messenger_streams.is_empty() {
		return Err(tg::error!(
			"cannot read logged and live stdio in a single request"
		));
	}
	if !messenger_streams.is_empty() {
		if has_ranged_stdio_read_arg(arg) {
			return Err(tg::error!(
				"position, length, and size are only valid for logged stdio"
			));
		}
		return Ok(ReadBacking::Messenger(messenger_streams));
	}
	if log_streams.is_empty() {
		return Ok(ReadBacking::Empty);
	}
	let stream = if log_streams.len() == 2 {
		None
	} else {
		log_streams.into_iter().next()
	};
	Ok(ReadBacking::Log(stream))
}

fn classify_write_process_stdio_stream(
	data: &tg::process::Data,
	stream: tg::process::stdio::Stream,
) -> tg::Result<WriteBacking> {
	let stdio = process_stdio(data, stream);
	match stream {
		tg::process::stdio::Stream::Stdin => match stdio {
			tg::process::Stdio::Null => Ok(WriteBacking::Null),
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Ok(WriteBacking::Messenger),
			tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit | tg::process::Stdio::Log => {
				Err(tg::error!("invalid stdio"))
			},
		},
		tg::process::stdio::Stream::Stdout | tg::process::stdio::Stream::Stderr => match stdio {
			tg::process::Stdio::Log => Ok(WriteBacking::Log),
			tg::process::Stdio::Null => Ok(WriteBacking::Null),
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Ok(WriteBacking::Messenger),
			tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit => {
				Err(tg::error!("invalid stdio"))
			},
		},
	}
}

fn has_ranged_stdio_read_arg(arg: &tg::process::stdio::read::Arg) -> bool {
	arg.position.is_some() || arg.length.is_some() || arg.size.is_some()
}

fn process_stdio(
	data: &tg::process::Data,
	stream: tg::process::stdio::Stream,
) -> &tg::process::Stdio {
	match stream {
		tg::process::stdio::Stream::Stdin => &data.stdin,
		tg::process::stdio::Stream::Stdout => &data.stdout,
		tg::process::stdio::Stream::Stderr => &data.stderr,
	}
}

fn validate_log_stream(
	stream: tg::process::stdio::Stream,
) -> tg::Result<tg::process::stdio::Stream> {
	match stream {
		tg::process::stdio::Stream::Stdout | tg::process::stdio::Stream::Stderr => Ok(stream),
		tg::process::stdio::Stream::Stdin => Err(tg::error!("invalid stdio stream")),
	}
}
