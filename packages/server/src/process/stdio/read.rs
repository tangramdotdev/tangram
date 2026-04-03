use {
	super::{process_stdio, validate_log_stream},
	crate::{Context, Server},
	bytes::Bytes,
	futures::{
		StreamExt as _, future,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, io::SeekFrom, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::Ext as _,
		task::{Stopper, Task},
	},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

enum ReadBacking {
	Empty,
	Log(Option<tg::process::stdio::Stream>),
	Messenger(BTreeSet<tg::process::stdio::Stream>),
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
		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if let Some(event_stream) = self
			.try_read_process_stdio_peer(id, arg.clone(), &peers)
			.await
			.map_err(|source| tg::error!(!source, "failed to read peer process stdio"))?
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
			.get_stream("processes.stdio".to_owned())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stdio stream"))?;
		let consumer_config = tangram_messenger::ConsumerConfig {
			deliver_policy: tangram_messenger::DeliverPolicy::All,
			ack_policy: tangram_messenger::AckPolicy::None,
			durable_name: None,
			filter_subjects: streams
				.iter()
				.map(|stream| format!("processes.stdio.{id}.{stream}"))
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
					Ok((subject, bytes)) => {
						let event =
							serde_json::from_slice::<tg::process::stdio::read::Event>(&bytes)
								.map_err(|source| {
									tg::error!(!source, "failed to deserialize the stdio event")
								})?;
						(subject, event)
					},
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

	async fn try_read_process_stdio_peer(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		peers: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>> {
		if peers.is_empty() {
			return Ok(None);
		}
		let arg = tg::process::stdio::read::Arg {
			local: None,
			remotes: None,
			..arg
		};
		for peer in peers {
			let client = self.get_peer_client(peer.clone()).await.map_err(
				|source| tg::error!(!source, peer = %peer, "failed to get the peer client"),
			)?;
			let stream = client
				.try_read_process_stdio_all(id, arg.clone())
				.await
				.map_err(
					|source| tg::error!(!source, peer = %peer, "failed to read the process stdio"),
				)?
				.map(futures::StreamExt::boxed);
			if let Some(stream) = stream {
				return Ok(Some(stream));
			}
		}
		Ok(None)
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
		for remote in remotes {
			let client = self.get_remote_client(remote.clone()).await.map_err(
				|source| tg::error!(!source, remote = %remote, "failed to get the remote client"),
			)?;
			let stream = client
				.try_read_process_stdio_all(id, arg.clone())
				.await
				.map_err(
					|source| tg::error!(!source, remote = %remote, "failed to read the process stdio"),
				)?
				.map(futures::StreamExt::boxed);
			if let Some(stream) = stream {
				return Ok(Some(stream));
			}
		}
		Ok(None)
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

		let stopper = request.extensions().get::<Stopper>().cloned().unwrap();
		let stopper_wait = async move { stopper.wait().await };
		let event_stream = event_stream.take_until(stopper_wait);

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

fn has_ranged_stdio_read_arg(arg: &tg::process::stdio::read::Arg) -> bool {
	arg.position.is_some() || arg.length.is_some() || arg.size.is_some()
}
