use {
	crate::{Context, Server, database::Database},
	futures::{
		StreamExt as _, future,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, io::SeekFrom, time::Duration},
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

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

enum Source {
	Pipe(BTreeSet<tg::process::stdio::Stream>),
	Log(BTreeSet<tg::process::stdio::Stream>),
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
		let source = Self::get_process_stdio_source(&output.data, &arg)?;
		let stream = match source {
			Source::Pipe(streams) => self.try_read_process_stdio_pipe_local(id, &streams).await?,
			Source::Log(streams) => {
				self.try_read_process_stdio_log_local(id, arg, streams)
					.await?
			},
			Source::Null => stream::once(future::ok(tg::process::stdio::read::Event::End)).boxed(),
		};
		Ok(Some(stream))
	}

	async fn try_read_process_stdio_log_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		streams: BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>> {
		let (sender, receiver) = async_channel::unbounded();
		let server = self.clone();
		let id = id.clone();
		let task = Task::spawn(move |_| async move {
			let result = server
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
				.get_process_status_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the process status"))?;

			let mut stream = self
				.process_log_stream(id, arg.position, arg.length, arg.size, streams.clone())
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

	pub(crate) async fn try_read_process_stdio_pipe_local(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>> {
		let (sender, receiver) =
			async_channel::unbounded::<tg::Result<tg::process::stdio::read::Event>>();
		let server = self.clone();
		let id = id.clone();
		let streams = streams.clone();
		let task = Task::spawn(move |_| async move {
			let result = server
				.try_read_process_stdio_pipe_local_task(&id, streams, sender.clone())
				.await;
			if let Err(error) = result {
				sender.try_send(Err(error)).ok();
			}
		});
		Ok(receiver.attach(task).boxed())
	}

	async fn try_read_process_stdio_pipe_local_task(
		&self,
		id: &tg::process::Id,
		streams: BTreeSet<tg::process::stdio::Stream>,
		sender: async_channel::Sender<tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<()> {
		let mut streams_ = Vec::with_capacity(streams.len() + 1);
		for stream in &streams {
			let subject = format!("processes.{id}.{stream}.write");
			let stream = self
				.messenger
				.subscribe::<()>(subject, Some("processes.stdio.read".into()))
				.await
				.map_err(|source| tg::error!(!source, "failed to subscribe"))?
				.map(|_| ())
				.boxed();
			streams_.push(stream);
		}
		let interval = IntervalStream::new(tokio::time::interval(Duration::from_secs(1)))
			.map(|_| ())
			.boxed();
		streams_.push(interval);
		let mut streams_ = stream::select_all(streams_).boxed();
		while let Some(()) = streams_.next().await {
			loop {
				match self.try_read_process_stdio_pipe_event(id, &streams).await {
					Ok(Some(event)) => {
						let end = matches!(event, tg::process::stdio::read::Event::End);
						if let tg::process::stdio::read::Event::Chunk(chunk) = &event {
							self.spawn_publish_process_stdio_read_message_task(id, chunk.stream);
						}
						if sender.try_send(Ok(event)).is_err() {
							return Ok(());
						}
						if end {
							return Ok(());
						}
					},
					Ok(None) => break,
					Err(error) => {
						tracing::error!(
							error = %error.trace(),
							%id,
							"failed to read the process stdio event"
						);
						tokio::time::sleep(Duration::from_secs(1)).await;
						break;
					},
				}
			}
		}
		Ok(())
	}

	async fn try_read_process_stdio_pipe_event(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<tg::process::stdio::read::Event>> {
		match &self.sandbox_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(sandbox_store) => {
				self.try_read_process_stdio_pipe_event_postgres(sandbox_store, id, streams)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(sandbox_store) => {
				self.try_read_process_stdio_pipe_event_sqlite(sandbox_store, id, streams)
					.await
			},
		}
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

	fn get_process_stdio_source(
		data: &tg::process::Data,
		arg: &tg::process::stdio::read::Arg,
	) -> tg::Result<Source> {
		let mut log_streams = BTreeSet::new();
		let mut pipe_streams = BTreeSet::new();
		for stream in &arg.streams {
			let stdio = match stream {
				tg::process::stdio::Stream::Stdin => &data.stdin,
				tg::process::stdio::Stream::Stdout => &data.stdout,
				tg::process::stdio::Stream::Stderr => &data.stderr,
			};
			match stdio {
				tg::process::Stdio::Log => {
					if matches!(stream, tg::process::stdio::Stream::Stdin) {
						return Err(tg::error!("invalid stdio stream"));
					}
					log_streams.insert(*stream);
				},
				tg::process::Stdio::Pipe | tg::process::Stdio::Tty => {
					pipe_streams.insert(*stream);
				},
				tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit => {
					return Err(tg::error!("invalid stdio"));
				},
				tg::process::Stdio::Null => (),
			}
		}
		if !log_streams.is_empty() && !pipe_streams.is_empty() {
			return Err(tg::error!(
				"cannot read logged and piped stdio in a single request"
			));
		}
		if !pipe_streams.is_empty() {
			if arg.position.is_some() || arg.length.is_some() || arg.size.is_some() {
				return Err(tg::error!(
					"position, length, and size are only valid for logged stdio"
				));
			}
			return Ok(Source::Pipe(pipe_streams));
		}
		if log_streams.is_empty() {
			return Ok(Source::Null);
		}
		Ok(Source::Log(log_streams))
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
		let stopper = async move { stopper.wait().await };
		let stream = event_stream.take_until(stopper);

		let content_type = mime::TEXT_EVENT_STREAM;
		let stream = stream.map(|result| match result {
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
