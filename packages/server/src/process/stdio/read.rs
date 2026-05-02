use {
	crate::{Context, Server, database::Database},
	futures::{
		StreamExt as _, future,
		stream::{self, BoxStream, FuturesUnordered},
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
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}

		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(stream) = self
					.try_read_process_stdio_local(context, id, arg.clone())
					.await
					.map_err(|source| tg::error!(!source, "failed to read local process stdio"))?
			{
				return Ok(Some(stream));
			}

			if let Some(stream) = self
				.try_read_process_stdio_regions(id, arg.clone(), &local.regions)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to read process stdio from another region")
				})? {
				return Ok(Some(stream));
			}
		}

		if let Some(stream) = self
			.try_read_process_stdio_remotes(id, arg, &locations.remotes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read process stdio from a remote"))?
		{
			return Ok(Some(stream));
		}

		Ok(None)
	}

	async fn try_read_process_stdio_local(
		&self,
		context: &Context,
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
			Source::Pipe(streams) => {
				self.try_read_process_stdio_pipe_local(context, id, &streams)
					.await?
			},
			Source::Log(streams) => {
				self.try_read_process_stdio_log_local(context, id, arg, streams)
					.await?
			},
			Source::Null => stream::once(future::ok(tg::process::stdio::read::Event::End)).boxed(),
		};
		Ok(Some(stream))
	}

	async fn try_read_process_stdio_log_local(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		streams: BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>> {
		let (sender, receiver) = async_channel::unbounded();
		let server = self.clone();
		let id = id.clone();
		let stopper = context.stopper.clone();
		let task = Task::spawn(move |_| async move {
			let result = server
				.try_read_process_stdio_log_local_task(&id, arg, streams, sender.clone(), stopper)
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
		stopper: Option<Stopper>,
	) -> tg::Result<()> {
		let subject = format!("processes.{id}.log");
		let log = self
			.messenger
			.subscribe::<()>(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();

		let subject = format!("processes.{id}.status");
		let status = self
			.messenger
			.subscribe::<()>(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();

		let interval = IntervalStream::new(tokio::time::interval(Duration::from_mins(1)))
			.map(|_| ())
			.boxed();

		let mut wakeups = stream::select_all([log, status, interval]).with_stopper(stopper);

		'outer: while wakeups.next().await.is_some() {
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
		}

		Ok(())
	}

	pub(crate) async fn try_read_process_stdio_pipe_local(
		&self,
		context: &Context,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>> {
		let (sender, receiver) =
			async_channel::unbounded::<tg::Result<tg::process::stdio::read::Event>>();
		let server = self.clone();
		let id = id.clone();
		let streams = streams.clone();
		let stopper = context.stopper.clone();
		let task = Task::spawn(move |_| async move {
			let result = server
				.try_read_process_stdio_pipe_local_task(&id, streams, sender.clone(), stopper)
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
		stopper: Option<Stopper>,
	) -> tg::Result<()> {
		let mut wakeups = Vec::with_capacity(streams.len() * 2 + 1);
		for stream in &streams {
			let subject = format!("processes.{id}.{stream}.write");
			let wakeup = self
				.messenger
				.subscribe_with_delivery::<()>(subject, Delivery::One)
				.await
				.map_err(|source| tg::error!(!source, "failed to subscribe"))?
				.map(|_| ())
				.boxed();
			wakeups.push(wakeup);
			let subject = format!("processes.{id}.{stream}.close");
			let wakeup = self
				.messenger
				.subscribe_with_delivery::<()>(subject, Delivery::One)
				.await
				.map_err(|source| tg::error!(!source, "failed to subscribe"))?
				.map(|_| ())
				.boxed();
			wakeups.push(wakeup);
		}
		let interval = Duration::from_secs(1);
		let interval = IntervalStream::new(tokio::time::interval(interval))
			.map(|_| ())
			.boxed();
		wakeups.push(interval);
		let wakeups = stream::select_all(wakeups).with_stopper(stopper);
		let mut wakeups = pin!(wakeups);
		while wakeups.next().await.is_some() {
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
		match &self.process_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(process_store) => {
				self.try_read_process_stdio_pipe_event_postgres(process_store, id, streams)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(process_store) => {
				self.try_read_process_stdio_pipe_event_sqlite(process_store, id, streams)
					.await
			},
		}
	}

	async fn try_read_process_stdio_regions(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		regions: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>> {
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
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(stream) = result? else {
			return Ok(None);
		};
		Ok(Some(stream))
	}

	async fn try_read_process_stdio_region(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		region: &str,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::stdio::read::Arg {
			location: Some(location.into()),
			..arg
		};
		let Some(stream) = client.try_read_process_stdio_all(id, arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to read the process stdio"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	async fn try_read_process_stdio_remotes(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>> {
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
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(stream) = result? else {
			return Ok(None);
		};
		Ok(Some(stream))
	}

	async fn try_read_process_stdio_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>> {
		let client = self.get_remote_client(remote.name.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::process::stdio::read::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			..arg
		};
		let Some(stream) = client.try_read_process_stdio_all(id, arg).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, "failed to read the process stdio"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
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
		let Some(stream) = self
			.try_read_process_stdio_with_context(context, &id, arg)
			.await?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

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
