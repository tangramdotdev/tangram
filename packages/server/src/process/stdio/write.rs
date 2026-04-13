use {
	crate::{Context, Server, database::Database},
	bytes::Bytes,
	futures::{
		StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	std::{collections::BTreeSet, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::Ext as _,
		task::{Stopper, Task},
	},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_log_store::Store as _,
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::{IntervalStream, ReceiverStream},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

enum Destination {
	Log,
	Pipe,
	Null,
}

pub(crate) enum WriteOutput {
	Written,
	Full,
	Closed,
}

impl Server {
	pub async fn try_write_process_stdio_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
		stopper: Option<Stopper>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>>> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}

		if Self::local(arg.local, arg.remotes.as_ref()) {
			return self
				.write_process_stdio_local(id, &arg.streams, input, stopper)
				.await;
		}

		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			return self
				.write_process_stdio_remote(id, &arg.streams, input, remote)
				.await;
		}

		Ok(None)
	}

	async fn write_process_stdio_local(
		&self,
		id: &tg::process::Id,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
		stopper: Option<Stopper>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>>> {
		let Some(output) = self
			.try_get_process_local(id, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?
		else {
			return Ok(None);
		};
		let data = output.data;
		let (sender, receiver) = tokio::sync::mpsc::channel(4);
		let task = Task::spawn({
			let server = self.clone();
			let data = data.clone();
			let id = id.clone();
			let streams = streams.to_owned();
			move |_| async move {
				let future = server.write_process_stdio_local_task(&id, data, &streams, input);
				let result = if let Some(stopper) = stopper {
					let future = pin!(future);
					let stopper = pin!(stopper.wait());
					match future::select(future, stopper).await {
						future::Either::Left((result, _)) => result,
						future::Either::Right(((), future)) => {
							sender
								.send(Ok(tg::process::stdio::write::Event::Stop))
								.await
								.inspect_err(
									|error| tracing::error!(%error, "failed to send stop event"),
								)
								.ok();
							let result = future.await;
							sender
								.send(Ok(tg::process::stdio::write::Event::End))
								.await
								.inspect_err(
									|error| tracing::error!(%error, "failed to send end event"),
								)
								.ok();
							result
						},
					}
				} else {
					future.await
				};
				if let Err(error) = result {
					sender.send(Err(error)).await.ok();
				}
				Ok::<_, tg::Error>(())
			}
		});
		let stream = ReceiverStream::new(receiver).attach(task).boxed();
		Ok(Some(stream))
	}

	async fn write_process_stdio_local_task(
		&self,
		id: &tg::process::Id,
		data: tg::process::Data,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<()> {
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
					match get_destination(&data, chunk.stream)? {
						Destination::Log => {
							let started_at = started_at
								.ok_or_else(|| tg::error!("expected the process to be started"))?;
							if data.status != tg::process::Status::Started {
								return Err(tg::error!("not found"));
							}
							let timestamp =
								time::OffsetDateTime::now_utc().unix_timestamp() - started_at;
							let stream =
								if matches!(chunk.stream, tg::process::stdio::Stream::Stdin) {
									return Err(tg::error!("invalid stdio stream"));
								} else {
									chunk.stream
								};
							let arg = tangram_log_store::PutProcessLogArg {
								bytes: chunk.bytes,
								process: id.clone(),
								stream,
								timestamp,
							};
							self.log_store
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
						Destination::Pipe => {
							self.write_process_stdio_chunk_local(id, chunk.stream, chunk.bytes)
								.await?;
						},
						Destination::Null => (),
					}
				},
				tg::process::stdio::read::Event::End => {
					for stream in &streams {
						if !matches!(get_destination(&data, *stream)?, Destination::Pipe) {
							continue;
						}
						self.close_process_stdio_local(id, *stream).await?;
					}
					return Ok(());
				},
			}
		}
		Ok(())
	}

	async fn write_process_stdio_chunk_local(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		bytes: Bytes,
	) -> tg::Result<()> {
		let mut wakeups = self
			.subscribe_process_stdio_write_wakeups_local(id, stream)
			.await?;
		loop {
			match self
				.try_write_process_stdio(id, stream, bytes.clone())
				.await?
			{
				WriteOutput::Written => {
					self.spawn_publish_process_stdio_write_message_task(id, stream);
					return Ok(());
				},
				WriteOutput::Full => {
					wakeups.next().await;
				},
				WriteOutput::Closed => {
					let error = tg::error!(%id, %stream, "the process stdio is closed");
					return Err(error);
				},
			}
		}
	}

	async fn subscribe_process_stdio_write_wakeups_local(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<BoxStream<'static, ()>> {
		let subject = format!("processes.{id}.{stream}.read");
		let read = self
			.messenger
			.subscribe::<()>(subject, Some("processes.stdio.write".into()))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());
		let subject = format!("processes.{id}.{stream}.close");
		let close = self
			.messenger
			.subscribe::<()>(subject, Some("processes.stdio.write".into()))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());
		let interval = Duration::from_secs(1);
		let interval = IntervalStream::new(tokio::time::interval(interval))
			.skip(1)
			.map(|_| ());
		let stream = stream::select_all([read.boxed(), close.boxed(), interval.boxed()]).boxed();
		Ok(stream)
	}

	async fn try_write_process_stdio(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		bytes: Bytes,
	) -> tg::Result<WriteOutput> {
		match &self.sandbox_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(sandbox_store) => {
				self.try_write_process_stdio_postgres(sandbox_store, id, stream, bytes)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(sandbox_store) => {
				self.try_write_process_stdio_sqlite(sandbox_store, id, stream, bytes)
					.await
			},
		}
	}

	async fn close_process_stdio_local(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<()> {
		self.try_close_process_stdio(id, stream).await?;
		self.spawn_publish_process_stdio_close_message_task(id, stream);
		Ok(())
	}

	async fn try_close_process_stdio(
		&self,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<()> {
		match &self.sandbox_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(sandbox_store) => {
				self.try_close_process_stdio_postgres(sandbox_store, id, stream)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(sandbox_store) => {
				self.try_close_process_stdio_sqlite(sandbox_store, id, stream)
					.await
			},
		}
	}

	async fn write_process_stdio_remote(
		&self,
		id: &tg::process::Id,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
		remote: String,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>>> {
		let client = self.get_remote_client(remote.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::process::stdio::write::Arg {
			streams: streams.to_vec(),
			..Default::default()
		};
		let stream = client
			.try_write_process_stdio(id, arg, input)
			.await
			.map_err(|source| tg::error!(!source, remote = %remote, "failed to write stdio"))?;
		Ok(stream.map(futures::StreamExt::boxed))
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
		let stopper = request.extensions().get::<Stopper>().cloned().unwrap();
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

		let Some(output) = self
			.try_write_process_stdio_with_context(context, &id, arg, input, Some(stopper.clone()))
			.await?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

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

fn get_destination(
	data: &tg::process::Data,
	stream: tg::process::stdio::Stream,
) -> tg::Result<Destination> {
	let stdio = match stream {
		tg::process::stdio::Stream::Stdin => &data.stdin,
		tg::process::stdio::Stream::Stdout => &data.stdout,
		tg::process::stdio::Stream::Stderr => &data.stderr,
	};
	match stream {
		tg::process::stdio::Stream::Stdin => match stdio {
			tg::process::Stdio::Null => Ok(Destination::Null),
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Ok(Destination::Pipe),
			tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit | tg::process::Stdio::Log => {
				Err(tg::error!("invalid stdio"))
			},
		},
		tg::process::stdio::Stream::Stdout => match stdio {
			tg::process::Stdio::Log => Ok(Destination::Log),
			tg::process::Stdio::Null => Ok(Destination::Null),
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Ok(Destination::Pipe),
			tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit => {
				Err(tg::error!("invalid stdio"))
			},
		},
		tg::process::stdio::Stream::Stderr => match stdio {
			tg::process::Stdio::Log => Ok(Destination::Log),
			tg::process::Stdio::Null => Ok(Destination::Null),
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Ok(Destination::Pipe),
			tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit => {
				Err(tg::error!("invalid stdio"))
			},
		},
	}
}
