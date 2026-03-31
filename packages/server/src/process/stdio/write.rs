use {
	super::{process_stdio, validate_log_stream},
	crate::{Context, Server},
	bytes::Bytes,
	futures::{
		FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future,
		stream::BoxStream,
	},
	std::{collections::BTreeSet, pin::pin},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Stopper},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::prelude::*,
	tangram_store::Store as _,
	tokio_stream::wrappers::ReceiverStream,
	tokio_util::task::AbortOnDropHandle,
};

enum WriteBacking {
	Log,
	Messenger,
	Null,
}

impl Server {
	pub async fn write_process_stdio_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
		stopper: Option<Stopper>,
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

				let result = if let Some(stopper) = stopper {
					let future = pin!(future);
					let stopper_wait = pin!(stopper.wait());
					match future::select(future, stopper_wait).await {
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

		let output = self
			.write_process_stdio_with_context(context, &id, arg, input, Some(stopper.clone()))
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
