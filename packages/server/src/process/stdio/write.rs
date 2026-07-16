use {
	crate::Session,
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	std::{collections::BTreeSet, pin::pin},
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
	tokio_stream::wrappers::ReceiverStream,
};

enum Destination {
	Log,
	Pipe,
	Null,
}

impl Session {
	pub async fn try_write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>>> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}

		let location = self.server.location(arg.location.as_ref())?;

		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_write_process_stdio_local(
					id,
					&arg.streams,
					input,
					self.context.stopper.clone(),
					arg.token.as_ref(),
				)
				.await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.try_write_process_stdio_region(id, &arg, input, region)
					.await?
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.try_write_process_stdio_remote(id, &arg, input, remote, region)
					.await?
			},
		};

		Ok(output)
	}

	async fn try_write_process_stdio_local(
		&self,
		id: &tg::process::Id,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
		stopper: Option<Stopper>,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>>> {
		let Some(tg::process::get::Output { data, .. }) = self
			.try_get_process_local(id, false, token)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process"))?
		else {
			return Ok(None);
		};

		self.authorize_process_stdio_write(id, streams, token)
			.await?;

		if data.status.is_finished() {
			return Ok(Some(
				futures::stream::once(future::ok(tg::process::stdio::write::Event::End)).boxed(),
			));
		}

		// Spawn the write task.
		let (sender, receiver) = tokio::sync::mpsc::channel(4);
		let finished = Stopper::new();
		let write_task = Task::spawn({
			let session = self.clone();
			let data = data.clone();
			let id = id.clone();
			let streams = streams.to_owned();
			let finished = finished.clone();
			move |_| async move {
				let future = session.write_process_stdio_local_task(&id, data, &streams, input);
				let mut future = pin!(future);
				let stopper_wait = async move {
					match stopper {
						Some(stopper) => stopper.wait().await,
						None => future::pending::<()>().await,
					}
				};
				let result = tokio::select! {
					result = &mut future => result,
					() = stopper_wait => {
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
					() = finished.wait() => {
						sender
							.send(Ok(tg::process::stdio::write::Event::End))
							.await
							.inspect_err(
								|error| tracing::error!(%error, "failed to send end event"),
							)
							.ok();
						Ok(())
					},
				};
				if let Err(error) = result {
					sender.send(Err(error)).await.ok();
				}
				Ok::<_, tg::Error>(())
			}
		});

		// Spawn the status task if necessary.
		let status_task = if streams.contains(&tg::process::stdio::Stream::Stdin) {
			Some(Task::spawn({
				let session = self.clone();
				let id = id.clone();
				let finished = finished.clone();
				move |stopper| async move {
					let stream = session
						.try_get_process_status_stream_local(&id, Some(stopper), None)
						.await
						.map_err(|error| tg::error!(!error, "failed to get the status stream"))?;
					if let Some(stream) = stream {
						let mut stream = pin!(stream);
						while let Some(event) = stream.try_next().await? {
							match event {
								tg::process::status::Event::Status(status)
									if status.is_finished() =>
								{
									break;
								},
								tg::process::status::Event::End => break,
								tg::process::status::Event::Status(_) => (),
							}
						}
					}
					finished.stop();
					Ok::<_, tg::Error>(())
				}
			}))
		} else {
			None
		};

		let stream = ReceiverStream::new(receiver).attach(write_task);
		let stream = match status_task {
			Some(status_task) => stream.attach(status_task).boxed(),
			None => stream.boxed(),
		};

		Ok(Some(stream))
	}

	async fn authorize_process_stdio_write(
		&self,
		id: &tg::process::Id,
		streams: &[tg::process::stdio::Stream],
		token: Option<&tg::grant::Token>,
	) -> tg::Result<()> {
		let stdin = streams.contains(&tg::process::stdio::Stream::Stdin);
		let output = streams
			.iter()
			.any(|stream| !matches!(stream, tg::process::stdio::Stream::Stdin));
		match (stdin, output) {
			(_, false) => {
				let permission = tg::grant::Permission::Process(
					tg::grant::permission::process::Permission::Write,
				);
				let resource = token.map_or_else(
					|| tg::Either::Left(id.clone()),
					|token| {
						tg::Either::Right(tg::WithToken {
							id: id.clone(),
							token: token.clone(),
						})
					},
				);
				let authorized = self.authorize(resource, permission).await?;
				if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
					return Err(tg::error!("unauthorized"));
				}
				Ok(())
			},
			(false, true) => {
				let authorized = matches!(
					&self.context.principal,
					tg::Principal::Process(process) if process == id
				);
				if !authorized {
					return Err(tg::error!("unauthorized"));
				}
				Ok(())
			},
			(true, true) => Err(tg::error!(
				"cannot write stdin and stdout or stderr in a single request"
			)),
		}
	}

	async fn write_process_stdio_local_task(
		&self,
		id: &tg::process::Id,
		data: tg::process::Data,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> tg::Result<()> {
		if data.status.is_finished() {
			return Ok(());
		}
		let started_at = data.started_at;
		let streams = streams.iter().copied().collect::<BTreeSet<_>>();
		let mut input = pin!(input);
		while let Some(result) = input.next().await {
			let event = match result {
				Ok(event) => event,
				Err(error) => {
					return Err(tg::error!(!error, "failed to read a stdio event"));
				},
			};
			match event {
				tg::process::stdio::read::Event::Chunk(mut chunk) => {
					if chunk.bytes.is_empty()
						&& !matches!(chunk.stream, tg::process::stdio::Stream::Stdin)
					{
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
							let arg = tangram_log_store::PutArg {
								bytes: chunk.bytes,
								process: id.clone(),
								stream,
								timestamp,
							};
							self.server
								.log_store
								.put(arg)
								.await
								.map_err(|error| tg::error!(!error, "failed to store the log"))?;
							tokio::spawn({
								let session = self.clone();
								let id = id.clone();
								async move {
									session
										.server
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
							let length = self
								.write_process_stdio_chunk_local(id, chunk.stream, chunk.bytes)
								.await?;
							if length == 0 {
								return Ok(());
							}
						},
						Destination::Null => (),
					}
				},
				tg::process::stdio::read::Event::End => {
					if streams.contains(&tg::process::stdio::Stream::Stdin)
						&& matches!(
							get_destination(&data, tg::process::stdio::Stream::Stdin),
							Ok(Destination::Pipe)
						) {
						self.write_process_stdio_chunk_local(
							id,
							tg::process::stdio::Stream::Stdin,
							Bytes::new(),
						)
						.await?;
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
	) -> tg::Result<usize> {
		let request = tg::process::control::ServerRequestArg::Write(
			tg::process::control::WriteServerRequestArg { stream, bytes },
		);
		let retry = tangram_futures::retry::Options {
			max_retries: u64::MAX,
			..Default::default()
		};
		let timeout = self
			.server
			.config
			.runner
			.as_ref()
			.map_or(std::time::Duration::from_secs(10), |runner| {
				runner.stdio_drain_timeout
			});
		let options = crate::control::Options { retry, timeout };
		let response = self
			.send_process_control_request(id, request, options)
			.await??;
		let response = response
			.try_unwrap_write()
			.map_err(|_| tg::error!("expected a write response"))?;
		Ok(response.length)
	}

	async fn try_write_process_stdio_region(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
		region: String,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>>> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::process::stdio::write::Arg {
			location: Some(location.into()),
			streams: arg.streams.clone(),
			token: arg.token.clone(),
		};
		let stream = client
			.try_write_process_stdio(id, arg, input)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to write stdio"))?;
		Ok(stream.map(futures::StreamExt::boxed))
	}

	async fn try_write_process_stdio_remote(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Event>>>> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::process::stdio::write::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			streams: arg.streams.clone(),
			token: arg.token.clone(),
		};
		let stream = client
			.try_write_process_stdio(id, arg, input)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to write stdio"))?;
		Ok(stream.map(futures::StreamExt::boxed))
	}

	pub(crate) async fn try_write_process_stdio_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

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
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;
		let arg: tg::process::stdio::write::Arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let input = request
			.sse()
			.map_err(|error| tg::error!(!error, "failed to read an event"))
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

		let Some(output) = self.try_write_process_stdio(&id, arg, input).await? else {
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
