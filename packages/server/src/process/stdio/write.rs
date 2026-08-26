use {
	crate::Session,
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, future, stream::BoxStream},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, pin::pin, time::Duration},
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
	tangram_store::{Store as _, log},
	tokio_stream::wrappers::ReceiverStream,
};

const LOG_BATCH_DELAY: Duration = Duration::from_millis(5);
const LOG_BATCH_MAX_CHUNKS: usize = 64;
const LOG_BATCH_SIZE: usize = 32 * 1024;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Destination {
	Log,
	Null,
	Pipe,
}

impl Session {
	pub async fn try_write_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::ServerMessage>>>>
	{
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
					arg.tokens.local(),
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
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
		stopper: Option<Stopper>,
		token: Option<&tg::authorization::Token>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::ServerMessage>>>>
	{
		let Some(tg::process::get::Output { data, .. }) = self
			.try_get_process_local(id, false, false, token)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process"))?
		else {
			return Ok(None);
		};
		self.authorize_process_stdio_write(id, streams, token)
			.await?;
		if data.status.is_finished() {
			let message = tg::process::stdio::write::ServerMessage::Response(
				tg::process::stdio::write::ServerResponse::End,
			);
			let stream = futures::stream::once(future::ok(message)).boxed();

			return Ok(Some(stream));
		}

		let (sender, receiver) = tokio::sync::mpsc::channel(4);
		let task = Task::spawn({
			let session = self.clone();
			let id = id.clone();
			let streams = streams.to_owned();
			move |_| async move {
				let mut future = Box::pin(
					session.write_process_stdio_local_task(&id, data, &streams, input, &sender),
				);
				let result = match stopper {
					Some(stopper) => {
						tokio::select! {
							result = &mut future => result,
							() = stopper.wait() => {
								let message = tg::process::stdio::write::ServerMessage::Notification(
									tg::process::stdio::write::ServerNotification::Stop,
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
			}
		});
		let stream = ReceiverStream::new(receiver).attach(task).boxed();

		Ok(Some(stream))
	}

	async fn authorize_process_stdio_write(
		&self,
		id: &tg::process::Id,
		streams: &[tg::process::stdio::Stream],
		token: Option<&tg::authorization::Token>,
	) -> tg::Result<()> {
		let stdin = streams.contains(&tg::process::stdio::Stream::Stdin);
		let output = streams
			.iter()
			.any(|stream| !matches!(stream, tg::process::stdio::Stream::Stdin));
		match (stdin, output) {
			(_, false) => {
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
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::process::stdio::write::ServerMessage>>,
	) -> tg::Result<()> {
		let destinations = streams
			.iter()
			.map(|&stream| get_destination(&data, stream))
			.collect::<tg::Result<Vec<_>>>()?;
		let logs = destinations
			.iter()
			.all(|destination| *destination == Destination::Log);
		let streams = streams.iter().copied().collect::<BTreeSet<_>>();
		if logs {
			self.write_process_stdio_log_local_task(id, &data, &streams, input, sender)
				.await?;
		} else {
			self.write_process_stdio_other_local_task(id, &data, &streams, input, sender)
				.await?;
		}

		Ok(())
	}

	async fn write_process_stdio_log_local_task(
		&self,
		id: &tg::process::Id,
		data: &tg::process::Data,
		streams: &BTreeSet<tg::process::stdio::Stream>,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::process::stdio::write::ServerMessage>>,
	) -> tg::Result<()> {
		if data.status != tg::process::Status::Started {
			return Err(tg::error!("not found"));
		}
		let combined = streams.len() > 1;
		let input =
			tokio_stream::StreamExt::chunks_timeout(input, LOG_BATCH_MAX_CHUNKS, LOG_BATCH_DELAY);
		let mut input = pin!(input);
		while let Some(messages) = input.next().await {
			let mut args = Vec::with_capacity(messages.len());
			let mut batch_length = 0_usize;
			let mut end = false;
			let mut position = None;
			for result in messages {
				let message =
					result.map_err(|error| tg::error!(!error, "failed to read a stdio message"))?;
				match message {
					tg::process::stdio::write::ClientMessage::Notification(
						tg::process::stdio::write::ClientNotification::Chunk(chunk),
					) => {
						if !streams.contains(&chunk.stream) {
							return Err(tg::error!(
								stream = %chunk.stream,
								"received an unexpected stdio stream"
							));
						}
						let timestamp = chunk
							.timestamp
							.ok_or_else(|| tg::error!("missing the log timestamp"))?;
						let length = chunk.bytes.len();
						if !args.is_empty() && batch_length.saturating_add(length) > LOG_BATCH_SIZE
						{
							self.put_process_log_batch_local(id, std::mem::take(&mut args))
								.await?;
							if let Some(position) = position {
								send_write_notification(sender, position).await;
							}
							batch_length = 0;
						}
						let chunk_position = if combined {
							chunk.combined_position
						} else {
							chunk.stream_position
						};
						position = Some(
							chunk_position
								.checked_add(length.to_u64().unwrap())
								.ok_or_else(|| tg::error!("the stdio position is too large"))?,
						);
						let arg = log::put::Arg {
							bytes: chunk.bytes,
							position: chunk.combined_position,
							process: id.clone(),
							stream: chunk.stream,
							stream_position: chunk.stream_position,
							timestamp,
						};
						args.push(arg);
						batch_length = batch_length.saturating_add(length);
					},
					tg::process::stdio::write::ClientMessage::Request(
						tg::process::stdio::write::ClientRequest::End,
					) => end = true,
				}
			}
			self.put_process_log_batch_local(id, args).await?;
			if let Some(position) = position {
				send_write_notification(sender, position).await;
			}
			if end {
				send_end_response(sender).await;

				return Ok(());
			}
		}

		Err(tg::error!(
			"the stdio write stream ended before the end request"
		))
	}

	async fn put_process_log_batch_local(
		&self,
		id: &tg::process::Id,
		args: Vec<log::put::Arg>,
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		self.server
			.store
			.put_log_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the log"))?;
		self.server.log_notifications.notify(id);

		Ok(())
	}

	async fn write_process_stdio_other_local_task(
		&self,
		id: &tg::process::Id,
		data: &tg::process::Data,
		streams: &BTreeSet<tg::process::stdio::Stream>,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::process::stdio::write::ServerMessage>>,
	) -> tg::Result<()> {
		let combined = streams.len() > 1;
		let mut input = pin!(input);
		let mut position = 0;
		while let Some(message) = input.try_next().await? {
			match message {
				tg::process::stdio::write::ClientMessage::Notification(
					tg::process::stdio::write::ClientNotification::Chunk(chunk),
				) => {
					if !streams.contains(&chunk.stream) {
						return Err(tg::error!(
							stream = %chunk.stream,
							"received an unexpected stdio stream"
						));
					}
					let start = if combined {
						chunk.combined_position
					} else {
						chunk.stream_position
					};
					let end = start
						.checked_add(chunk.bytes.len().to_u64().unwrap())
						.ok_or_else(|| tg::error!("the stdio position is too large"))?;
					position = match get_destination(data, chunk.stream)? {
						Destination::Log => {
							let timestamp = chunk
								.timestamp
								.ok_or_else(|| tg::error!("missing the log timestamp"))?;
							let arg = log::put::Arg {
								bytes: chunk.bytes,
								position: chunk.combined_position,
								process: id.clone(),
								stream: chunk.stream,
								stream_position: chunk.stream_position,
								timestamp,
							};
							self.put_process_log_batch_local(id, vec![arg]).await?;
							end
						},
						Destination::Null => end,
						Destination::Pipe => {
							self.write_process_stdio_chunk_local(id, chunk).await?
						},
					};
					send_write_notification(sender, position).await;
				},
				tg::process::stdio::write::ClientMessage::Request(
					tg::process::stdio::write::ClientRequest::End,
				) => {
					if streams.contains(&tg::process::stdio::Stream::Stdin)
						&& matches!(
							get_destination(data, tg::process::stdio::Stream::Stdin),
							Ok(Destination::Pipe)
						) {
						let chunk = tg::process::stdio::Chunk {
							bytes: Bytes::new(),
							combined_position: position,
							stream: tg::process::stdio::Stream::Stdin,
							stream_position: position,
							timestamp: None,
						};
						position = self.write_process_stdio_chunk_local(id, chunk).await?;
						send_write_notification(sender, position).await;
					}
					send_end_response(sender).await;

					return Ok(());
				},
			}
		}

		Err(tg::error!(
			"the stdio write stream ended before the end request"
		))
	}

	async fn write_process_stdio_chunk_local(
		&self,
		id: &tg::process::Id,
		chunk: tg::process::stdio::Chunk,
	) -> tg::Result<u64> {
		let arg = tg::process::control::WriteServerRequestArg { chunk };
		let request = tg::process::control::ServerRequestArg::Write(arg);
		let retry = tangram_futures::retry::Options {
			max_retries: u64::MAX,
			..Default::default()
		};
		let timeout = if self
			.server
			.config
			.roles
			.contains(&crate::config::Role::Runner)
		{
			self.server.config.runner.stdio_drain_timeout
		} else {
			Duration::from_secs(10)
		};
		let options = crate::control::Options { retry, timeout };
		let response = self
			.send_process_control_request(id, request, options)
			.await??;
		let response = response
			.try_unwrap_write()
			.map_err(|_| tg::error!("expected a write response"))?;

		Ok(response.position)
	}

	async fn try_write_process_stdio_region(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
		region: String,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::ServerMessage>>>>
	{
		let client = self.get_region_session_for_process(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::process::stdio::write::Arg {
			location: Some(location.clone().into()),
			streams: arg.streams.clone(),
			tokens: arg.tokens.for_location(&location),
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
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::ServerMessage>>>>
	{
		let client = self.get_remote_session_for_process(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
		)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.clone(),
			region: region.clone(),
		});
		let arg = tg::process::stdio::write::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			streams: arg.streams.clone(),
			tokens: arg.tokens.for_location(&location),
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
		let arg: tg::process::stdio::write::Arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let max_frame_size = self.server.config.sync.max_frame_size;
		let input = super::decode(request, input_encoding, max_frame_size);
		let Some(output) = self.try_write_process_stdio(&id, arg, input).await? else {
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

async fn send_end_response(
	sender: &tokio::sync::mpsc::Sender<tg::Result<tg::process::stdio::write::ServerMessage>>,
) {
	let message = tg::process::stdio::write::ServerMessage::Response(
		tg::process::stdio::write::ServerResponse::End,
	);
	sender.send(Ok(message)).await.ok();
}

async fn send_write_notification(
	sender: &tokio::sync::mpsc::Sender<tg::Result<tg::process::stdio::write::ServerMessage>>,
	position: u64,
) {
	let message = tg::process::stdio::write::ServerMessage::Notification(
		tg::process::stdio::write::ServerNotification::Write { position },
	);
	sender.send(Ok(message)).await.ok();
}

fn get_destination(
	data: &tg::process::Data,
	stream: tg::process::stdio::Stream,
) -> tg::Result<Destination> {
	let stdio = match stream {
		tg::process::stdio::Stream::Stderr => &data.stderr,
		tg::process::stdio::Stream::Stdin => &data.stdin,
		tg::process::stdio::Stream::Stdout => &data.stdout,
	};
	match stream {
		tg::process::stdio::Stream::Stderr => match stdio {
			tg::process::Stdio::Log => Ok(Destination::Log),
			tg::process::Stdio::Null => Ok(Destination::Null),
			tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Ok(Destination::Pipe),
			tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit => {
				Err(tg::error!("invalid stdio"))
			},
		},
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
	}
}
