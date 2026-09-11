use {
	crate::Session,
	bytes::Bytes,
	futures::{
		StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture},
		stream::BoxStream,
	},
	num::ToPrimitive as _,
	std::{pin::pin, time::Duration},
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
	tokio_stream::wrappers::ReceiverStream,
};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Destination {
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
		tokens: &[tg::authorization::Token],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::ServerMessage>>>>
	{
		let Some(tg::process::get::Output { data, location, .. }) = self
			.try_get_process_local(id, false, false, tokens)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process"))?
		else {
			return Ok(None);
		};
		if location.as_ref().is_some_and(tg::Location::is_remote)
			&& streams.contains(&tg::process::stdio::Stream::Stdin)
			&& get_stdin_destination(&data)? == Destination::Pipe
		{
			return Ok(None);
		}
		self.authorize_process_stdio_write(id, streams, tokens)
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
					session.write_process_stdio_local_task(&id, &data, &streams, input, &sender),
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
		tokens: &[tg::authorization::Token],
	) -> tg::Result<()> {
		let stdin = streams.contains(&tg::process::stdio::Stream::Stdin);
		let output = streams
			.iter()
			.any(|stream| !matches!(stream, tg::process::stdio::Stream::Stdin));
		match (stdin, output) {
			(true, false) => {
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
			(false, false) => Err(tg::error!("expected at least one stdio stream")),
			(_, true) => Err(tg::error!("cannot write process stdout or stderr")),
		}
	}

	async fn write_process_stdio_local_task(
		&self,
		id: &tg::process::Id,
		data: &tg::process::Data,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::process::stdio::write::ServerMessage>>,
	) -> tg::Result<()> {
		let destination = get_stdin_destination(data)?;
		let mut wait = if destination == Destination::Pipe {
			Some(self.create_wait_process_finished_future_local(id).await?)
		} else {
			None
		};
		let mut input = pin!(input);
		while let Some(message) = input.try_next().await? {
			match message {
				tg::process::stdio::write::ClientMessage::Notification(notification) => {
					match notification {}
				},
				tg::process::stdio::write::ClientMessage::Request(
					tg::process::stdio::write::ClientRequest::Chunk(chunk),
				) => {
					if !streams.contains(&chunk.stream) {
						return Err(tg::error!(
							stream = %chunk.stream,
							"received an unexpected stdio stream"
						));
					}
					let length = chunk.bytes.len().to_u64().unwrap();
					let output = match destination {
						Destination::Null => tg::process::control::WriteClientResponseOutput {
							closed: false,
							length,
						},
						Destination::Pipe => {
							let wait = wait
								.as_mut()
								.ok_or_else(|| tg::error!("missing the process wait future"))?;
							self.write_process_stdin_chunk_local(id, chunk, wait)
								.await?
						},
					};
					send_write_response(sender, output).await;
					if output.closed {
						send_end_response(sender).await;

						return Ok(());
					}
				},
				tg::process::stdio::write::ClientMessage::Request(
					tg::process::stdio::write::ClientRequest::End { position },
				) => {
					if destination == Destination::Pipe {
						let chunk = tg::process::stdio::Chunk {
							bytes: Bytes::new(),
							combined_position: position,
							stream: tg::process::stdio::Stream::Stdin,
							stream_position: position,
							timestamp: None,
						};
						let wait = wait
							.as_mut()
							.ok_or_else(|| tg::error!("missing the process wait future"))?;
						self.write_process_stdin_chunk_local(id, chunk, wait)
							.await?;
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

	async fn write_process_stdin_chunk_local(
		&self,
		id: &tg::process::Id,
		chunk: tg::process::stdio::Chunk,
		wait: &mut BoxFuture<'static, tg::Result<()>>,
	) -> tg::Result<tg::process::control::WriteClientResponseOutput> {
		crate::checkpoint!(
			self.server,
			"process.stdio.write.request",
			close = %chunk.bytes.is_empty(),
			stream = %chunk.stream,
		)
		.await;

		let response = self.write_process_stdio_chunk_local(id, chunk);
		let mut response = std::pin::pin!(response);
		let output = tokio::select! {
			biased;
			result = wait.as_mut() => {
				result?;
				tg::process::control::WriteClientResponseOutput {
					closed: true,
					length: 0,
				}
			},
			response = &mut response => response?,
		};

		Ok(output)
	}

	async fn write_process_stdio_chunk_local(
		&self,
		id: &tg::process::Id,
		chunk: tg::process::stdio::Chunk,
	) -> tg::Result<tg::process::control::WriteClientResponseOutput> {
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

		Ok(response)
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
		let content_type = output_encoding.content_type(tangram_content_type);
		let body = super::encode(output, output_encoding, max_frame_size);
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
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

async fn send_write_response(
	sender: &tokio::sync::mpsc::Sender<tg::Result<tg::process::stdio::write::ServerMessage>>,
	output: tg::process::stdio::write::Output,
) {
	let message = tg::process::stdio::write::ServerMessage::Response(
		tg::process::stdio::write::ServerResponse::Write(output),
	);
	sender.send(Ok(message)).await.ok();
}

fn get_stdin_destination(data: &tg::process::Data) -> tg::Result<Destination> {
	match &data.stdin {
		tg::process::Stdio::Null => Ok(Destination::Null),
		tg::process::Stdio::Pipe | tg::process::Stdio::Tty => Ok(Destination::Pipe),
		tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit | tg::process::Stdio::Log => {
			Err(tg::error!("invalid stdio"))
		},
	}
}
