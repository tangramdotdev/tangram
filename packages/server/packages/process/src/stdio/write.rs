use {
	crate::Component,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture},
		stream::BoxStream,
	},
	std::{ops::ControlFlow, time::Duration},
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
	tangram_server_core::Session,
	tg::process::stdio::write::{Data, Output},
	tokio_stream::wrappers::ReceiverStream,
};

mod log;
mod relay;
#[cfg(test)]
mod tests;

pub(crate) struct TryWriteProcessStdioLocalArg<'a> {
	pub(crate) id: &'a tg::process::Id,
	pub(crate) input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
	pub(crate) open: Option<String>,
	pub(crate) stopper: Option<Stopper>,
	pub(crate) streams: &'a [tg::process::stdio::Stream],
	pub(crate) tokens: &'a [tg::authorization::Token],
}

struct WriteProcessStdioWithControlArg<'a> {
	control_sender: Option<tangram_server_core::process::control::local::Local>,
	data: tg::process::Data,
	id: &'a tg::process::Id,
	input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
	open: Option<String>,
	stopper: Option<Stopper>,
	streams: &'a [tg::process::stdio::Stream],
}

struct WriteProcessStdioLocalTaskArg<'a> {
	control_sender: Option<&'a tangram_server_core::process::control::local::Local>,
	data: &'a tg::process::Data,
	id: &'a tg::process::Id,
	input: BoxStream<'static, tg::Result<Data>>,
	open: Option<String>,
	sender: &'a tokio::sync::mpsc::Sender<tg::Result<Output>>,
	streams: &'a [tg::process::stdio::Stream],
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Destination {
	Null,
	Pipe,
}

impl Component {
	pub async fn try_write_process_stdio(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		mut arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
	) -> tg::Result<
		ControlFlow<
			Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Output>>>,
			tg::Error,
		>,
	> {
		// Resolve the process, claiming the connect's open for the write.
		let mut process = process.clone();
		let Some(output) = self
			.try_resolve_process(
				session,
				&mut process,
				&mut arg.location,
				Some(&mut arg.tokens),
				&arg.streams.clone(),
			)
			.await?
		else {
			return Ok(ControlFlow::Break(None));
		};
		let open = output.open;
		let id = process.clone().right();
		let process = &process;
		Self::validate_process_stdio_write_streams(&arg.streams)?;
		let stdin = arg.streams.contains(&tg::process::stdio::Stream::Stdin);
		if stdin
			&& let Some(id) = &id
			&& let Some(control) = self
				.try_get_process_control_runner(
					session,
					id,
					arg.location.as_ref(),
					&arg.tokens,
					tg::authorization::permission::process::Set::PARENT,
				)
				.await?
		{
			let write_arg = WriteProcessStdioWithControlArg {
				control_sender: Some(control.control_sender),
				data: control.data,
				id,
				input,
				open: None,
				stopper: session.context.stopper.clone(),
				streams: &arg.streams,
			};
			let stream = self.write_process_stdio_with_control(session, write_arg);
			return Ok(ControlFlow::Break(Some(stream)));
		}
		let location = session.server.location(arg.location.as_ref())?;
		let output = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				self.try_write_process_stdio_region(session, process, &arg, input, region)
					.await?
			},
			tg::Location::Local(_) => {
				let Some(id) = &id else {
					return Ok(ControlFlow::Break(None));
				};
				let output = if let Some(relay) = arg.id.as_deref() {
					self.write_process_stdio_relay_local(session, id, relay, &arg.streams, input)?
				} else if stdin {
					let arg = TryWriteProcessStdioLocalArg {
						id,
						input,
						open,
						stopper: session.context.stopper.clone(),
						streams: &arg.streams,
						tokens: arg.tokens.local_authorization(),
					};
					self.try_write_process_stdio_local(session, arg).await?
				} else {
					self.try_write_process_stdio_log_local(session, id, &arg.streams, input)
						.await?
				};
				ControlFlow::Break(output)
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.try_write_process_stdio_remote(session, process, &arg, input, remote, region)
					.await?
			},
		};

		Ok(output)
	}

	pub(crate) async fn try_write_process_stdio_local(
		&self,
		session: &Session,
		arg: TryWriteProcessStdioLocalArg<'_>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Output>>>> {
		let TryWriteProcessStdioLocalArg {
			id,
			input,
			open,
			stopper,
			streams,
			tokens,
		} = arg;
		let Some(tg::process::get::Output { data, location, .. }) = self
			.try_get_process_local(session, id, false, false, tokens, tg::process::Source::Auto)
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
		Self::validate_process_stdio_write_streams(streams)?;
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
		);
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let authorized = session
			.authorize(resource, permission)
			.await?
			.check_exhaustion()?;
		if !authorized.permissions.contains(permission) {
			return Err(tg::error!("unauthorized"));
		}
		let arg = WriteProcessStdioWithControlArg {
			control_sender: None,
			data,
			id,
			input,
			open,
			stopper,
			streams,
		};
		let stream = self.write_process_stdio_with_control(session, arg);
		Ok(Some(stream))
	}

	fn write_process_stdio_with_control(
		&self,
		session: &Session,
		arg: WriteProcessStdioWithControlArg<'_>,
	) -> BoxStream<'static, tg::Result<tg::process::stdio::write::Output>> {
		let WriteProcessStdioWithControlArg {
			control_sender,
			data,
			id,
			input,
			open,
			stopper,
			streams,
		} = arg;
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			let id = id.clone();
			let streams = streams.to_owned();
			move |_| async move {
				let arg = WriteProcessStdioLocalTaskArg {
					control_sender: control_sender.as_ref(),
					data: &data,
					id: &id,
					input,
					open,
					sender: &sender,
					streams: &streams,
				};
				let mut future = component
					.write_process_stdio_local_task(&session, arg)
					.boxed();
				let result = match stopper {
					Some(stopper) => {
						tokio::select! {
							result = &mut future => result,
							() = stopper.wait() => Ok(()),
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
		ReceiverStream::new(receiver).attach(task).boxed()
	}

	fn validate_process_stdio_write_streams(
		streams: &[tg::process::stdio::Stream],
	) -> tg::Result<()> {
		let stdin = streams.contains(&tg::process::stdio::Stream::Stdin);
		let output = streams
			.iter()
			.any(|stream| !matches!(stream, tg::process::stdio::Stream::Stdin));
		match (stdin, output) {
			(true, false) | (false, true) => Ok(()),
			(false, false) => Err(tg::error!("expected at least one stdio stream")),
			(true, true) => Err(tg::error!(
				"cannot write stdin and stdout or stderr in a single request"
			)),
		}
	}

	async fn write_process_stdio_local_task(
		&self,
		session: &Session,
		arg: WriteProcessStdioLocalTaskArg<'_>,
	) -> tg::Result<()> {
		let WriteProcessStdioLocalTaskArg {
			control_sender,
			data,
			id,
			mut input,
			open,
			sender,
			streams,
		} = arg;
		let destination = get_stdin_destination(data)?;
		let wait = if data.status.is_finished() {
			future::ok(()).boxed()
		} else {
			self.create_wait_process_finished_future_local(session, id)
				.await?
		}
		.shared();

		// Relay stdin to a runner on another server, unless the process has already finished.
		if destination == Destination::Pipe
			&& control_sender.is_none()
			&& wait.clone().now_or_never().is_none()
		{
			let arg = relay::WriteProcessStdioRelayTaskArg {
				id,
				input,
				open,
				sender,
				streams,
				wait,
			};
			return self
				.write_process_stdio_relay_task(session, arg)
				.boxed()
				.await;
		}

		// Write each chunk, then report the position through which it was written.
		while let Some(data) = input.try_next().await? {
			validate_write(&data, streams)?;
			tangram_server_core::checkpoint!(session.server, "process.stdio.write.request", close = %matches!(&data, Data::End(_)), stream = %tg::process::stdio::Stream::Stdin).await;
			let output = self
				.write_process_stdio_data(session, data, destination, wait.clone(), control_sender)
				.await?;
			sender
				.send(Ok(output))
				.await
				.map_err(|_| tg::error!("the stdio write output closed"))?;
			if output.closed {
				return Ok(());
			}
		}

		Ok(())
	}

	async fn write_process_stdio_data(
		&self,
		session: &Session,
		data: Data,
		destination: Destination,
		wait: future::Shared<BoxFuture<'static, tg::Result<()>>>,
		control_sender: Option<&tangram_server_core::process::control::local::Local>,
	) -> tg::Result<Output> {
		let (start, end) = match &data {
			Data::Chunk(chunk) => (
				chunk.stream_position,
				chunk.stream_position + chunk.bytes.len() as u64,
			),
			Data::End(end) => {
				let position = end
					.stream_positions
					.get(&tg::process::stdio::Stream::Stdin)
					.copied()
					.unwrap_or_default();
				(position, position)
			},
		};
		let closed = Output {
			closed: true,
			position: start,
		};
		if destination == Destination::Null {
			let output = Output {
				closed: matches!(data, Data::End(_)),
				position: end,
			};
			return Ok(output);
		}
		if let Some(result) = wait.clone().now_or_never() {
			result?;
			return Ok(closed);
		}
		let Some(control_sender) = control_sender else {
			return Ok(closed);
		};
		let response = match control_sender.write(data).await {
			Ok(response) => response,
			Err(error) => future::err(error).boxed(),
		};
		let timeout = session.server.config.process.stdio_retire_timeout;
		let output = Self::finish_write_process_stdio(response, wait, start, timeout).await?;

		Ok(output)
	}

	async fn finish_write_process_stdio(
		response: BoxFuture<'static, tg::Result<Output>>,
		wait: future::Shared<BoxFuture<'static, tg::Result<()>>>,
		start: u64,
		timeout: Duration,
	) -> tg::Result<Output> {
		let closed = Output {
			closed: true,
			position: start,
		};
		let response = tokio::select! {
			biased;
			response = response => response,
			result = wait.clone() => {
				result?;
				return Ok(closed);
			},
		};
		match response {
			Ok(output) => Ok(output),
			Err(error) => {
				// The local handler can retire between enqueueing a write and delivering its response.
				if let Ok(result) = tokio::time::timeout(timeout, wait).await {
					result?;
					return Ok(closed);
				}
				Err(error)
			},
		}
	}

	async fn try_write_process_stdio_region(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: &tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
		region: String,
	) -> tg::Result<
		ControlFlow<
			Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Output>>>,
			tg::Error,
		>,
	> {
		let client = session
			.get_region_session_for_process(&region)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the region client"),
			)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::process::stdio::write::stream::Arg {
			id: arg.id.clone(),
			location: Some(location.clone().into()),
			streams: arg.streams.clone(),
			tokens: arg.tokens.for_location(&location),
		};
		let output = client
			.try_write_process_stdio(process, arg, input)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to write stdio"))?
			.map_break(|output| output.map(futures::StreamExt::boxed))
			.map_continue(|error| tg::error!(!error, region = %region, "failed to write stdio"));

		Ok(output)
	}

	async fn try_write_process_stdio_remote(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: &tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<
		ControlFlow<
			Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Output>>>,
			tg::Error,
		>,
	> {
		let client = session
			.get_remote_session_for_process(&remote)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
			)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.clone(),
			region: region.clone(),
		});
		let arg = tg::process::stdio::write::stream::Arg {
			id: arg.id.clone(),
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			streams: arg.streams.clone(),
			tokens: arg.tokens.for_location(&location),
		};
		let output = client
			.try_write_process_stdio(process, arg, input)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to write stdio"))?
			.map_break(|output| output.map(futures::StreamExt::boxed))
			.map_continue(|error| tg::error!(!error, remote = %remote, "failed to write stdio"));

		Ok(output)
	}

	pub(crate) async fn try_write_process_stdio_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		process: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()
			.map_err(|error| {
				tg::error!(argument, !error, "failed to parse the content type header")
			})?;
		let tangram_content_type = tg::process::stdio::TANGRAM_CONTENT_TYPE;
		let output_encoding = tangram_server_core::http::frame::Encoding::from_accept(
			accept.as_ref(),
			tangram_content_type,
		)
		.map_err(|error| tg::error!(argument, !error, "invalid accept type"))?;
		let input_encoding = tangram_server_core::http::frame::Encoding::from_content_type(
			content_type
				.as_ref()
				.ok_or_else(|| tg::error!(argument, "missing the content type"))?,
			tangram_content_type,
		)
		.map_err(|error| tg::error!(argument, !error, "invalid content type"))?;
		let process = process
			.parse::<tg::Either<tg::process::Nonce, tg::process::Id>>()
			.map_err(|_| tg::error!(argument, "failed to parse the process"))?;
		let (arg, request) = request
			.arg_with_tangram::<tg::process::stdio::write::stream::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let max_frame_size = session.server.config.sync.max_frame_size;
		let input =
			tangram_server_core::http::frame::decode(request, input_encoding, max_frame_size);
		let output = match self
			.try_write_process_stdio(session, &process, arg, input)
			.await?
		{
			ControlFlow::Break(Some(output)) => output,
			ControlFlow::Break(None) => {
				return Ok(http::Response::builder()
					.not_found()
					.empty()
					.unwrap()
					.boxed_body());
			},
			ControlFlow::Continue(error) => {
				return Err(tg::error!(unavailable, !error, "failed to write stdio"));
			},
		};
		tangram_server_core::checkpoint!(session.server, "process.stdio.write.response", process = %process).await;
		let content_type = output_encoding.content_type(tangram_content_type);
		let body =
			tangram_server_core::http::frame::encode(output, output_encoding, max_frame_size);
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}
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

fn validate_write(data: &Data, streams: &[tg::process::stdio::Stream]) -> tg::Result<()> {
	match data {
		Data::Chunk(chunk) => {
			if chunk.bytes.is_empty()
				|| chunk.bytes.len() > tg::process::stdio::MAX_CHUNK_SIZE
				|| !streams.contains(&chunk.stream)
			{
				return Err(tg::error!("invalid process stdio chunk"));
			}
		},
		Data::End(end) => {
			if end.stream_positions.len() != streams.len()
				|| !streams
					.iter()
					.all(|stream| end.stream_positions.contains_key(stream))
				|| end
					.stream_positions
					.get(&tg::process::stdio::Stream::Stdin)
					.copied() != Some(end.combined_position)
			{
				return Err(tg::error!("invalid process stdio end positions"));
			}
		},
	}
	Ok(())
}
