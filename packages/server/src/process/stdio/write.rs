use {
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture},
		stream::BoxStream,
	},
	std::time::Duration,
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
	tg::process::stdio::write::{Data, Output},
	tokio_stream::wrappers::ReceiverStream,
};

mod log;
mod relay;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Destination {
	Null,
	Pipe,
}

impl Session {
	pub async fn try_write_process_stdio(
		&self,
		arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Output>>>> {
		let id = &arg.process;
		Self::validate_process_stdio_write_streams(&arg.streams)?;
		let stdin = arg.streams.contains(&tg::process::stdio::Stream::Stdin);
		if stdin
			&& let Some(control) = self
				.try_get_process_control_runner(
					id,
					arg.location.as_ref(),
					&arg.tokens,
					tg::authorization::permission::process::Set::PARENT,
				)
				.await?
		{
			let stream = self.write_process_stdio_with_control(
				id,
				control.data,
				&arg.streams,
				input,
				self.context.stopper.clone(),
				Some(control.control_sender),
			);
			return Ok(Some(stream));
		}
		let location = self.server.location(arg.location.as_ref())?;
		let output = match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != self.server.config.region.as_deref() => {
				self.try_write_process_stdio_region(id, &arg, input, region)
					.await?
			},
			tg::Location::Local(_) if arg.id.is_some() => {
				let open = arg.id.as_deref().unwrap();
				self.write_process_stdio_relay_local(id, open, &arg.streams, input)?
			},
			tg::Location::Local(_) if stdin => {
				self.try_write_process_stdio_local(
					id,
					&arg.streams,
					input,
					self.context.stopper.clone(),
					arg.tokens.local_authorization(),
				)
				.await?
			},
			tg::Location::Local(_) => {
				self.try_write_process_stdio_log_local(id, &arg.streams, input)
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

	pub(in crate::process) async fn try_write_process_stdio_local(
		&self,
		id: &tg::process::Id,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
		stopper: Option<Stopper>,
		tokens: &[tg::authorization::Token],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Output>>>> {
		let Some(tg::process::get::Output { data, location, .. }) = self
			.try_get_process_local(id, false, false, tokens, tg::process::Source::Auto)
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
		let authorized = self
			.authorize(resource, permission)
			.await?
			.check_exhaustion()?;
		if !authorized.permissions.contains(permission) {
			return Err(tg::error!("unauthorized"));
		}
		let stream = self.write_process_stdio_with_control(id, data, streams, input, stopper, None);
		Ok(Some(stream))
	}

	fn write_process_stdio_with_control(
		&self,
		id: &tg::process::Id,
		data: tg::process::Data,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
		stopper: Option<Stopper>,
		control_sender: Option<crate::process::control::local::Local>,
	) -> BoxStream<'static, tg::Result<tg::process::stdio::write::Output>> {
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let task = Task::spawn({
			let session = self.clone();
			let id = id.clone();
			let streams = streams.to_owned();
			move |_| async move {
				let mut future = session
					.write_process_stdio_local_task(
						&id,
						&data,
						&streams,
						input,
						&sender,
						control_sender.as_ref(),
					)
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
		id: &tg::process::Id,
		data: &tg::process::Data,
		streams: &[tg::process::stdio::Stream],
		mut input: BoxStream<'static, tg::Result<Data>>,
		sender: &tokio::sync::mpsc::Sender<tg::Result<Output>>,
		control_sender: Option<&crate::process::control::local::Local>,
	) -> tg::Result<()> {
		let destination = get_stdin_destination(data)?;
		let wait = if data.status.is_finished() {
			future::ok(()).boxed()
		} else {
			self.create_wait_process_finished_future_local(id).await?
		}
		.shared();

		// Relay stdin to a runner on another server, unless the process has already finished.
		if destination == Destination::Pipe
			&& control_sender.is_none()
			&& wait.clone().now_or_never().is_none()
		{
			return self
				.write_process_stdio_relay_task(id, streams, input, sender, wait)
				.boxed()
				.await;
		}

		// Write each chunk, then report the position through which it was written.
		while let Some(data) = input.try_next().await? {
			validate_write(&data, streams)?;
			crate::checkpoint!(self.server, "process.stdio.write.request", close = %matches!(&data, Data::End(_)), stream = %tg::process::stdio::Stream::Stdin).await;
			let output = self
				.write_process_stdio_data(data, destination, wait.clone(), control_sender)
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
		data: Data,
		destination: Destination,
		wait: future::Shared<BoxFuture<'static, tg::Result<()>>>,
		control_sender: Option<&crate::process::control::local::Local>,
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
		let timeout = self.server.config.process.stdio_retire_timeout;
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
		id: &tg::process::Id,
		arg: &tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
		region: String,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Output>>>> {
		let client = self.get_region_session_for_process(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::process::stdio::write::stream::Arg {
			id: arg.id.clone(),
			location: Some(location.clone().into()),
			process: id.clone(),
			streams: arg.streams.clone(),
			tokens: arg.tokens.for_location(&location),
		};
		let stream = client
			.try_write_process_stdio(arg, input)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to write stdio"))?;

		Ok(stream.map(futures::StreamExt::boxed))
	}

	async fn try_write_process_stdio_remote(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::Data>>,
		remote: String,
		region: Option<String>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::Output>>>> {
		let client = self.get_remote_session_for_process(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
		)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.clone(),
			region: region.clone(),
		});
		let arg = tg::process::stdio::write::stream::Arg {
			id: arg.id.clone(),
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			process: id.clone(),
			streams: arg.streams.clone(),
			tokens: arg.tokens.for_location(&location),
		};
		let stream = client
			.try_write_process_stdio(arg, input)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to write stdio"))?;

		Ok(stream.map(futures::StreamExt::boxed))
	}

	pub(crate) async fn try_write_process_stdio_request(
		&self,
		request: http::Request<BoxBody>,
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
		let output_encoding =
			super::Encoding::from_accept(accept.as_ref(), tangram_content_type)
				.map_err(|error| tg::error!(argument, !error, "invalid accept type"))?;
		let input_encoding = super::Encoding::from_content_type(
			content_type
				.as_ref()
				.ok_or_else(|| tg::error!(argument, "missing the content type"))?,
			tangram_content_type,
		)
		.map_err(|error| tg::error!(argument, !error, "invalid content type"))?;
		let (arg, request) = request
			.arg_with_tangram::<tg::process::stdio::write::stream::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.ok_or_else(|| tg::error!(argument, "missing the arg"))?;
		let id = arg.process.clone();
		let max_frame_size = self.server.config.sync.max_frame_size;
		let input = super::decode(request, input_encoding, max_frame_size);
		let Some(output) = self.try_write_process_stdio(arg, input).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		crate::checkpoint!(self.server, "process.stdio.write.response", process = %id).await;
		let content_type = output_encoding.content_type(tangram_content_type);
		let body = super::encode(output, output_encoding, max_frame_size);
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
