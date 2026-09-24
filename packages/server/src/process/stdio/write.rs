use {
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _,
		future::{self, BoxFuture},
		stream::{BoxStream, FuturesOrdered},
	},
	std::{collections::BTreeMap, time::Duration},
	tangram_client::{
		prelude::*,
		process::stdio::{
			flow,
			write::{Ack, ClientMessage, Data, Output, Response, ServerMessage},
		},
	},
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
		id: &tg::process::Id,
		arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::write::ServerMessage>>>>
	{
		Self::validate_process_stdio_write_streams(&arg.streams)?;
		if let Some(control) = self
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
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_write_process_stdio_local(
					id,
					&arg.streams,
					input,
					self.context.stopper.clone(),
					arg.tokens.local_authorization(),
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

	pub(in crate::process) async fn try_write_process_stdio_local(
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
		Self::validate_process_stdio_write_streams(streams)?;
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
		);
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let authorized = self.authorize(resource, permission).await?;
		if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
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
		input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
		stopper: Option<Stopper>,
		control_sender: Option<crate::process::control::local::Local>,
	) -> BoxStream<'static, tg::Result<tg::process::stdio::write::ServerMessage>> {
		let (sender, receiver) =
			tokio::sync::mpsc::channel(tg::process::stdio::flow::CHANNEL_CAPACITY);
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
			(true, false) => Ok(()),
			(false, false) => Err(tg::error!("expected at least one stdio stream")),
			(_, true) => Err(tg::error!("cannot write process stdout or stderr")),
		}
	}

	async fn write_process_stdio_local_task(
		&self,
		id: &tg::process::Id,
		data: &tg::process::Data,
		streams: &[tg::process::stdio::Stream],
		mut input: BoxStream<'static, tg::Result<tg::process::stdio::write::ClientMessage>>,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::process::stdio::write::ServerMessage>>,
		control_sender: Option<&crate::process::control::local::Local>,
	) -> tg::Result<()> {
		let destination = get_stdin_destination(data)?;
		let wait = if data.status.is_finished() {
			future::ok(()).boxed()
		} else {
			self.create_wait_process_finished_future_local(id).await?
		}
		.shared();
		let mut pending = FuturesOrdered::<BoxFuture<'static, (u64, tg::Result<Output>)>>::new();
		let mut responses = BTreeMap::new();
		let mut end = None;
		let mut ended = false;
		loop {
			if pending.is_empty()
				&& let Some(request) = end.take()
			{
				let request: tg::process::stdio::write::Request = request;
				let response = self
					.start_write_process_stdio_local(
						id,
						request.arg,
						destination,
						wait.clone(),
						control_sender,
					)
					.await?;
				pending.push_back(async move { (request.id, response.await) }.boxed());
				ended = true;
			}
			if ended && pending.is_empty() && responses.is_empty() {
				return Ok(());
			}
			tokio::select! {
				message = input.try_next() => {
					let Some(message) = message? else { return Err(tg::error!("the stdio write input closed before completion")); };
					match message {
						ClientMessage::Ack(Ack { id }) => {
							if responses.get(&id) == Some(&true) {
								responses.remove(&id);
							}
						},
						ClientMessage::Request(request) => {
							if ended || end.is_some() { return Err(tg::error!("received a write after stdio EOF")); }
							if responses.len() >= flow::MAX_CHUNKS || responses.insert(request.id, false).is_some() {
								return Err(tg::error!("the stdio write window was exceeded"));
							}
							validate_write(&request.arg, streams)?;
							sender.send(Ok(ServerMessage::Ack(Ack { id: request.id }))).await.map_err(|_| tg::error!("the stdio write output closed"))?;
							if matches!(request.arg, Data::End(_)) { end = Some(request); continue; }
							// Publish writes in order while their completed outcomes remain pending.
							let response = self.start_write_process_stdio_local(id, request.arg, destination, wait.clone(), control_sender).await?;
							pending.push_back(async move { (request.id, response.await) }.boxed());
						},
					}
				},
				response = pending.next(), if !pending.is_empty() => {
					let (id, result) = response.unwrap();
					responses.insert(id, true);
					let response = create_response(id, result);
					sender.send(Ok(ServerMessage::Response(response))).await.map_err(|_| tg::error!("the stdio write output closed"))?;
				},
			}
		}
	}

	async fn start_write_process_stdio_local(
		&self,
		id: &tg::process::Id,
		data: tg::process::stdio::write::Data,
		destination: Destination,
		wait: futures::future::Shared<BoxFuture<'static, tg::Result<()>>>,
		control_sender: Option<&crate::process::control::local::Local>,
	) -> tg::Result<BoxFuture<'static, tg::Result<tg::process::stdio::write::Output>>> {
		crate::checkpoint!(self.server, "process.stdio.write.request", close = %matches!(&data, Data::End(_)), stream = %tg::process::stdio::Stream::Stdin).await;
		let length = match &data {
			Data::Chunk(chunk) => chunk.bytes.len() as u64,
			Data::End(_) => 0,
		};
		if destination == Destination::Null {
			let output = Output {
				closed: matches!(data, Data::End(_)),
				length,
			};
			return Ok(future::ok(output).boxed());
		}
		if let Some(result) = wait.clone().now_or_never() {
			result?;
			return Ok(future::ok(Output {
				closed: true,
				length: 0,
			})
			.boxed());
		}
		let request = tg::process::control::ServerRequestArg::Write(data);
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options {
				max_retries: u64::MAX,
				..Default::default()
			},
			timeout: Duration::from_secs(10),
		};
		let response = if let Some(control_sender) = control_sender {
			match control_sender.start(request).await {
				Ok(response) => response,
				Err(error) => future::err(error).boxed(),
			}
		} else {
			self.start_process_control_request(id, request, options)
				.await?
		};
		let local = control_sender.is_some();
		let future = Self::finish_write_process_stdio(response, wait, local).boxed();
		Ok(future)
	}

	async fn finish_write_process_stdio(
		response: BoxFuture<
			'static,
			tg::Result<tg::Result<tg::process::control::ClientResponseOutput>>,
		>,
		wait: futures::future::Shared<BoxFuture<'static, tg::Result<()>>>,
		local: bool,
	) -> tg::Result<Output> {
		let response = tokio::select! {
			biased;
			response = response => response,
			result = wait.clone() => { result?; return Ok(Output { closed: true, length: 0 }); },
		};
		let response = match response {
			Ok(response) => response?,
			Err(error) => {
				// The local handler can retire between enqueueing a write and delivering its response.
				let finished = if local {
					tokio::time::timeout(Duration::from_secs(10), wait)
						.await
						.ok()
				} else {
					wait.now_or_never()
				};
				if let Some(result) = finished {
					result?;
					return Ok(Output {
						closed: true,
						length: 0,
					});
				}
				return Err(error);
			},
		};
		let output = response
			.try_unwrap_write()
			.map_err(|_| tg::error!("expected a write response"))?;

		Ok(output)
	}

	async fn try_write_process_stdio_region(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::stdio::write::stream::Arg,
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
		let arg = tg::process::stdio::write::stream::Arg {
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
		arg: &tg::process::stdio::write::stream::Arg,
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
		let arg = tg::process::stdio::write::stream::Arg {
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
		let (arg, request) = request
			.arg::<tg::process::stdio::write::stream::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
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
				|| chunk.bytes.len() > flow::CHUNK_SIZE
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

fn create_response(id: u64, result: tg::Result<Output>) -> Response {
	let (error, output) = match result {
		Err(error) => {
			let error = tg::error::Data {
				message: Some(error.to_string()),
				source: Some(tg::Referent::new(
					error.to_data_or_id().map_left(Box::new),
					tg::referent::Options::default(),
				)),
				..Default::default()
			};
			(Some(error), None)
		},
		Ok(output) => (None, Some(output)),
	};
	Response { error, id, output }
}
