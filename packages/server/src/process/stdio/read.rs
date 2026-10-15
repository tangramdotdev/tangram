use {
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream, FuturesUnordered},
	},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, io::SeekFrom, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::{IntervalStream, ReceiverStream},
	tokio_util::task::AbortOnDropHandle,
};

mod relay;

type Acks = tokio::sync::watch::Receiver<u64>;

enum Source {
	Log(BTreeSet<tg::process::stdio::Stream>),
	Null,
	Pipe(BTreeSet<tg::process::stdio::Stream>),
}

impl Session {
	pub async fn try_read_process_stdio(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one stdio stream"));
		}
		if arg.id.is_some() {
			return self.try_read_process_stdio_relay(id, arg, input).await;
		}

		// Track the position through which the reader acknowledges the read's bytes, so any route to the runner can forward it.
		let start = match arg.position {
			Some(SeekFrom::Start(position)) => position,
			_ => 0,
		};
		let (ack_sender, acks) = tokio::sync::watch::channel(start);
		let ack_task = Task::spawn(move |_| async move {
			let mut input = input;
			while let Some(Ok(tg::process::stdio::read::ClientMessage::Ack { position })) =
				input.next().await
			{
				ack_sender.send_if_modified(|acked| {
					let modified = position > *acked;
					*acked = (*acked).max(position);
					modified
				});
			}
		});

		let Some(source) = self
			.try_read_process_stdio_source(id, arg.clone(), &acks)
			.await?
		else {
			return Ok(None);
		};
		let stream = self
			.read_process_stdio_protocol(source)
			.attach(ack_task)
			.boxed();

		Ok(Some(stream))
	}

	async fn try_read_process_stdio_source(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		acks: &Acks,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
		if let Some(control) = self
			.try_get_process_control_runner(
				id,
				arg.location.as_ref(),
				&arg.tokens,
				tg::authorization::permission::process::Set::NODE,
			)
			.await?
		{
			let source = Self::get_process_stdio_source(&control.data, &arg)?;
			if let Source::Pipe(streams) = &source
				&& self
					.authorize_process_stdio_read(id, &source, arg.tokens.local_authorization())
					.await?
			{
				let mut arg = arg;
				arg.streams = streams.iter().copied().collect();
				let server = self.server.clone();
				let id = id.clone();
				let input = read_process_stdio_acks(acks);
				let stream = stream::once(async move {
					for stream in &arg.streams {
						crate::checkpoint!(server, "process.stdio.read.request", process = %id, stream = %stream).await;
					}
					Ok::<_, tg::Error>(control.control_sender.read(arg, input))
				})
				.try_flatten()
				.boxed();
				return Ok(Some(stream));
			}
		}
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;
		if let Some(local) = &locations.local {
			if local.current
				&& let Some(stream) = self
					.try_read_process_stdio_local(id, arg.clone(), acks)
					.await
					.map_err(|error| tg::error!(!error, "failed to read local process stdio"))?
			{
				return Ok(Some(stream));
			}
			if let Some(stream) = self
				.try_read_process_stdio_regions(id, arg.clone(), &local.regions, acks)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to read process stdio from another region")
				})? {
				return Ok(Some(stream));
			}
		}
		if let Some(stream) = self
			.try_read_process_stdio_remotes(id, arg, &locations.remotes, acks)
			.await
			.map_err(|error| tg::error!(!error, "failed to read process stdio from a remote"))?
		{
			return Ok(Some(stream));
		}

		Ok(None)
	}

	fn read_process_stdio_protocol(
		&self,
		output: BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>,
	) -> BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>> {
		match self.context.stopper.clone() {
			Some(stopper) => output
				.take_until(async move { stopper.wait().await })
				.boxed(),
			None => output,
		}
	}

	async fn try_read_process_stdio_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		acks: &Acks,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
		let output = self
			.try_get_process_local(
				id,
				false,
				false,
				arg.tokens.local_authorization(),
				tg::process::Source::Auto,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process"))?;
		let Some(output) = output else {
			return Ok(None);
		};
		let source = Self::get_process_stdio_source(&output.data, &arg)?;
		if matches!(source, Source::Pipe(_))
			&& output
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
		{
			return Ok(None);
		}
		if !self
			.authorize_process_stdio_read(id, &source, arg.tokens.local_authorization())
			.await?
		{
			return Err(tg::error!("unauthorized"));
		}
		let mut arg = arg;
		if arg.size == Some(0) {
			return Err(tg::error!("expected a nonzero stdio chunk size"));
		}
		let max = tg::process::stdio::MAX_CHUNK_SIZE as u64;
		arg.size = Some(arg.size.unwrap_or(max).min(max));
		let stream = match source {
			Source::Log(streams) => {
				self.try_read_process_stdio_log_local(id, arg, streams)
					.await?
			},
			Source::Null => stream::once(futures::future::ok(
				tg::process::stdio::read::ServerMessage::Response(
					tg::process::stdio::read::Output::End(tg::process::stdio::End {
						combined_position: 0,
						stream_positions: arg.streams.iter().map(|stream| (*stream, 0)).collect(),
					}),
				),
			))
			.boxed(),
			Source::Pipe(streams) => {
				self.try_read_process_stdio_pipe_local(id, &arg, streams, acks)
			},
		};

		Ok(Some(stream))
	}

	async fn authorize_process_stdio_read(
		&self,
		id: &tg::process::Id,
		source: &Source,
		tokens: &[tg::authorization::Token],
	) -> tg::Result<bool> {
		let Source::Pipe(streams) = source else {
			return Ok(true);
		};
		let stdin = streams.contains(&tg::process::stdio::Stream::Stdin);
		let output = streams
			.iter()
			.any(|stream| !matches!(stream, tg::process::stdio::Stream::Stdin));
		match (stdin, output) {
			(true, false) => Ok(matches!(
					&self.context.principal,
					tg::Principal::Process(process) if process == id
			)),
			(false, _) => {
				let permission = tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Parent,
				);
				let resource =
					tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
				let authorized = self
					.authorize(resource, permission)
					.await?
					.check_exhaustion()?;
				Ok(authorized.permissions.contains(permission))
			},
			(true, true) => Err(tg::error!(
				"cannot read stdin and stdout or stderr in a single request"
			)),
		}
	}

	async fn try_read_process_stdio_log_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		streams: BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>> {
		let (sender, receiver) = async_channel::bounded(1);
		let session = self.clone();
		let id = id.clone();
		let task = Task::spawn(move |_| async move {
			let result = session
				.try_read_process_stdio_log_local_task(&id, arg, streams, sender.clone())
				.boxed()
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});

		Ok(receiver.attach(task).boxed())
	}

	async fn try_read_process_stdio_log_local_task(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::stdio::read::Arg,
		streams: BTreeSet<tg::process::stdio::Stream>,
		sender: async_channel::Sender<tg::Result<tg::process::stdio::read::ServerMessage>>,
	) -> tg::Result<()> {
		let mut wakeups = if arg.timeout == Some(Duration::ZERO) {
			None
		} else {
			let mut wakeups: Vec<BoxStream<'static, ()>> = Vec::new();
			let subject = format!("processes.{id}.log");
			let log_wakeups = self
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ())
				.boxed();
			wakeups.push(log_wakeups);
			let subject = format!("processes.{id}.status");
			let status_wakeups = self
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ())
				.boxed();
			wakeups.push(status_wakeups);
			for &stream in &streams {
				let subject = format!("processes.{id}.{stream}.close");
				let close_wakeups = self
					.server
					.messenger
					.subscribe::<()>(subject)
					.await
					.map_err(|error| tg::error!(!error, "failed to subscribe"))?
					.map(|_| ())
					.boxed();
				wakeups.push(close_wakeups);
			}
			let interval = IntervalStream::new(tokio::time::interval(
				self.server.config.process.stdio_wakeup_interval,
			))
			.skip(1)
			.map(|_| ())
			.boxed();
			wakeups.push(interval);
			let wakeups = stream::select_all(wakeups);
			let wakeups = match arg.timeout {
				Some(timeout) => wakeups.take_until(tokio::time::sleep(timeout)).boxed(),
				None => wakeups.boxed(),
			};

			Some(wakeups)
		};
		let mut positioned = false;
		let mut outcome = None;
		'outer: loop {
			let indexed = self
				.get_process_from_index(id)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the process"))?;
			let data = indexed
				.data
				.ok_or_else(|| tg::error!(%id, "missing the process data"))?;
			// Read the completion marker before draining so every committed chunk is visible.
			let end =
				if data.log.is_some() {
					None
				} else if data.status.is_finished() && data.started_at.is_none() {
					Some(tg::process::stdio::End {
						combined_position: 0,
						stream_positions: [
							(tg::process::stdio::Stream::Stderr, 0),
							(tg::process::stdio::Stream::Stdout, 0),
						]
						.into(),
					})
				} else {
					self.server.cache.try_get_log_end(id).await?.map(|end| {
						tg::process::stdio::End {
							combined_position: end.position,
							stream_positions: [
								(tg::process::stdio::Stream::Stderr, end.stderr_position),
								(tg::process::stdio::Stream::Stdout, end.stdout_position),
							]
							.into(),
						}
					})
				};
			let previous = (arg.position, arg.length);
			let (end, mut stream) = self
				.process_log_stream(id, &mut arg, end, streams.clone())
				.await
				.map_err(|error| tg::error!(!error, "failed to create the log stream"))?;
			// Report the resolved window before its chunks so reconnecting readers do not infer it from a changing EOF.
			if !positioned || previous != (arg.position, arg.length) {
				let Some(SeekFrom::Start(position)) = arg.position else {
					unreachable!()
				};
				let notification = tg::process::stdio::read::Event::Position {
					length: arg.length,
					position,
				};
				if sender
					.send(Ok(tg::process::stdio::read::ServerMessage::Notification(
						notification,
					)))
					.await
					.is_err()
				{
					break;
				}
				positioned = true;
			}
			while let Some(chunk) = stream.try_next().await? {
				let position = if streams.len() > 1 {
					chunk.combined_position
				} else {
					chunk.stream_position
				};
				let forward = arg.length.is_none_or(|length| length >= 0);
				arg.position.replace(SeekFrom::Start(if forward {
					position + chunk.bytes.len().to_u64().unwrap()
				} else {
					position
				}));
				if let Some(length) = &mut arg.length {
					if *length >= 0 {
						*length -= chunk.bytes.len().to_i64().unwrap().min(*length);
					} else {
						*length = length
							.saturating_add_unsigned(chunk.bytes.len().to_u64().unwrap())
							.min(0);
					}
				}
				let notification = tg::process::stdio::read::Event::Chunk(chunk);
				if sender
					.send(Ok(tg::process::stdio::read::ServerMessage::Notification(
						notification,
					)))
					.await
					.is_err()
				{
					break 'outer;
				}
				if arg.length.is_some_and(|length| length == 0) {
					break;
				}
			}
			let reached_start = arg.length.is_some_and(|length| length < 0)
				&& matches!(arg.position, Some(SeekFrom::Start(0)));
			if arg.length == Some(0) || reached_start {
				outcome = Some(tg::process::stdio::read::Output::Limit {
					position: match arg.position {
						Some(SeekFrom::Start(position)) => position,
						_ => unreachable!(),
					},
				});
				break;
			}
			if let Some(end) = end {
				// A reverse read must reach its limit or the start before completing.
				if arg.length.is_some_and(|length| length < 0) {
					return Err(tg::error!("encountered a gap in the process log"));
				}
				outcome = Some(tg::process::stdio::read::Output::End(end));
				break;
			}
			let Some(wakeups) = &mut wakeups else {
				break;
			};
			let Some(()) = wakeups.next().await else {
				break;
			};
		}

		let outcome = outcome.unwrap_or(tg::process::stdio::read::Output::Timeout {
			position: match arg.position {
				Some(SeekFrom::Start(position)) => position,
				_ => 0,
			},
		});
		let message = tg::process::stdio::read::ServerMessage::Response(outcome);
		sender.send(Ok(message)).await.ok();

		Ok(())
	}

	fn try_read_process_stdio_pipe_local(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::stdio::read::Arg,
		streams: BTreeSet<tg::process::stdio::Stream>,
		acks: &Acks,
	) -> BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>> {
		let arg = tg::process::stdio::read::Arg {
			streams: streams.into_iter().collect(),
			..arg.clone()
		};
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let task = Task::spawn({
			let session = self.clone();
			let id = id.clone();
			let acks = acks.clone();
			move |_| async move {
				let result = session
					.read_process_stdio_relay(&id, arg, &sender, acks)
					.boxed()
					.await;
				if let Err(error) = result {
					sender.send(Err(error)).await.ok();
				}
			}
		});
		ReceiverStream::new(receiver).attach(task).boxed()
	}

	async fn read_process_stdio_relay(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::process::stdio::read::ServerMessage>>,
		acks: Acks,
	) -> tg::Result<()> {
		for stream in &arg.streams {
			crate::checkpoint!(self.server, "process.stdio.read.request", process = %id, stream = %stream).await;
		}

		// Track the read's delivered position, so a failed relay can be replaced by one that resumes without delivering a byte twice.
		let combined = arg.streams.len() > 1;
		let mut position = match arg.position {
			None => 0,
			Some(SeekFrom::Start(position)) => position,
			Some(_) => return Err(tg::error!("piped stdio only supports an absolute position")),
		};
		let mut length = arg.length;
		let options: tangram_futures::retry::Options = self
			.server
			.config()
			.process
			.stdio_relay_retry
			.clone()
			.into();
		let mut retries = tangram_futures::retry::stream(options.clone()).boxed();
		let mut error = None;
		loop {
			if retries.next().await.is_none() {
				return Err(error.unwrap());
			}

			// Open the read at the reader's acknowledged position, because the bytes delivered after it may not have reached the reader.
			let acked = (*acks.borrow()).min(position);
			let mut arg = arg.clone();
			arg.position = Some(SeekFrom::Start(acked));
			arg.length =
				length.map(|length| length.saturating_add((position - acked).to_i64().unwrap()));
			let start = position;
			let result = self
				.read_process_stdio_relay_attempt(
					id,
					arg,
					sender,
					combined,
					&mut position,
					&mut length,
					acks.clone(),
				)
				.await;
			let attempt_error = match result {
				Ok(()) => return Ok(()),
				Err(error) => error,
			};

			// Return the error unless the relay failed, so that only a failed relay is replaced and a superseded read never takes over again.
			if attempt_error.kind() != Some(tg::error::Kind::Unavailable) || sender.is_closed() {
				return Err(attempt_error);
			}

			// Restart the retries if the attempt delivered bytes, so that only consecutive failures exhaust them.
			if position > start {
				retries = tangram_futures::retry::stream(options.clone()).boxed();
			}
			tracing::warn!(error = %attempt_error.trace(), %id, "reopening the stdio relay");
			crate::checkpoint!(self.server, "process.stdio.read.reopen", process = %id).await;
			error = Some(attempt_error);
		}
	}

	#[expect(clippy::too_many_arguments)]
	async fn read_process_stdio_relay_attempt(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		sender: &tokio::sync::mpsc::Sender<tg::Result<tg::process::stdio::read::ServerMessage>>,
		combined: bool,
		position: &mut u64,
		length: &mut Option<i64>,
		acks: Acks,
	) -> tg::Result<()> {
		// Open the relay before the read, so the runner's first bytes are not missed.
		let timeout = arg.timeout;
		let request_id = crate::control::id();
		let send_subject = super::relay::ack_subject(id, &request_id);
		let receive_subject = super::relay::data_subject(id, &request_id);
		let options = self.server.config().process.stdio_relay;
		let (relay_sender, receiver) =
			tangram_messenger::relay::open::<super::relay::Ack, super::relay::Data, _>(
				&self.server.messenger,
				send_subject,
				receive_subject,
				options,
			)
			.await
			.map_err(super::relay::error)?;
		let mut items = receiver;

		// Send the reader's acknowledgments back to the runner.
		let (end_sender, end) = tokio::sync::watch::channel(None);
		let ack_items = relay_process_stdio_acks(acks.clone(), end);
		let mut ack_task = AbortOnDropHandle::new(tokio::spawn(relay_sender.send_all(ack_items)));

		// Open the read on the runner.
		let open = tg::process::control::OpenServerRequestArg { arg };
		let request = tg::process::control::ServerRequest {
			arg: tg::process::control::ServerRequestArg::Open(open),
			id: request_id.clone(),
		};
		let mut response = self.send_process_control_open(id, request).await?;

		// Forward the relayed chunks until the relay ends.
		let mut opened = false;
		loop {
			let item = tokio::select! {
				item = items.next() => item,
				result = &mut ack_task => {
					result
						.map_err(|error| tg::error!(unavailable, !error, "the stdio acknowledgment task panicked"))?
						.map_err(super::relay::error)?;
					return Err(tg::error!(unavailable, "the stdio acknowledgments ended"));
				},
				result = &mut response, if !opened => {
					result?;
					opened = true;
					continue;
				},
				() = sender.closed() => return Ok(()),
			};
			let message = match item {
				Some(Ok(super::relay::Data(tg::process::stdio::write::Data::Chunk(mut chunk)))) => {
					// Skip the bytes that were already delivered.
					let start = if combined {
						chunk.combined_position
					} else {
						chunk.stream_position
					};
					let size = chunk.bytes.len().to_u64().unwrap();
					if start + size <= *position {
						continue;
					}
					if start > *position {
						return Err(
							tg::error!(expected = %*position, actual = %start, "encountered a gap in the process stdio stream"),
						);
					}
					let offset = *position - start;
					chunk.bytes = chunk.bytes.slice(offset.to_usize().unwrap()..);
					chunk.combined_position += offset;
					chunk.stream_position += offset;
					let size = size - offset;
					*position += size;
					if let Some(length) = length {
						*length -= size.to_i64().unwrap();
					}
					tg::process::stdio::read::ServerMessage::Notification(
						tg::process::stdio::read::Event::Chunk(chunk),
					)
				},
				Some(Ok(super::relay::Data(tg::process::stdio::write::Data::End(end)))) => {
					let end_position = if combined {
						end.combined_position
					} else {
						end.stream_positions
							.values()
							.copied()
							.next()
							.unwrap_or_default()
					};
					let output = tg::process::stdio::read::Output::End(end);
					sender
						.send(Ok(tg::process::stdio::read::ServerMessage::Response(
							output,
						)))
						.await
						.ok();

					// Read the relay to its end while the acknowledgments finish, so that the relay acknowledges its end to the runner.
					let drain = items.for_each(|_| future::ready(()));
					let finish = finish_process_stdio_acks(
						end_position,
						&end_sender,
						ack_task,
						options.peer_timeout(),
					);
					future::join(drain, finish).await;
					return Ok(());
				},
				Some(Err(error)) => {
					return Err(super::relay::error(error));
				},
				None => {
					// Respond with the read's limit or timeout when the relay ends without EOF, and otherwise fail the attempt, so that the relay is reopened.
					let output = if length.is_some_and(|length| length <= 0) {
						tg::process::stdio::read::Output::Limit {
							position: *position,
						}
					} else if timeout.is_some() {
						tg::process::stdio::read::Output::Timeout {
							position: *position,
						}
					} else {
						return Err(tg::error!(unavailable, "the stdio relay ended before EOF"));
					};
					sender
						.send(Ok(tg::process::stdio::read::ServerMessage::Response(
							output,
						)))
						.await
						.ok();
					finish_process_stdio_acks(
						*position,
						&end_sender,
						ack_task,
						options.peer_timeout(),
					)
					.await;
					return Ok(());
				},
			};
			if sender.send(Ok(message)).await.is_err() {
				return Ok(());
			}
		}
	}

	pub(super) async fn send_process_control_open(
		&self,
		id: &tg::process::Id,
		request: tg::process::control::ServerRequest,
	) -> tg::Result<futures::future::BoxFuture<'static, tg::Result<()>>> {
		let request_id = request.id.clone();
		let request = crate::process::control::ServerMessage(
			tg::process::control::ServerMessage::Request(request),
		);
		let options = crate::control::Options {
			retry: self.server.config.process.stdio_open_retry.clone().into(),
			timeout: self.server.config.process.stdio_open_timeout,
		};
		let arg = crate::control::SendControlRequestArg {
			ack: |id| {
				crate::process::control::ServerMessage(tg::process::control::ServerMessage::Ack(
					tg::process::control::ServerAck { id },
				))
			},
			client_subject: format!("processes.{id}.control.client.{request_id}"),
			is_ack: |message: &crate::process::control::ClientMessage| {
				matches!(message.0, tg::process::control::ClientMessage::Ack(_))
			},
			marker: std::marker::PhantomData,
			options,
			request,
			response: |message: crate::process::control::ClientMessage| match message.0 {
				tg::process::control::ClientMessage::Response(response) => {
					let result = if let Some(error) = response.error {
						Err(tg::Error::try_from(error)?)
					} else {
						Ok(())
					};
					Ok(Some((response.id, result)))
				},
				_ => Ok(None),
			},
			server_subject: format!("processes.{id}.control.server"),
		};
		let response = self.server.send_control_request(arg).await?;
		let response = async move { response.await? }.boxed();

		Ok(response)
	}

	async fn try_read_process_stdio_regions(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		regions: &[String],
		acks: &Acks,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
		let mut futures = regions
			.iter()
			.map(|region| self.try_read_process_stdio_region(id, arg.clone(), region, acks))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(stream)) => {
					result = Ok(Some(stream));
					break;
				},
				Ok(None) => (),
				Err(error) => result = Err(error),
			}
		}

		result
	}

	async fn try_read_process_stdio_region(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		region: &str,
		acks: &Acks,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
		let client = self.get_region_session_for_process(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let tokens = arg.tokens.for_location(&location);
		let arg = tg::process::stdio::read::Arg {
			location: Some(location.into()),
			tokens,
			..arg
		};
		let acks = read_process_stdio_acks(acks);
		let stream = client
			.try_read_process_stdio_all_inner(id, arg, Some(acks))
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to read the process stdio"),
			)?
			.map(futures::StreamExt::boxed);

		Ok(stream)
	}

	async fn try_read_process_stdio_remotes(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		remotes: &[crate::location::Remote],
		acks: &Acks,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_read_process_stdio_remote(id, arg.clone(), remote, acks))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(stream)) => {
					result = Ok(Some(stream));
					break;
				},
				Ok(None) => (),
				Err(error) => result = Err(error),
			}
		}

		result
	}

	async fn try_read_process_stdio_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
		remote: &crate::location::Remote,
		acks: &Acks,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>>
	{
		let client = self
			.get_remote_session_for_process(&remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let tokens = arg.tokens.for_location(&location);
		let arg = tg::process::stdio::read::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			tokens,
			..arg
		};
		let acks = read_process_stdio_acks(acks);
		let stream = client
			.try_read_process_stdio_all_inner(id, arg, Some(acks))
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to read the process stdio"),
			)?
			.map(futures::StreamExt::boxed);

		Ok(stream)
	}

	fn get_process_stdio_source(
		data: &tg::process::Data,
		arg: &tg::process::stdio::read::Arg,
	) -> tg::Result<Source> {
		let mut log_streams = BTreeSet::new();
		let mut pipe_streams = BTreeSet::new();
		for stream in &arg.streams {
			let stdio = match stream {
				tg::process::stdio::Stream::Stderr => &data.stderr,
				tg::process::stdio::Stream::Stdin => &data.stdin,
				tg::process::stdio::Stream::Stdout => &data.stdout,
			};
			match stdio {
				tg::process::Stdio::Log => {
					if matches!(stream, tg::process::stdio::Stream::Stdin) {
						return Err(tg::error!("invalid stdio stream"));
					}
					log_streams.insert(*stream);
				},
				tg::process::Stdio::Null => (),
				tg::process::Stdio::Pipe | tg::process::Stdio::Tty => {
					pipe_streams.insert(*stream);
				},
				tg::process::Stdio::Blob(_) | tg::process::Stdio::Inherit => {
					return Err(tg::error!("invalid stdio"));
				},
			}
		}
		if !log_streams.is_empty() && !pipe_streams.is_empty() {
			return Err(tg::error!(
				"cannot read logged and piped stdio in a single request"
			));
		}
		if !pipe_streams.is_empty() {
			if arg.length.is_some_and(|length| length < 0) {
				return Err(tg::error!("piped stdio only supports forward reads"));
			}
			if matches!(arg.position, Some(SeekFrom::Current(_) | SeekFrom::End(_))) {
				return Err(tg::error!("piped stdio only supports an absolute position"));
			}
			if pipe_streams.contains(&tg::process::stdio::Stream::Stdout)
				&& pipe_streams.contains(&tg::process::stdio::Stream::Stderr)
				&& matches!(data.stdout, tg::process::Stdio::Tty)
				&& matches!(data.stderr, tg::process::Stdio::Tty)
			{
				pipe_streams.remove(&tg::process::stdio::Stream::Stderr);
			}

			return Ok(Source::Pipe(pipe_streams));
		}
		if log_streams.is_empty() {
			return Ok(Source::Null);
		}

		Ok(Source::Log(log_streams))
	}

	pub(crate) async fn try_read_process_stdio_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
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
		let id = id
			.parse::<tg::process::Id>()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the process id"))?;
		let (arg, request) = request
			.arg_with_tangram::<tg::process::stdio::read::Arg>()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let max_frame_size = self.server.config.sync.max_frame_size;
		let input = super::decode(request, input_encoding, max_frame_size);
		let Some(output) = self.try_read_process_stdio(&id, arg, input).await? else {
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

fn read_process_stdio_acks(
	acks: &Acks,
) -> BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>> {
	tokio_stream::wrappers::WatchStream::new(acks.clone())
		.map(|position| Ok(tg::process::stdio::read::ClientMessage::Ack { position }))
		.boxed()
}

/// Relay the reader's acknowledgments, ending once one at or past the end position has been sent.
fn relay_process_stdio_acks(
	acks: Acks,
	end: tokio::sync::watch::Receiver<Option<u64>>,
) -> BoxStream<'static, Result<super::relay::Ack, bytes::Bytes>> {
	struct State {
		acks: Option<Acks>,
		end: tokio::sync::watch::Receiver<Option<u64>>,
		sent: Option<u64>,
	}
	let state = State {
		acks: Some(acks),
		end,
		sent: None,
	};
	futures::stream::unfold(state, |mut state| async move {
		loop {
			if let Some(acks) = &mut state.acks {
				let acked = *acks.borrow_and_update();
				if state.sent.is_none_or(|sent| acked > sent) {
					state.sent = Some(acked);
					let message = tg::process::stdio::read::ClientMessage::Ack { position: acked };
					return Some((Ok(super::relay::Ack(message)), state));
				}
			}
			let end = *state.end.borrow_and_update();
			if end.is_some_and(|end| state.sent.is_none_or(|sent| sent >= end)) {
				return None;
			}
			tokio::select! {
				result = async { state.acks.as_mut().unwrap().changed().await }, if state.acks.is_some() => {
					if result.is_err() {
						state.acks = None;
					}
				},
				result = state.end.changed() => {
					if result.is_err() {
						return None;
					}
				},
			}
		}
	})
	.boxed()
}

/// Give the reader time to acknowledge the read's bytes after its final response, then finish sending acknowledgments, so the runner can release the bytes and complete its write.
async fn finish_process_stdio_acks(
	position: u64,
	end: &tokio::sync::watch::Sender<Option<u64>>,
	mut ack_task: AbortOnDropHandle<Result<(), tangram_messenger::relay::Error>>,
	timeout: Duration,
) {
	// Finish sending acknowledgments once the reader acknowledges the end position.
	end.send_replace(Some(position));
	if tokio::time::timeout(timeout, &mut ack_task).await.is_ok() {
		return;
	}

	// Stop waiting for a reader that does not acknowledge, and let the relay deliver what was sent.
	end.send_replace(Some(0));
	tokio::time::timeout(timeout, &mut ack_task).await.ok();
}
