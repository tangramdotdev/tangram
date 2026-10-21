use {
	crate::prelude::*,
	futures::{
		FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	std::{
		io::SeekFrom,
		ops::ControlFlow,
		pin::pin,
		sync::{Arc, Mutex},
	},
	tangram_futures::{stream::Ext as _, task::Task},
};

pub trait Ext: tg::Instance {
	fn read(
		&self,
		arg: tg::read::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::read::Chunk>> + Send + 'static>,
	> + Send {
		self.try_read(arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the blob")))
		})
	}

	fn try_read(
		&self,
		arg: tg::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::read::Chunk>> + Send + 'static>,
		>,
	> + Send {
		async move {
			let instance = self.clone();
			let Some(stream) = instance.try_read_stream(arg.clone()).await? else {
				return Ok(None);
			};
			let stream = stream.boxed();
			struct State {
				stream: Option<BoxStream<'static, tg::Result<tg::read::Event>>>,
				arg: tg::read::Arg,
				end: bool,
			}
			let state = State {
				stream: Some(stream),
				arg,
				end: false,
			};
			let state = Arc::new(Mutex::new(state));
			let stream = stream::try_unfold(state.clone(), move |state| {
				let instance = instance.clone();
				async move {
					if state.lock().unwrap().end {
						return Ok(None);
					}
					let stream = state.lock().unwrap().stream.take();
					let stream = if let Some(stream) = stream {
						stream
					} else {
						let arg = state.lock().unwrap().arg.clone();
						instance
							.try_read_stream(arg)
							.await?
							.ok_or_else(|| tg::error!("the stream was not found"))?
							.boxed()
					};
					Ok::<_, tg::Error>(Some((stream, state)))
				}
			})
			.try_flatten()
			.take_while(|event| future::ready(!matches!(event, Ok(tg::read::Event::End))))
			.map(|event| match event {
				Ok(tg::read::Event::Chunk(chunk)) => Ok(chunk),
				Err(e) => Err(e),
				_ => unreachable!(),
			})
			.inspect_ok(move |chunk| {
				let mut state = state.lock().unwrap();

				// Compute the end condition.
				state.end = chunk.bytes.is_empty() || matches!(state.arg.options.length, Some(0));

				// Update the length argument.
				if let Some(length) = &mut state.arg.options.length {
					*length -= chunk.bytes.len().to_u64().unwrap().min(*length);
				}

				// Update the position argument.
				let position = chunk.position + chunk.bytes.len().to_u64().unwrap();
				state.arg.options.position = Some(SeekFrom::Start(position));
			});
			Ok(Some(stream))
		}
	}

	fn get_sandbox_status(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::sandbox::Status>> + Send + 'static>,
	> + Send {
		self.try_get_sandbox_status(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to find the sandbox")))
		})
	}

	fn try_get_sandbox_status(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::sandbox::Status>> + Send + 'static>,
		>,
	> + Send {
		async move {
			let instance = self.clone();
			let id = id.clone();
			let Some(stream) = instance
				.try_get_sandbox_status_stream(&id, arg.clone())
				.await?
			else {
				return Ok(None);
			};
			let stream = stream.boxed();
			struct State {
				stream: Option<stream::BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>,
				end: bool,
			}
			let state = Arc::new(Mutex::new(State {
				stream: Some(stream),
				end: false,
			}));
			let stream = stream::try_unfold(state.clone(), move |state| {
				let instance = instance.clone();
				let id = id.clone();
				let arg = arg.clone();
				async move {
					if state.lock().unwrap().end {
						return Ok(None);
					}
					let stream = state.lock().unwrap().stream.take();
					let stream = if let Some(stream) = stream {
						stream
					} else {
						instance
							.try_get_sandbox_status_stream(&id, arg)
							.await?
							.ok_or_else(|| tg::error!("failed to find the sandbox"))?
							.boxed()
					};
					Ok::<_, tg::Error>(Some((stream, state)))
				}
			})
			.try_flatten()
			.take_while(|event| {
				future::ready(!matches!(event, Ok(tg::sandbox::status::Event::End)))
			})
			.map(|event| match event {
				Ok(tg::sandbox::status::Event::Status(status)) => Ok(status),
				Err(error) => Err(error),
				_ => unreachable!(),
			})
			.inspect_ok({
				let state = state.clone();
				move |status| {
					state.lock().unwrap().end = status.is_destroyed();
				}
			});
			Ok(Some(stream))
		}
	}

	fn get_sandbox_processes(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::sandbox::processes::get::Chunk>> + Send + 'static,
		>,
	> + Send {
		self.try_get_sandbox_processes(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to find the sandbox")))
		})
	}

	fn try_get_sandbox_processes(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::sandbox::processes::get::Chunk>> + Send + 'static,
			>,
		>,
	> + Send {
		async move {
			let instance = self.clone();
			let id = id.clone();
			let Some(stream) = instance
				.try_get_sandbox_processes_stream(&id, arg.clone())
				.await?
			else {
				return Ok(None);
			};
			let stream = stream.boxed();
			struct State {
				arg: tg::sandbox::processes::get::Arg,
				end: bool,
				stream: Option<
					stream::BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>,
				>,
			}
			let state = Arc::new(Mutex::new(State {
				arg,
				end: false,
				stream: Some(stream),
			}));
			let stream = stream::try_unfold(state.clone(), move |state| {
				let instance = instance.clone();
				let id = id.clone();
				async move {
					if state.lock().unwrap().end {
						return Ok(None);
					}
					let stream = state.lock().unwrap().stream.take();
					let stream = if let Some(stream) = stream {
						stream
					} else {
						let arg = state.lock().unwrap().arg.clone();
						instance
							.try_get_sandbox_processes_stream(&id, arg)
							.await?
							.ok_or_else(|| tg::error!("failed to find the sandbox"))?
							.boxed()
					};
					Ok::<_, tg::Error>(Some((stream, state)))
				}
			})
			.try_flatten()
			.take_while(|event| {
				future::ready(!matches!(
					event,
					Ok(tg::sandbox::processes::get::Event::End)
				))
			})
			.map(|event| match event {
				Ok(tg::sandbox::processes::get::Event::Chunk(chunk)) => Ok(chunk),
				Err(error) => Err(error),
				_ => unreachable!(),
			})
			.inspect_ok({
				let state = state.clone();
				move |chunk| {
					let mut state = state.lock().unwrap();

					// If the chunk is empty, then end the stream.
					if chunk.data.is_empty() {
						state.end = true;
						return;
					}

					// Update the length argument if necessary.
					if let Some(length) = &mut state.arg.length {
						*length -= chunk.data.len().to_u64().unwrap();
					}

					// Update the position argument.
					let position = chunk.position + chunk.data.len().to_u64().unwrap();
					state.arg.position = Some(SeekFrom::Start(position));
				}
			});

			Ok(Some(stream))
		}
	}

	fn get_process_status(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::process::Status>> + Send + 'static>,
	> + Send {
		self.try_get_process_status(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to find the process")))
		})
	}

	fn try_get_process_status(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::Status>> + Send + 'static>,
		>,
	> + Send {
		async move {
			let instance = self.clone();
			let id = id.clone();
			let Some(stream) = instance
				.try_get_process_status_stream(&tg::Either::Right(id.clone()), arg.clone())
				.await?
			else {
				return Ok(None);
			};
			let stream = stream.boxed();
			struct State {
				stream: Option<stream::BoxStream<'static, tg::Result<tg::process::status::Event>>>,
				end: bool,
			}
			let state = Arc::new(Mutex::new(State {
				stream: Some(stream),
				end: false,
			}));
			let stream = stream::try_unfold(state.clone(), move |state| {
				let instance = instance.clone();
				let id = id.clone();
				let arg = arg.clone();
				async move {
					if state.lock().unwrap().end {
						return Ok(None);
					}
					let stream = state.lock().unwrap().stream.take();
					let stream = if let Some(stream) = stream {
						stream
					} else {
						instance
							.try_get_process_status_stream(&tg::Either::Right(id.clone()), arg)
							.await?
							.ok_or_else(|| tg::error!("failed to find the process"))?
							.boxed()
					};
					Ok::<_, tg::Error>(Some((stream, state)))
				}
			})
			.try_flatten()
			.take_while(|event| {
				future::ready(!matches!(event, Ok(tg::process::status::Event::End)))
			})
			.map(|event| match event {
				Ok(tg::process::status::Event::Status(status)) => Ok(status),
				Err(e) => Err(e),
				_ => unreachable!(),
			})
			.inspect_ok({
				let state = state.clone();
				move |status| {
					state.lock().unwrap().end = status.is_finished();
				}
			});
			Ok(Some(stream))
		}
	}

	fn get_process_children(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::process::children::get::Chunk>> + Send + 'static,
		>,
	> + Send {
		self.try_get_process_children(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to find the process")))
		})
	}

	fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::children::get::Chunk>> + Send + 'static,
			>,
		>,
	> + Send {
		async move {
			let instance = self.clone();
			let id = id.clone();
			let Some(stream) = instance
				.try_get_process_children_stream(&tg::Either::Right(id.clone()), arg.clone())
				.await?
			else {
				return Ok(None);
			};
			let stream = stream.boxed();
			struct State {
				stream: Option<
					stream::BoxStream<'static, tg::Result<tg::process::children::get::Event>>,
				>,
				arg: tg::process::children::get::Arg,
				end: bool,
			}
			let state = Arc::new(Mutex::new(State {
				stream: Some(stream),
				arg,
				end: false,
			}));
			let stream = stream::try_unfold(state.clone(), move |state| {
				let instance = instance.clone();
				let id = id.clone();
				async move {
					if state.lock().unwrap().end {
						return Ok(None);
					}
					let stream = state.lock().unwrap().stream.take();
					let stream = if let Some(stream) = stream {
						stream
					} else {
						let arg = state.lock().unwrap().arg.clone();
						instance
							.try_get_process_children_stream(&tg::Either::Right(id.clone()), arg)
							.await?
							.ok_or_else(|| tg::error!("failed to find the process"))?
							.boxed()
					};
					Ok::<_, tg::Error>(Some((stream, state)))
				}
			})
			.try_flatten()
			.take_while(|event| {
				future::ready(!matches!(event, Ok(tg::process::children::get::Event::End)))
			})
			.map(|event| match event {
				Ok(tg::process::children::get::Event::Chunk(chunk)) => Ok(chunk),
				Err(e) => Err(e),
				_ => unreachable!(),
			})
			.inspect_ok({
				let state = state.clone();
				move |chunk| {
					let mut state = state.lock().unwrap();

					// If the chunk is empty, then end the stream.
					if chunk.data.is_empty() {
						state.end = true;
						return;
					}

					// Update the length argument if necessary.
					if let Some(length) = &mut state.arg.length {
						*length -= chunk.data.len().to_u64().unwrap();
					}

					// Update the position argument.
					let length = chunk.data.len().to_u64().unwrap();
					state.arg.position = Some(match state.arg.position {
						Some(SeekFrom::End(position) | SeekFrom::Current(position)) => {
							SeekFrom::End(position + length.to_i64().unwrap())
						},
						None | Some(SeekFrom::Start(_)) => SeekFrom::Start(chunk.position + length),
					});
				}
			});
			Ok(Some(stream))
		}
	}

	fn wait_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> impl Future<Output = tg::Result<tg::process::outcome::Data>> + Send {
		async move {
			let process = tg::Either::Right(id.clone());
			let mut future = self.wait_process_future(&process, arg.clone()).await?;
			loop {
				if let Some(outcome) = future.await? {
					return Ok(outcome);
				}
				future = self.wait_process_future(&process, arg.clone()).await?;
			}
		}
	}

	fn get_sandbox_control_stream_all(
		&self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
		reconnect: impl FnOnce(&tg::sandbox::control::Header) -> Self + Send,
	) -> impl Future<
		Output = tg::Result<(
			tg::sandbox::control::Header,
			impl Stream<Item = tg::Result<tg::control::Event<tg::sandbox::control::ServerMessage>>>
			+ Send
			+ 'static,
		)>,
	> + Send {
		async move {
			let instance = self.clone();
			let deferred = arg.create.is_none();
			#[derive(Default)]
			struct CreateState {
				create: Option<tg::sandbox::control::ClientRequest>,
			}
			let create_state = Arc::new(Mutex::new(CreateState::default()));

			// Create a channel for buffering events from the input.
			let (input_sender, input_receiver) = async_channel::bounded(1);

			// Create the input task. This will read events from the input stream and write them to the input channel. It is detached so that it forwards the remaining events when the request stream is dropped. It completes when the input stream ends or all of the receivers are dropped.
			let mut input_task = Task::spawn({
				let create_state = create_state.clone();
				move |_| async move {
					let mut input = pin!(stream);
					while let Some(event) = input.next().await {
						if deferred
							&& let Ok(tg::sandbox::control::ClientMessage::Request(request)) =
								&event && let tg::sandbox::control::ClientRequestArg::Create(_) =
							&request.arg
						{
							create_state.lock().unwrap().create.replace(request.clone());
						}
						if input_sender.send(event).await.is_err() {
							break;
						}
					}
				}
			});
			input_task.detach();

			// Get the initial header and stream, without retrying, because a create that has no sandbox ID yet is not safe to repeat.
			let result = instance
				.get_sandbox_control_stream(arg.clone(), input_receiver.clone().boxed())
				.await;
			let (header, output_stream) = tangram_futures::retry::once(result)?;
			let stream_retry = instance.arg().stream_retry;
			let instance = reconnect(&header);
			let arg = tg::sandbox::control::Arg {
				id: Some(header.id.clone()),
				..arg
			};

			// Yield events from the stream, reconnecting with backoff when the stream ends early, which is how a stream reports a lost connection.
			struct State {
				error: Option<tg::Error>,
				retries: Option<BoxStream<'static, ()>>,
				stream: Option<BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>>,
			}
			let state = State {
				error: None,
				retries: None,
				stream: Some(output_stream.boxed()),
			};
			let stream = stream::unfold(state, move |mut state| {
				let instance = instance.clone();
				let arg = arg.clone();
				let input_receiver = input_receiver.clone();
				let create_state = create_state.clone();
				let stream_retry = stream_retry.clone();
				async move {
					loop {
						if state.stream.is_none() {
							// A successful create confirms submission, not persistence.
							let create = create_state.lock().unwrap().create.clone();
							let input = stream::iter(create.map(|request| {
								Ok(tg::sandbox::control::ClientMessage::Request(request))
							}))
							.chain(input_receiver.clone())
							.boxed();
							// Fail with the last error once the retries are exhausted.
							let retries = state.retries.get_or_insert_with(|| {
								tangram_futures::retry::stream(stream_retry.clone()).boxed()
							});
							if retries.next().await.is_none() {
								let error = state.error.take().unwrap_or_else(|| {
									tg::error!("the sandbox control stream ended")
								});
								return Some((Err(error), state));
							}
							match instance
								.get_sandbox_control_stream(arg.clone(), input)
								.await
							{
								Ok(ControlFlow::Break((_, stream))) => {
									state.error = None;
									state.stream.replace(stream.boxed());
									return Some((Ok(tg::control::Event::Reconnect), state));
								},
								Ok(ControlFlow::Continue(error)) => {
									tracing::error!(error = %error.trace(), "failed to reconnect the control stream");
									state.error = Some(error);
									continue;
								},
								Err(error) => return Some((Err(error), state)),
							}
						}
						match state.stream.as_mut().unwrap().next().await {
							Some(Ok(event)) => {
								state.retries.take();
								return Some((Ok(tg::control::Event::Message(event)), state));
							},
							Some(Err(error)) => return Some((Err(error), state)),
							None => {
								state.stream.take();
							},
						}
					}
				}
			});

			Ok((header, stream))
		}
	}

	fn try_read_process_stdio_all(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::stdio::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::stdio::Chunk>> + Send + 'static>,
		>,
	> + Send {
		async move {
			let output = self
				.try_read_process_stdio_all_inner(process, arg, None)
				.await?;
			let output = output.map(|stream| {
				stream.try_filter_map(|message| {
					future::ready(Ok(match message {
						tg::process::stdio::read::ServerMessage::Notification(
							tg::process::stdio::read::Event::Chunk(chunk),
						) => Some(chunk),
						tg::process::stdio::read::ServerMessage::Notification(
							tg::process::stdio::read::Event::Position { .. },
						)
						| tg::process::stdio::read::ServerMessage::Response(_) => None,
					}))
				})
			});
			Ok(output)
		}
	}

	/// Read the process's stdio, reconnecting from the acknowledged position. The reader acknowledges each chunk as it yields it, unless `acks` reports the positions that its own reader has acknowledged.
	fn try_read_process_stdio_all_inner(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::stdio::read::Arg,
		acks: Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>>>,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::stdio::read::ServerMessage>> + Send + 'static,
			>,
		>,
	> + Send {
		async move {
			let instance = self.clone();
			let process = process.clone();
			let forward = arg.length.is_none_or(|length| length >= 0);
			let combined = arg.streams.len() > 1;
			let position = match arg.position {
				None => Some(0),
				Some(SeekFrom::Start(position)) => Some(position),
				Some(SeekFrom::Current(_) | SeekFrom::End(_)) => None,
			};

			// Acknowledge the read's bytes, so the runner releases them only once this reader or its own reader has them.
			let (ack_sender, ack_receiver) =
				tokio::sync::watch::channel(position.unwrap_or_default());
			let ack_task = acks.map(|mut acks| {
				let ack_sender = ack_sender.clone();
				Task::spawn(move |_| async move {
					while let Some(Ok(tg::process::stdio::read::ClientMessage::Ack { position })) =
						acks.next().await
					{
						ack_sender.send_if_modified(|acked| {
							let modified = position > *acked;
							*acked = (*acked).max(position);
							modified
						});
					}
				})
			});
			let ack_sender = ack_task.is_none().then_some(ack_sender);
			// Open the read, leaving a read that could not reach a server to be reopened with the retries.
			let input = read_process_stdio_acks(ack_receiver.clone());
			let (output, error) = match instance
				.try_read_process_stdio(&process, arg.clone(), input)
				.await?
			{
				ControlFlow::Break(Some(output)) => (Some(output.boxed()), None),
				ControlFlow::Break(None) => return Ok(None),
				ControlFlow::Continue(error) => {
					tracing::error!(error = %error.trace(), "failed to open the stdio read stream");
					(None, Some(error))
				},
			};
			struct State<I> {
				ack_receiver: tokio::sync::watch::Receiver<u64>,
				ack_sender: Option<tokio::sync::watch::Sender<u64>>,
				arg: tg::process::stdio::read::Arg,
				combined: bool,
				ended: bool,
				error: Option<tg::Error>,
				forward: bool,
				instance: I,
				output:
					Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>,
				position: Option<u64>,
				process: tg::Either<tg::process::Nonce, tg::process::Id>,
				retries: Option<BoxStream<'static, ()>>,
				stream_retry: tangram_futures::retry::Options,
			}
			let stream_retry = instance.arg().stream_retry;
			let state = State {
				ack_receiver,
				ack_sender,
				arg,
				combined,
				ended: false,
				error,
				forward,
				instance,
				output,
				position,
				process,
				retries: None,
				stream_retry,
			};
			let stream = stream::try_unfold(state, move |mut state| async move {
				if state.ended {
					return Ok(None);
				}
				loop {
					if state.output.is_none() {
						// Fail with the last error once the retries are exhausted.
						let retries = state.retries.get_or_insert_with(|| {
							tangram_futures::retry::stream(state.stream_retry.clone()).boxed()
						});
						if retries.next().await.is_none() {
							let error = state.error.take().unwrap_or_else(|| {
								tg::error!(
									unavailable,
									"the process stdio stream ended before completion"
								)
							});
							return Err(error);
						}

						// Resume from the acknowledged position, because the bytes delivered after it may not have reached this reader's own reader.
						let mut arg = state.arg.clone();
						if state.forward
							&& let Some(position) = state.position
						{
							let acked = (*state.ack_receiver.borrow()).min(position);
							arg.position = Some(SeekFrom::Start(acked));
							if let Some(length) = &mut arg.length {
								*length =
									length.saturating_add((position - acked).to_i64().unwrap());
							}
						}
						let input = read_process_stdio_acks(state.ack_receiver.clone());
						match state
							.instance
							.try_read_process_stdio(&state.process, arg, input)
							.await?
						{
							ControlFlow::Break(Some(output)) => {
								state.output = Some(output.boxed());
							},
							ControlFlow::Break(None) => {
								return Err(tg::error!("failed to find the process"));
							},
							ControlFlow::Continue(error) => {
								tracing::error!(error = %error.trace(), "failed to reconnect the stdio read stream");
								state.error = Some(error);
								continue;
							},
						}
					}
					let message = state.output.as_mut().unwrap().next().await;
					match message {
						Some(Ok(tg::process::stdio::read::ServerMessage::Notification(
							tg::process::stdio::read::Event::Chunk(mut chunk),
						))) => {
							state.error = None;
							state.retries.take();
							let start = if state.combined {
								chunk.combined_position
							} else {
								chunk.stream_position
							};
							let length = chunk.bytes.len().to_u64().unwrap();
							let end = start
								.checked_add(length)
								.ok_or_else(|| tg::error!("the stdio position is too large"))?;
							if let Some(position) = state.position {
								if state.forward && end <= position
									|| !state.forward && start >= position
								{
									continue;
								}
								if state.forward && start > position
									|| !state.forward && end < position
								{
									return Err(tg::error!(
										expected = %position,
										start = %start,
										end = %end,
										"encountered a gap in the stdio stream"
									));
								}
								if state.forward && start < position {
									let overlap = (position - start).to_usize().unwrap();
									chunk.bytes = chunk.bytes.slice(overlap..);
									chunk.combined_position += overlap.to_u64().unwrap();
									chunk.stream_position += overlap.to_u64().unwrap();
								} else if !state.forward && end > position {
									let length = (position - start).to_usize().unwrap();
									chunk.bytes = chunk.bytes.slice(..length);
								}
							}
							let length = chunk.bytes.len().to_u64().unwrap();
							let position = if state.forward {
								if state.combined {
									chunk.combined_position + length
								} else {
									chunk.stream_position + length
								}
							} else if state.combined {
								chunk.combined_position
							} else {
								chunk.stream_position
							};
							if let Some(remaining) = &mut state.arg.length {
								let length = length.to_i64().unwrap();
								if *remaining >= 0 {
									*remaining -= length.min(*remaining);
								} else {
									*remaining = remaining.saturating_add(length).min(0);
								}
							}
							state.arg.position = Some(SeekFrom::Start(position));

							state.position = Some(position);
							if state.forward
								&& let Some(ack_sender) = &state.ack_sender
							{
								ack_sender.send_replace(position);
							}

							let notification =
								tg::process::stdio::read::ServerMessage::Notification(
									tg::process::stdio::read::Event::Chunk(chunk),
								);
							return Ok(Some((notification, state)));
						},
						Some(Ok(tg::process::stdio::read::ServerMessage::Notification(
							tg::process::stdio::read::Event::Position { length, position },
						))) => {
							state.arg.length = length;
							state.arg.position = Some(SeekFrom::Start(position));
							state.position = Some(position);
							let message = tg::process::stdio::read::ServerMessage::Notification(
								tg::process::stdio::read::Event::Position { length, position },
							);
							return Ok(Some((message, state)));
						},
						Some(Ok(tg::process::stdio::read::ServerMessage::Response(output))) => {
							output
								.validate(&state.arg.streams, state.position.unwrap_or_default())?;
							state.ended = true;
							let message = tg::process::stdio::read::ServerMessage::Response(output);
							return Ok(Some((message, state)));
						},
						Some(Err(error)) => return Err(error),
						None => {
							state.output.take();
						},
					}
				}
			});
			let stream = match ack_task {
				Some(task) => stream.attach(task).left_stream(),
				None => stream.right_stream(),
			};
			Ok(Some(stream))
		}
	}

	fn write_process_stdio_all(
		&self,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		let input = input
			.map_ok(|chunk| tg::process::stdio::write::Input {
				chunk,
				completion: None,
			})
			.boxed();
		tg::process::stdio::write::all(self, process, arg, input)
	}
}

impl<T> Ext for T where T: tg::Instance {}

fn read_process_stdio_acks(
	ack_receiver: tokio::sync::watch::Receiver<u64>,
) -> BoxStream<'static, tg::Result<tg::process::stdio::read::ClientMessage>> {
	tokio_stream::wrappers::WatchStream::new(ack_receiver)
		.map(|position| Ok(tg::process::stdio::read::ClientMessage::Ack { position }))
		.boxed()
}
