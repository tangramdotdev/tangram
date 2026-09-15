use {
	crate::prelude::*,
	futures::{
		FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	std::{
		io::SeekFrom,
		pin::pin,
		sync::{Arc, Mutex},
	},
	tangram_futures::task::Task,
};

pub trait Ext: tg::Handle {
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
			let handle = self.clone();
			let Some(stream) = handle.try_read_stream(arg.clone()).await? else {
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
				let handle = handle.clone();
				async move {
					if state.lock().unwrap().end {
						return Ok(None);
					}
					let stream = state.lock().unwrap().stream.take();
					let stream = if let Some(stream) = stream {
						stream
					} else {
						let arg = state.lock().unwrap().arg.clone();
						handle
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
			let handle = self.clone();
			let id = id.clone();
			let Some(stream) = handle
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
				let handle = handle.clone();
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
						handle
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
			let handle = self.clone();
			let id = id.clone();
			let Some(stream) = handle
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
				let handle = handle.clone();
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
						handle
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
			let handle = self.clone();
			let id = id.clone();
			let Some(stream) = handle
				.try_get_process_status_stream(&id, arg.clone())
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
				let handle = handle.clone();
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
						handle
							.try_get_process_status_stream(&id, arg)
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
			let handle = self.clone();
			let id = id.clone();
			let Some(stream) = handle
				.try_get_process_children_stream(&id, arg.clone())
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
				let handle = handle.clone();
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
						handle
							.try_get_process_children_stream(&id, arg)
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
	) -> impl Future<Output = tg::Result<tg::process::wait::Output>> + Send {
		async move {
			let mut future = self.wait_process_future(id, arg.clone()).await?;
			loop {
				if let Some(output) = future.await? {
					return Ok(output);
				}
				future = self.wait_process_future(id, arg.clone()).await?;
			}
		}
	}

	fn get_sandbox_control_stream_all(
		&self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
		reconnect: impl FnOnce(&tg::sandbox::control::Output) -> Self + Send,
	) -> impl Future<
		Output = tg::Result<(
			tg::sandbox::control::Output,
			impl Stream<Item = tg::Result<tg::control::Event<tg::sandbox::control::ServerMessage>>>
			+ Send
			+ 'static,
		)>,
	> + Send {
		async move {
			let handle = self.clone();

			// Create a channel for buffering events from the input.
			let (input_sender, input_receiver) = async_channel::bounded(1);

			// Create the input task. This will read events from the input stream and write them to the input channel. It is detached so that it forwards the remaining events when the request stream is dropped. It completes when the input stream ends or all of the receivers are dropped.
			let mut input_task = Task::spawn(move |_| async move {
				let mut input = pin!(stream);
				while let Some(event) = input.next().await {
					if input_sender.send(event).await.is_err() {
						break;
					}
				}
			});
			input_task.detach();

			// Get the initial output stream.
			let (output, output_stream) = handle
				.get_sandbox_control_stream(arg.clone(), input_receiver.clone().boxed())
				.await?;
			let handle = reconnect(&output);
			let arg = tg::sandbox::control::Arg {
				id: Some(output.id.clone()),
				..arg
			};

			// Yield events from the stream, reconnecting with backoff when the stream ends or returns an error.
			struct State {
				retries: Option<BoxStream<'static, ()>>,
				stream: Option<BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>>,
			}
			let state = State {
				retries: None,
				stream: Some(output_stream.boxed()),
			};
			let stream = stream::unfold(state, move |mut state| {
				let handle = handle.clone();
				let arg = arg.clone();
				let input_receiver = input_receiver.clone();
				async move {
					loop {
						if state.stream.is_none() {
							let retries = state.retries.get_or_insert_with(|| {
								let options = tangram_futures::retry::Options {
									max_retries: u64::MAX,
									..Default::default()
								};
								tangram_futures::retry::stream(options).boxed()
							});
							retries.next().await?;
							match handle
								.get_sandbox_control_stream(
									arg.clone(),
									input_receiver.clone().boxed(),
								)
								.await
							{
								Ok((_, stream)) => {
									state.stream.replace(stream.boxed());
									return Some((Ok(tg::control::Event::Reconnect), state));
								},
								Err(error) => {
									tracing::error!(error = %error.trace(), "failed to reconnect the control stream");
									continue;
								},
							}
						}
						match state.stream.as_mut().unwrap().next().await {
							Some(Ok(event)) => {
								state.retries.take();
								return Some((Ok(tg::control::Event::Message(event)), state));
							},
							Some(Err(error)) => {
								tracing::error!(error = %error.trace(), "the control stream returned an error");
								state.stream.take();
							},
							None => {
								state.stream.take();
							},
						}
					}
				}
			});

			Ok((output, stream))
		}
	}

	fn try_get_process_control_stream_all(
		&self,
		arg: tg::process::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::process::control::ClientMessage>>,
		reconnect: impl FnOnce(&tg::process::control::Output) -> Self + Send,
	) -> impl Future<
		Output = tg::Result<
			Option<(
				tg::process::control::Output,
				impl Stream<Item = tg::Result<tg::control::Event<tg::process::control::ServerMessage>>>
				+ Send
				+ 'static,
			)>,
		>,
	> + Send {
		async move {
			let handle = self.clone();

			// Create a channel for buffering events from the input.
			let (response_sender, response_receiver) = async_channel::bounded(1);

			// Create the input task. This will read events from the input stream and write them to the response channel. It is detached so that it forwards the remaining events when the request stream is dropped. It completes when the input stream ends or all of the receivers are dropped.
			let mut input_task = Task::spawn(move |_| async move {
				let mut input = pin!(stream);
				while let Some(event) = input.next().await {
					if response_sender.send(event).await.is_err() {
						break;
					}
				}
			});
			input_task.detach();

			// Get the initial output stream.
			let Some((output, output_stream)) = handle
				.try_get_process_control_stream(arg.clone(), response_receiver.clone().boxed())
				.await?
			else {
				input_task.abort();
				return Ok(None);
			};
			let handle = reconnect(&output);
			let arg = tg::process::control::Arg {
				data: None,
				id: Some(output.process.node.clone()),
				sync: output.sync.clone(),
				..arg
			};

			// Yield events from the stream, reconnecting with backoff when the stream ends or returns an error.
			struct State {
				retries: Option<BoxStream<'static, ()>>,
				stream: Option<BoxStream<'static, tg::Result<tg::process::control::ServerMessage>>>,
			}
			let state = State {
				retries: None,
				stream: Some(output_stream.boxed()),
			};
			let stream = stream::unfold(state, move |mut state| {
				let handle = handle.clone();
				let arg = arg.clone();
				let response_receiver = response_receiver.clone();
				async move {
					loop {
						if state.stream.is_none() {
							let retries = state.retries.get_or_insert_with(|| {
								let options = tangram_futures::retry::Options {
									max_retries: u64::MAX,
									..Default::default()
								};
								tangram_futures::retry::stream(options).boxed()
							});
							retries.next().await?;
							match handle
								.try_get_process_control_stream(
									arg.clone(),
									response_receiver.clone().boxed(),
								)
								.await
							{
								Ok(Some((_, stream))) => {
									state.stream.replace(stream.boxed());
									return Some((Ok(tg::control::Event::Reconnect), state));
								},
								Ok(None) => {
									let error = tg::error!("failed to find the process");
									return Some((Err(error), state));
								},
								Err(error) => {
									tracing::error!(error = %error.trace(), "failed to reconnect the control stream");
									continue;
								},
							}
						}
						match state.stream.as_mut().unwrap().next().await {
							Some(Ok(event)) => {
								state.retries.take();
								return Some((Ok(tg::control::Event::Message(event)), state));
							},
							Some(Err(error)) => {
								tracing::error!(error = %error.trace(), "the control stream returned an error");
								state.stream.take();
							},
							None => {
								state.stream.take();
							},
						}
					}
				}
			});

			Ok(Some((output, stream)))
		}
	}

	fn try_read_process_stdio_all(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::stdio::Chunk>> + Send + 'static>,
		>,
	> + Send {
		async move {
			let output = self.try_read_process_stdio_all_inner(id, arg).await?;
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

	fn try_read_process_stdio_all_inner(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::stdio::read::ServerMessage>> + Send + 'static,
			>,
		>,
	> + Send {
		async move {
			let handle = self.clone();
			let id = id.clone();
			let forward = arg.length.is_none_or(|length| length >= 0);
			let combined = arg.streams.len() > 1;
			let position = match arg.position {
				None => Some(0),
				Some(SeekFrom::Start(position)) => Some(position),
				Some(SeekFrom::Current(_) | SeekFrom::End(_)) => None,
			};
			let (sender, receiver) = async_channel::bounded(4);
			let Some(output) = handle
				.try_read_process_stdio(&id, arg.clone(), receiver.boxed())
				.await?
			else {
				return Ok(None);
			};
			struct State<H> {
				arg: tg::process::stdio::read::Arg,
				combined: bool,
				ended: bool,
				flow: tg::process::stdio::flow::Receiver,
				forward: bool,
				handle: H,
				id: tg::process::Id,
				output:
					Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>,
				pending: usize,
				position: Option<u64>,
				retries: Option<BoxStream<'static, ()>>,
				sender: async_channel::Sender<tg::Result<tg::process::stdio::read::ClientMessage>>,
			}
			let state = State {
				arg,
				combined,
				ended: false,
				flow: tg::process::stdio::flow::Receiver::default(),
				forward,
				handle,
				id,
				output: Some(output.boxed()),
				pending: 0,
				position,
				retries: None,
				sender,
			};
			let stream = stream::try_unfold(state, move |mut state| async move {
				if state.ended {
					return Ok(None);
				}
				loop {
					if state.output.is_none() {
						let retries = state.retries.get_or_insert_with(|| {
							let options = tangram_futures::retry::Options {
								max_retries: u64::MAX,
								..Default::default()
							};
							tangram_futures::retry::stream(options).boxed()
						});
						retries.next().await;
						let (sender, receiver) = async_channel::bounded(4);
						match state
							.handle
							.try_read_process_stdio(&state.id, state.arg.clone(), receiver.boxed())
							.await
						{
							Ok(Some(output)) => {
								state.flow = tg::process::stdio::flow::Receiver::default();
								state.output = Some(output.boxed());
								state.pending = 0;
								state.sender = sender;
							},
							Ok(None) => return Err(tg::error!("failed to find the process")),
							Err(error) => {
								tracing::error!(error = %error.trace(), "failed to reconnect the stdio read stream");
								continue;
							},
						}
					}
					if let Some(progress) =
						state.flow.consume(std::mem::take(&mut state.pending))?
					{
						let message =
							tg::process::stdio::read::ClientMessage::Notification(progress);
						// Preserve terminal errors on the response stream when the input closes.
						state.sender.send(Ok(message)).await.ok();
					}
					let message = state.output.as_mut().unwrap().next().await;
					match message {
						Some(Ok(tg::process::stdio::read::ServerMessage::Notification(
							tg::process::stdio::read::Event::Chunk(mut chunk),
						))) => {
							state.retries.take();
							state.pending = chunk.bytes.len();
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
							state
								.sender
								.send(Ok(tg::process::stdio::read::ClientMessage::Ack))
								.await
								.ok();
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
			Ok(Some(stream))
		}
	}

	fn write_process_stdio_all(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::stream::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		let input = input
			.map_ok(|chunk| tg::process::stdio::write::Input {
				chunk,
				completion: None,
			})
			.boxed();
		tg::process::stdio::write::all(self, id, arg, input)
	}
}

impl<T> Ext for T where T: tg::Handle {}
