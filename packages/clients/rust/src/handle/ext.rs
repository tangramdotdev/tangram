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
			impl Stream<Item = tg::Result<tg::sandbox::control::ServerMessage>> + Send + 'static,
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
								return Some((Ok(event), state));
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
				impl Stream<Item = tg::Result<tg::process::control::ServerMessage>> + Send + 'static,
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
				id: Some(output.id.clone()),
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
								return Some((Ok(event), state));
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
			let handle = self.clone();
			let id = id.clone();
			let position = match arg.position {
				None => Some(0),
				Some(SeekFrom::Start(position)) => Some(position),
				Some(SeekFrom::Current(_) | SeekFrom::End(_)) => None,
			};
			let (sender, receiver) = async_channel::bounded(1);
			if let Some(position) = position {
				let message = tg::process::stdio::read::ClientMessage::Notification(
					tg::process::stdio::read::ClientNotification::Read { position },
				);
				sender.try_send(Ok(message)).unwrap();
			}
			let Some(output) = handle
				.try_read_process_stdio(&id, arg.clone(), receiver.boxed())
				.await?
			else {
				return Ok(None);
			};
			struct State<H> {
				arg: tg::process::stdio::read::Arg,
				combined: bool,
				forward: bool,
				handle: H,
				id: tg::process::Id,
				output:
					Option<BoxStream<'static, tg::Result<tg::process::stdio::read::ServerMessage>>>,
				pending_notification: bool,
				position: Option<u64>,
				retries: Option<BoxStream<'static, ()>>,
				sender: Option<
					async_channel::Sender<tg::Result<tg::process::stdio::read::ClientMessage>>,
				>,
			}
			let combined = arg.streams.len() > 1;
			let forward = arg.length.is_none_or(|length| length >= 0);
			let state = State {
				arg,
				combined,
				forward,
				handle,
				id,
				output: Some(output.boxed()),
				pending_notification: false,
				position,
				retries: None,
				sender: Some(sender),
			};
			let stream = stream::try_unfold(state, move |mut state| async move {
				loop {
					if state.pending_notification {
						let position = state.position.unwrap();
						let message = tg::process::stdio::read::ClientMessage::Notification(
							tg::process::stdio::read::ClientNotification::Read { position },
						);
						let result = state.sender.as_ref().unwrap().send(Ok(message)).await;
						if result.is_err() {
							state.output.take();
							state.sender.take();
							continue;
						}
						state.pending_notification = false;
					}
					if state.output.is_none() {
						let retries = state.retries.get_or_insert_with(|| {
							let options = tangram_futures::retry::Options {
								max_retries: u64::MAX,
								..Default::default()
							};
							tangram_futures::retry::stream(options).boxed()
						});
						retries.next().await;
						let (sender, receiver) = async_channel::bounded(1);
						if let Some(position) = state.position {
							let message = tg::process::stdio::read::ClientMessage::Notification(
								tg::process::stdio::read::ClientNotification::Read { position },
							);
							sender.try_send(Ok(message)).unwrap();
						}
						match state
							.handle
							.try_read_process_stdio(&state.id, state.arg.clone(), receiver.boxed())
							.await
						{
							Ok(Some(output)) => {
								state.output = Some(output.boxed());
								state.pending_notification = false;
								state.sender = Some(sender);
							},
							Ok(None) => return Err(tg::error!("failed to find the process")),
							Err(error) => {
								tracing::error!(error = %error.trace(), "failed to reconnect the stdio read stream");
								continue;
							},
						}
					}
					let message = state.output.as_mut().unwrap().next().await;
					match message {
						Some(Ok(tg::process::stdio::read::ServerMessage::Notification(
							tg::process::stdio::read::ServerNotification::Chunk(mut chunk),
						))) => {
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
									state.pending_notification = true;
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
									*remaining += length.min(remaining.abs());
								}
							}
							state.arg.position = Some(SeekFrom::Start(position));
							state.pending_notification = true;
							state.position = Some(position);

							return Ok(Some((chunk, state)));
						},
						Some(Ok(tg::process::stdio::read::ServerMessage::Notification(
							tg::process::stdio::read::ServerNotification::Stop,
						)))
						| None => {
							state.output.take();
							state.sender.take();
						},
						Some(Ok(tg::process::stdio::read::ServerMessage::Request(
							tg::process::stdio::read::ServerRequest::End,
						))) => {
							let message = tg::process::stdio::read::ClientMessage::Response(
								tg::process::stdio::read::ClientResponse::End,
							);
							if state
								.sender
								.as_ref()
								.unwrap()
								.send(Ok(message))
								.await
								.is_err()
							{
								state.output.take();
								state.sender.take();
								continue;
							}
							return Ok(None);
						},
						Some(Err(error)) => return Err(error),
					}
				}
			});

			Ok(Some(stream))
		}
	}

	fn write_process_stdio_all(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::Chunk>>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			enum Event {
				Input(tg::Result<Option<tg::process::stdio::Chunk>>),
				Output(Option<tg::Result<tg::process::stdio::write::ServerMessage>>),
			}

			// Forward the input so waiting for it cannot block server messages.
			let (input_sender, input_receiver) =
				async_channel::bounded::<tg::Result<Option<tg::process::stdio::Chunk>>>(1);
			let _input_task = Task::spawn(move |_| async move {
				let mut input = pin!(input);
				while let Some(result) = input.next().await {
					let end = result.is_err();
					if input_sender.send(result.map(Some)).await.is_err() || end {
						return;
					}
				}
				input_sender.send(Ok(None)).await.ok();
			});

			let combined = arg.streams.len() > 1;
			let mut connected = false;
			let mut end_sent = false;
			let id = id.clone();
			let handle = self.clone();
			let mut input_ended = false;
			let mut output =
				None::<BoxStream<'static, tg::Result<tg::process::stdio::write::ServerMessage>>>;
			let mut pending = None::<tg::process::stdio::Chunk>;
			let mut pending_sent = false;
			let mut retries = None::<BoxStream<'static, ()>>;
			let mut sender =
				None::<async_channel::Sender<tg::Result<tg::process::stdio::write::ClientMessage>>>;
			loop {
				// Connect or reconnect.
				if output.is_none() {
					if let Some(retries) = &mut retries {
						retries.next().await;
					}
					let (new_sender, receiver) = async_channel::bounded(1);
					match handle
						.try_write_process_stdio(&id, arg.clone(), receiver.boxed())
						.await
					{
						Ok(Some(new_output)) => {
							connected = true;
							end_sent = false;
							output = Some(new_output.boxed());
							pending_sent = false;
							sender = Some(new_sender);
						},
						Ok(None) => return Err(tg::error!("failed to find the process")),
						Err(error) if connected => {
							tracing::error!(error = %error.trace(), "failed to reconnect the stdio write stream");
							let options = tangram_futures::retry::Options {
								max_retries: u64::MAX,
								..Default::default()
							};
							retries.get_or_insert_with(|| {
								tangram_futures::retry::stream(options).boxed()
							});
							continue;
						},
						Err(error) => return Err(error),
					}
				}

				// Send the next pending message.
				if let Some(chunk) = &pending
					&& !pending_sent
				{
					if !arg.streams.contains(&chunk.stream) {
						return Err(tg::error!("invalid process stdio stream"));
					}
					let message = tg::process::stdio::write::ClientMessage::Notification(
						tg::process::stdio::write::ClientNotification::Chunk(chunk.clone()),
					);
					if sender.as_ref().unwrap().send(Ok(message)).await.is_err() {
						output.take();
						sender.take();
						continue;
					}
					pending_sent = true;
				} else if pending.is_none() && input_ended && !end_sent {
					let message = tg::process::stdio::write::ClientMessage::Request(
						tg::process::stdio::write::ClientRequest::End,
					);
					if sender.as_ref().unwrap().send(Ok(message)).await.is_err() {
						output.take();
						sender.take();
						continue;
					}
					end_sent = true;
				}

				// Receive whichever side is ready.
				let event = if pending.is_none() && !input_ended {
					tokio::select! {
						result = input_receiver.recv() => {
							let result = result.map_err(|_| tg::error!("the process stdio input task stopped"))?;
							Event::Input(result)
						},
						message = output.as_mut().unwrap().next() => Event::Output(message),
					}
				} else {
					Event::Output(output.as_mut().unwrap().next().await)
				};
				match event {
					Event::Input(result) => match result? {
						Some(chunk) => {
							if !arg.streams.contains(&chunk.stream) {
								return Err(tg::error!("invalid process stdio stream"));
							}
							pending = Some(chunk);
						},
						None => input_ended = true,
					},
					Event::Output(Some(Ok(
						tg::process::stdio::write::ServerMessage::Notification(
							tg::process::stdio::write::ServerNotification::Stop,
						),
					))) => {
						output.take();
						sender.take();
					},
					Event::Output(Some(Ok(
						tg::process::stdio::write::ServerMessage::Notification(
							tg::process::stdio::write::ServerNotification::Write { position },
						),
					))) => {
						retries.take();
						let Some(mut chunk) = pending.take() else {
							continue;
						};
						let start = if combined {
							chunk.combined_position
						} else {
							chunk.stream_position
						};
						let end = start
							.checked_add(chunk.bytes.len().to_u64().unwrap())
							.ok_or_else(|| tg::error!("the stdio position is too large"))?;
						if position > end {
							return Err(tg::error!(
								%end,
								%position,
								"invalid process stdio write position"
							));
						}
						if position <= start {
							pending = Some(chunk);
							pending_sent = false;
							continue;
						}
						if position < end {
							let overlap = (position - start).to_usize().unwrap();
							chunk.bytes = chunk.bytes.slice(overlap..);
							chunk.combined_position += overlap.to_u64().unwrap();
							chunk.stream_position += overlap.to_u64().unwrap();
							pending = Some(chunk);
						}
						pending_sent = false;
					},
					Event::Output(Some(Ok(
						tg::process::stdio::write::ServerMessage::Response(
							tg::process::stdio::write::ServerResponse::End,
						),
					))) => {
						return Ok(());
					},
					Event::Output(Some(Err(error))) => return Err(error),
					Event::Output(None) => {
						output.take();
						sender.take();
						let options = tangram_futures::retry::Options {
							max_retries: u64::MAX,
							..Default::default()
						};
						retries
							.get_or_insert_with(|| tangram_futures::retry::stream(options).boxed());
					},
				}
			}
		}
	}
}

impl<T> Ext for T where T: tg::Handle {}
