use {
	crate::prelude::*,
	futures::{
		FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream},
	},
	num::ToPrimitive as _,
	std::{
		io::SeekFrom,
		ops::ControlFlow,
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

	fn get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<tg::process::Metadata>> + Send {
		let arg = tg::process::metadata::Arg::default();
		self.try_get_process_metadata(id, arg).map(move |result| {
			result.and_then(|option| {
				option.ok_or_else(|| tg::error!(?id, "failed to get the process metadata"))
			})
		})
	}

	fn get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<tg::process::get::Output>> + Send {
		let arg = tg::process::get::Arg::default();
		self.try_get_process(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the process")))
		})
	}

	fn get_sandbox(
		&self,
		id: &tg::sandbox::Id,
	) -> impl Future<Output = tg::Result<tg::sandbox::get::Output>> + Send {
		let arg = tg::sandbox::get::Arg::default();
		self.try_get_sandbox(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the sandbox")))
		})
	}

	fn dequeue_process(
		&self,
		sandbox: &tg::sandbox::Id,
		arg: tg::process::queue::Arg,
	) -> impl Future<Output = tg::Result<tg::process::queue::Output>> + Send {
		self.try_dequeue_process(sandbox, arg).map(|result| {
			result
				.and_then(|option| option.ok_or_else(|| tg::error!("failed to dequeue a process")))
		})
	}

	fn dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::queue::Output>> + Send {
		self.try_dequeue_sandbox(arg).map(|result| {
			result
				.and_then(|option| option.ok_or_else(|| tg::error!("failed to dequeue a sandbox")))
		})
	}

	fn get_sandbox_status(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::sandbox::Status>> + Send + 'static>,
	> + Send {
		self.try_get_sandbox_status(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the sandbox")))
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
							.ok_or_else(|| tg::error!("the stream was not found"))?
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
					state.lock().unwrap().end = status.is_finished();
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
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the process")))
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
							.ok_or_else(|| tg::error!("the stream was not found"))?
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
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the process")))
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
							.ok_or_else(|| tg::error!("the stream was not found"))?
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
					let position = chunk.position + chunk.data.len().to_u64().unwrap();
					state.arg.position = Some(SeekFrom::Start(position));
				}
			});
			Ok(Some(stream))
		}
	}

	fn wait_process_future(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
		>,
	> + Send {
		self.try_wait_process_future(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the process")))
		})
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

	fn get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<tg::object::Metadata>> + Send {
		let arg = tg::object::metadata::Arg::default();
		self.try_get_object_metadata(id, arg).map(move |result| {
			result.and_then(|option| {
				option.ok_or_else(|| tg::error!(%id, "failed to get the object metadata"))
			})
		})
	}

	fn get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> impl Future<Output = tg::Result<tg::object::get::Output>> + Send {
		self.try_get_object(id, arg).map(|result| {
			result.and_then(|option| {
				option.ok_or_else(|| tg::error!(%id, "failed to find the object"))
			})
		})
	}

	fn spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::process::spawn::Output>>>
			+ Send
			+ 'static,
		>,
	> {
		self.try_spawn_process(arg).map_ok(|stream| {
			stream.and_then(|event| {
				future::ready(
					event.try_map_output(|item| {
						item.ok_or_else(|| tg::error!("expected a process"))
					}),
				)
			})
		})
	}

	fn get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<
				Item = tg::Result<
					tg::progress::Event<tg::Referent<tg::Either<tg::Object, tg::Process>>>,
				>,
			> + Send
			+ 'static,
		>,
	> + Send {
		self.try_get(reference, arg).map(|result| {
			result.map(|stream| {
				let reference = reference.clone();
				stream.map(move |event_result| {
					event_result.and_then(|event| match event {
						tg::progress::Event::Log(log) => Ok(tg::progress::Event::Log(log)),
						tg::progress::Event::Diagnostic(diagnostic) => {
							Ok(tg::progress::Event::Diagnostic(diagnostic))
						},
						tg::progress::Event::Indicators(indicators) => {
							Ok(tg::progress::Event::Indicators(indicators))
						},
						tg::progress::Event::Output(output) => output
							.map(|output| {
								let referent = output.referent.map(|item| {
									item.map_left(tg::Object::with_id).map_right(|id| {
										tg::Process::new(id, None, None, None, None, None)
									})
								});
								tg::progress::Event::Output(referent)
							})
							.ok_or_else(|| tg::error!(%reference, "failed to get the reference")),
					})
				})
			})
		})
	}

	fn try_read_process_stdio_all(
		&self,
		id: &tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::process::stdio::read::Event>> + Send + 'static,
			>,
		>,
	> + Send {
		async move {
			let handle = self.clone();
			let id = id.clone();
			let Some(stream) = handle.try_read_process_stdio(&id, arg.clone()).await? else {
				return Ok(None);
			};
			struct State<H> {
				arg: tg::process::stdio::read::Arg,
				end: bool,
				handle: H,
				id: tg::process::Id,
				stream: Option<BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>,
			}
			let state = State {
				arg,
				end: false,
				handle,
				id,
				stream: Some(stream.boxed()),
			};
			let stream = stream::try_unfold(state, move |mut state| async move {
				loop {
					if state.end {
						return Ok(None);
					}
					let mut stream = if let Some(stream) = state.stream.take() {
						stream
					} else {
						state
							.handle
							.try_read_process_stdio(&state.id, state.arg.clone())
							.await?
							.ok_or_else(|| tg::error!("the stream was not found"))?
							.boxed()
					};
					match stream.next().await.transpose()? {
						Some(event) => {
							state.end = matches!(event, tg::process::stdio::read::Event::End);
							if let tg::process::stdio::read::Event::Chunk(chunk) = &event
								&& let Some(position) = chunk.position
							{
								let forward = state.arg.length.is_none_or(|length| length >= 0);
								if let Some(length) = &mut state.arg.length {
									if *length >= 0 {
										*length -= chunk.bytes.len().to_i64().unwrap().min(*length);
									} else {
										*length +=
											chunk.bytes.len().to_i64().unwrap().min(length.abs());
									}
								}
								let position = if forward {
									position + chunk.bytes.len().to_u64().unwrap()
								} else {
									position.saturating_sub(1)
								};
								state.arg.position = Some(SeekFrom::Start(position));
							}
							if !state.end {
								state.stream = Some(stream);
							}
							return Ok(Some((event, state)));
						},
						None => {
							state.stream = None;
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
		arg: tg::process::stdio::write::Arg,
		input: BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			// Create a channel for notifications.
			let (notification_sender, notification_receiver) = async_channel::bounded(1);

			// Create a channel for the events.
			let (event_sender, event_receiver) =
				async_channel::bounded::<tg::Result<tg::process::stdio::read::Event>>(16);

			// Spawn a task to forward events from the input stream to the event channel.
			let send_task = Task::spawn(move |_stopper| async move {
				let mut input = std::pin::pin!(input);
				while let Some(event) = input.next().await {
					if event_sender.send(event).await.is_err() {
						break;
					}

					// Check if there is a pending notification. This is done instead of a select() to avoid cancelling the input stream future.
					match notification_receiver.try_recv() {
						Ok(()) => {
							if notification_receiver.recv().await.is_err() {
								break;
							}
						},
						Err(async_channel::TryRecvError::Empty) => (),
						Err(async_channel::TryRecvError::Closed) => break,
					}
				}
				notification_receiver.close();
			});

			// Write input events in a retry loop.
			let handle = self.clone();
			let result =
				tangram_futures::retry(&tangram_futures::retry::Options::default(), move || {
					let event_receiver = event_receiver.clone();
					let notification_sender = notification_sender.clone();
					let arg = arg.clone();
					let handle = handle.clone();
					async move {
						let events = event_receiver.clone().boxed();
						let result = handle
							.write_process_stdio(id, arg, events)
							.await
							.map(futures::StreamExt::boxed);
						let mut output = match result {
							Ok(output) => output,
							Err(error) => return Ok(ControlFlow::Continue(error)),
						};
						match output.next().await {
							Some(Ok(tg::process::stdio::write::Event::Stop)) => {
								if notification_sender.send(()).await.is_err() {
									return Ok(ControlFlow::Break(()));
								}
								while let Some(event) = output.next().await {
									match event {
										Ok(tg::process::stdio::write::Event::Stop) => (),
										Ok(tg::process::stdio::write::Event::End) => {
											if notification_sender.send(()).await.is_err() {
												return Ok(ControlFlow::Break(()));
											}
											return Ok(ControlFlow::Continue(tg::error!(
												"the stream ended early"
											)));
										},
										Err(error) => {
											return Ok(ControlFlow::Continue(error));
										},
									}
								}
								return Err(tg::error!("expected an end event"));
							},
							Some(Ok(tg::process::stdio::write::Event::End)) => {
								notification_sender.close();
								return Ok(ControlFlow::Break(()));
							},
							Some(Err(error)) => {
								return Ok(ControlFlow::Continue(error));
							},
							None => (),
						}
						Ok(ControlFlow::Continue(tg::error!(
							"expected an output event"
						)))
					}
				})
				.await
				.map_err(|source| tg::error!(!source, "failed to write process stdio"));

			send_task
				.wait()
				.await
				.map_err(|source| tg::error!(!source, "the send task panicked"))?;

			result
		}
	}
}

impl<T> Ext for T where T: tg::Handle {}
