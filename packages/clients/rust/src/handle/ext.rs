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
					let position = chunk.position + chunk.data.len().to_u64().unwrap();
					state.arg.position = Some(SeekFrom::Start(position));
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
							.ok_or_else(|| tg::error!("failed to find the process"))?
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
			// Create a channel for buffering events from the input.
			let (event_sender, event_receiver) = async_channel::bounded(1);

			// Create a channel for notifying the input task when a stop signal has been received.
			let (stop_sender, mut stop_receiver) =
				tokio::sync::mpsc::channel::<tokio::sync::oneshot::Receiver<()>>(1);

			// Create the input task. This will read events from the input stream and write them to the event channel.
			let input_task = Task::spawn({
				move |_| async move {
					let input = input.peekable();
					let mut input = pin!(input);
					loop {
						let event = pin!(input.as_mut().peek());
						let stop = pin!(stop_receiver.recv());
						match future::select(event, stop).await {
							future::Either::Left((event, _)) => {
								// Try to send the event. Exit the loop if it's the end of the input.
								let Some(event) = event.cloned() else {
									break;
								};
								input.next().await;
								if event_sender.send(Some(event)).await.is_err() {
									break;
								}
							},
							future::Either::Right((end_receiver, _)) => {
								// Try to send the sentinel. Exit the loop if the channel was closed.
								let Some(end_receiver) = end_receiver else {
									break;
								};
								if event_sender.send(None).await.is_err() {
									break;
								}
								end_receiver.await.ok();
							},
						}
					}
					Ok::<_, tg::Error>(())
				}
			});

			// Create the output task.
			let id = id.clone();
			let handle = self.clone();
			let output_task = Task::spawn(async move |_| {
				let options = tangram_futures::retry::Options::default();
				// In a retry loop, attempt to drain input.
				tangram_futures::retry(&options, {
					|| {
						let arg = arg.clone();
						let event_receiver = event_receiver.clone();
						let handle = handle.clone();
						let id = id.clone();
						let stop_sender = stop_sender.clone();
						async move {
							// The input stream is chunked around `Stop` boundaries using `None` as a sentinel value.
							let input = event_receiver
								.take_while(|opt| future::ready(opt.is_some()))
								.take_while_inclusive(|result| {
									future::ready(result.as_ref().is_some_and(Result::is_ok))
								})
								.filter_map(future::ready)
								.boxed();

							// Try to create the output stream.
							let mut output = match handle.write_process_stdio(&id, arg, input).await
							{
								Ok(stream) => stream.boxed(),
								// Retry if write returned an error.
								Err(error) => {
									return Ok(ControlFlow::Continue(tg::error!(
										!error,
										"failed to get the stdio stream"
									)));
								},
							};

							// Drain the output stream.
							let mut end_sender = None;
							while let Some(output) = output.next().await {
								match output {
									Ok(tg::process::stdio::write::Event::Stop) => {
										// Skip duplicate stop events.
										if end_sender.is_some() {
											continue;
										}

										// Create the end event channel.
										let (tx, rx) = tokio::sync::oneshot::channel();
										end_sender.replace(tx);

										// Send the end event notification, breaking out if this fails.
										if stop_sender.send(rx).await.is_err() {
											return Ok(ControlFlow::Break(()));
										}
									},

									Ok(tg::process::stdio::write::Event::End) => {
										// If an end sender was created by receiving a previous stop, use it.
										if let Some(end_sender) = end_sender.take() {
											if end_sender.send(()).is_err() {
												return Ok(ControlFlow::Break(()));
											}

											// Retry.
											return Ok(ControlFlow::Continue(tg::error!(
												"retrying stdio"
											)));
										}
										break;
									},

									Err(error) => {
										// Retry if the server returned an error.
										return Ok(ControlFlow::Continue(tg::error!(
											!error,
											"stdio stream returned an error"
										)));
									},
								}
							}

							Ok(ControlFlow::Break(()))
						}
					}
				})
				.await
			});

			// Wait for both tasks.
			let (input, output) = future::join(input_task.wait(), output_task.wait()).await;
			input.map_err(|source| tg::error!(!source, "the input task panicked"))??;
			output.map_err(|source| tg::error!(!source, "the output task panicked"))??;
			Ok(())
		}
	}
}

impl<T> Ext for T where T: tg::Handle {}
