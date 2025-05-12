use crate as tg;
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future,
	stream::{self, BoxStream},
};
use num::ToPrimitive as _;
use std::{
	io::SeekFrom,
	sync::{Arc, Mutex},
};
use tangram_either::Either;

pub trait Ext: tg::Handle {
	fn read_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::read::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::blob::read::Chunk>> + Send + 'static>,
	> + Send {
		self.try_read_blob(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the blob")))
		})
	}

	fn try_read_blob(
		&self,
		id: &tg::blob::Id,
		arg: tg::blob::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::blob::read::Chunk>> + Send + 'static>,
		>,
	> + Send {
		async move {
			let handle = self.clone();
			let id = id.clone();
			let Some(stream) = handle.try_read_blob_stream(&id, arg.clone()).await? else {
				return Ok(None);
			};
			let stream = stream.boxed();
			struct State {
				stream: Option<BoxStream<'static, tg::Result<tg::blob::read::Event>>>,
				arg: tg::blob::read::Arg,
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
							.try_read_blob_stream(&id, arg)
							.await?
							.unwrap()
							.boxed()
					};
					Ok::<_, tg::Error>(Some((stream, state)))
				}
			})
			.try_flatten()
			.take_while(|event| future::ready(!matches!(event, Ok(tg::blob::read::Event::End))))
			.map(|event| match event {
				Ok(tg::blob::read::Event::Chunk(chunk)) => Ok(chunk),
				Err(e) => Err(e),
				_ => unreachable!(),
			})
			.inspect_ok(move |chunk| {
				let mut state = state.lock().unwrap();

				// Compute the end condition.
				state.end = chunk.bytes.is_empty() || matches!(state.arg.length, Some(0));

				// Update the length argument.
				if let Some(length) = &mut state.arg.length {
					*length -= chunk.bytes.len().to_u64().unwrap().min(*length);
				}

				// Update the position argument.
				let position = chunk.position + chunk.bytes.len().to_u64().unwrap();
				state.arg.position = Some(SeekFrom::Start(position));
			});
			Ok(Some(stream))
		}
	}

	fn get_process_metadata(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<tg::process::Metadata>> + Send {
		self.try_get_process_metadata(id).map(move |result| {
			result.and_then(|option| {
				option.ok_or_else(|| tg::error!(?id, "failed to get the process metadata"))
			})
		})
	}

	fn get_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<tg::process::get::Output>> + Send {
		self.try_get_process(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the process")))
		})
	}

	fn dequeue_process(
		&self,
		arg: tg::process::dequeue::Arg,
	) -> impl Future<Output = tg::Result<tg::process::dequeue::Output>> + Send {
		self.try_dequeue_process(arg).map(|result| {
			result
				.and_then(|option| option.ok_or_else(|| tg::error!("failed to dequeue a process")))
		})
	}

	fn get_process_status(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::process::Status>> + Send + 'static>,
	> + Send {
		self.try_get_process_status(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the process")))
		})
	}

	fn try_get_process_status(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::Status>> + Send + 'static>,
		>,
	> + Send {
		async move {
			let handle = self.clone();
			let id = id.clone();
			let Some(stream) = handle.try_get_process_status_stream(&id).await? else {
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
				async move {
					if state.lock().unwrap().end {
						return Ok(None);
					}
					let stream = state.lock().unwrap().stream.take();
					let stream = if let Some(stream) = stream {
						stream
					} else {
						handle
							.try_get_process_status_stream(&id)
							.await?
							.unwrap()
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
							.unwrap()
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

	fn try_get_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::process::log::get::Chunk>> + Send + 'static>,
		>,
	> + Send {
		async move {
			let handle = self.clone();
			let id = id.clone();
			let Some(stream) = handle.try_get_process_log_stream(&id, arg.clone()).await? else {
				return Ok(None);
			};
			let stream = stream.boxed();
			struct State {
				stream: Option<BoxStream<'static, tg::Result<tg::process::log::get::Event>>>,
				arg: tg::process::log::get::Arg,
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
							.try_get_process_log_stream(&id, arg)
							.await?
							.unwrap()
							.boxed()
					};
					Ok::<_, tg::Error>(Some((stream, state)))
				}
			})
			.try_flatten()
			.take_while(|event| {
				future::ready(!matches!(event, Ok(tg::process::log::get::Event::End)))
			})
			.map(|event| match event {
				Ok(tg::process::log::get::Event::Chunk(chunk)) => Ok(chunk),
				Err(e) => Err(e),
				_ => unreachable!(),
			})
			.inspect_ok(move |chunk| {
				let mut state = state.lock().unwrap();

				// Compute the end condition.
				let forward = state.arg.length.is_none_or(|l| l >= 0);
				state.end = chunk.bytes.is_empty()
					|| (!forward && chunk.position == 0)
					|| matches!(state.arg.length, Some(0));

				// Update the length argument.
				if let Some(length) = &mut state.arg.length {
					if *length >= 0 {
						*length -= chunk.bytes.len().to_i64().unwrap().min(*length);
					} else {
						*length += chunk.bytes.len().to_i64().unwrap().min(length.abs());
					}
				}

				// Update the position argument.
				let position = if forward {
					chunk.position + chunk.bytes.len().to_u64().unwrap()
				} else {
					chunk.position.saturating_sub(1)
				};
				state.arg.position = Some(SeekFrom::Start(position));
			});
			Ok(Some(stream))
		}
	}

	fn get_process_log(
		&self,
		id: &tg::process::Id,
		arg: tg::process::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::process::log::get::Chunk>> + Send + 'static,
		>,
	> + Send {
		self.try_get_process_log(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the process")))
		})
	}

	fn wait_process_future(
		&self,
		id: &tg::process::Id,
	) -> impl Future<
		Output = tg::Result<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static,
		>,
	> + Send {
		self.try_wait_process_future(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the process")))
		})
	}

	fn wait_process(
		&self,
		id: &tg::process::Id,
	) -> impl Future<Output = tg::Result<tg::process::wait::Output>> + Send {
		async move {
			let mut future = self.wait_process_future(id).await?;
			loop {
				if let Some(output) = future.await? {
					return Ok(output);
				}
				future = self.wait_process_future(id).await?;
			}
		}
	}

	fn get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<tg::object::Metadata>> + Send {
		self.try_get_object_metadata(id).map(move |result| {
			result.and_then(|option| {
				option.ok_or_else(|| tg::error!(?id, "failed to get the object metadata"))
			})
		})
	}

	fn get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<tg::object::get::Output>> + Send {
		self.try_get_object(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the object")))
		})
	}

	fn spawn_process(
		&self,
		arg: tg::process::spawn::Arg,
	) -> impl Future<Output = tg::Result<tg::process::spawn::Output>> + Send {
		self.try_spawn_process(arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("expected a process")))
		})
	}

	fn get(
		&self,
		reference: &tg::Reference,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<
				Item = tg::Result<
					tg::progress::Event<tg::Referent<Either<tg::Process, tg::Object>>>,
				>,
			> + Send
			+ 'static,
		>,
	> + Send {
		self.try_get(reference).map(|result| {
			result.map(|stream| {
				let reference = reference.clone();
				stream.map(move |event_result| {
					event_result.and_then(|event| match event {
						crate::progress::Event::Log(log) => Ok(tg::progress::Event::Log(log)),
						crate::progress::Event::Diagnostic(diagnostic) => {
							Ok(tg::progress::Event::Diagnostic(diagnostic))
						},
						crate::progress::Event::Start(indicator) => {
							Ok(tg::progress::Event::Start(indicator))
						},
						crate::progress::Event::Update(indicator) => {
							Ok(tg::progress::Event::Update(indicator))
						},
						crate::progress::Event::Finish(indicator) => {
							Ok(tg::progress::Event::Finish(indicator))
						},
						crate::progress::Event::Output(output) => output
							.map(|output| {
								let referent = output.referent.map(|item| {
									item.map_left(|id| tg::Process::new(id, None, None, None, None))
										.map_right(tg::Object::with_id)
								});
								crate::progress::Event::Output(referent)
							})
							.ok_or_else(|| tg::error!(%reference, "failed to get reference")),
					})
				})
			})
		})
	}

	fn get_tag(
		&self,
		tag: &tg::tag::Pattern,
	) -> impl Future<Output = tg::Result<tg::tag::get::Output>> + Send {
		self.try_get_tag(tag).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the tag")))
		})
	}
}

impl<T> Ext for T where T: tg::Handle {}
