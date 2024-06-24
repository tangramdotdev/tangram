use crate as tg;
use futures::{
	future,
	stream::{self, BoxStream},
	Future, FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use num::ToPrimitive as _;
use std::{
	io::SeekFrom,
	sync::{Arc, Mutex},
};

pub trait Ext: tg::Handle {
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

	fn get_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<tg::build::get::Output>> + Send {
		self.try_get_build(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn dequeue_build(
		&self,
		arg: tg::build::dequeue::Arg,
	) -> impl Future<Output = tg::Result<tg::build::dequeue::Output>> + Send {
		self.try_dequeue_build(arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to dequeue a build")))
		})
	}

	fn try_get_build_status(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>,
		>,
	> + Send {
		async move {
			let handle = self.clone();
			let id = id.clone();
			let Some(stream) = handle.try_get_build_status_stream(&id).await? else {
				return Ok(None);
			};
			let stream = stream.boxed();
			struct State {
				stream: Option<stream::BoxStream<'static, tg::Result<tg::build::status::Event>>>,
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
							.try_get_build_status_stream(&id)
							.await?
							.unwrap()
							.boxed()
					};
					Ok::<_, tg::Error>(Some((stream, state)))
				}
			})
			.try_flatten()
			.take_while(|event| future::ready(!matches!(event, Ok(tg::build::status::Event::End))))
			.map(|event| match event {
				Ok(tg::build::status::Event::Status(status)) => Ok(status),
				Err(e) => Err(e),
				_ => unreachable!(),
			})
			.inspect_ok({
				let state = state.clone();
				move |status| {
					state.lock().unwrap().end = matches!(status, tg::build::Status::Finished);
				}
			});
			Ok(Some(stream))
		}
	}

	fn get_build_status(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>,
	> + Send {
		self.try_get_build_status(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Stream<Item = tg::Result<tg::build::children::get::Chunk>> + Send + 'static,
			>,
		>,
	> + Send {
		async move {
			let handle = self.clone();
			let id = id.clone();
			let Some(stream) = handle
				.try_get_build_children_stream(&id, arg.clone())
				.await?
			else {
				return Ok(None);
			};
			let stream = stream.boxed();
			struct State {
				stream:
					Option<stream::BoxStream<'static, tg::Result<tg::build::children::get::Event>>>,
				arg: tg::build::children::get::Arg,
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
							.try_get_build_children_stream(&id, arg)
							.await?
							.unwrap()
							.boxed()
					};
					Ok::<_, tg::Error>(Some((stream, state)))
				}
			})
			.try_flatten()
			.take_while(|event| {
				future::ready(!matches!(event, Ok(tg::build::children::get::Event::End)))
			})
			.map(|event| match event {
				Ok(tg::build::children::get::Event::Chunk(chunk)) => Ok(chunk),
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

	fn get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::build::children::get::Chunk>> + Send + 'static,
		>,
	> + Send {
		self.try_get_build_children(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::log::get::Chunk>> + Send + 'static>,
		>,
	> + Send {
		async move {
			let handle = self.clone();
			let id = id.clone();
			let Some(stream) = handle.try_get_build_log_stream(&id, arg.clone()).await? else {
				return Ok(None);
			};
			let stream = stream.boxed();
			struct State {
				stream: Option<BoxStream<'static, tg::Result<tg::build::log::get::Event>>>,
				arg: tg::build::log::get::Arg,
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
							.try_get_build_log_stream(&id, arg)
							.await?
							.unwrap()
							.boxed()
					};
					Ok::<_, tg::Error>(Some((stream, state)))
				}
			})
			.try_flatten()
			.take_while(|event| {
				future::ready(!matches!(event, Ok(tg::build::log::get::Event::End)))
			})
			.map(|event| match event {
				Ok(tg::build::log::get::Event::Chunk(chunk)) => Ok(chunk),
				Err(e) => Err(e),
				_ => unreachable!(),
			})
			.inspect_ok(move |chunk| {
				let mut state = state.lock().unwrap();

				// Compute the end condition.
				let forward = state.arg.length.map_or(true, |l| l >= 0);
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
					chunk.position - 1
				};
				state.arg.position = Some(SeekFrom::Start(position));
			});
			Ok(Some(stream))
		}
	}

	fn get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::build::log::get::Chunk>> + Send + 'static,
		>,
	> + Send {
		self.try_get_build_log(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
		>,
	> + Send {
		async move {
			let Some(future) = self.try_get_build_outcome_future(id).await? else {
				return Ok(None);
			};
			let future = {
				let mut future = future.boxed();
				let handle = self.clone();
				let id = id.clone();
				async move {
					loop {
						if let Some(outcome) = future.await? {
							return Ok(Some(outcome));
						};
						future = handle
							.try_get_build_outcome_future(&id)
							.await?
							.ok_or_else(|| tg::error!("expected the build to exist"))?
							.boxed();
					}
				}
			};
			Ok(Some(future))
		}
	}

	fn get_build_outcome(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static,
		>,
	> + Send {
		self.try_get_build_outcome(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<tg::object::Metadata>> + Send {
		self.try_get_object_metadata(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the object")))
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

	fn get_tag(
		&self,
		tag: &tg::tag::Pattern,
	) -> impl Future<Output = tg::Result<tg::tag::get::Output>> + Send {
		self.try_get_tag(tag).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the tag")))
		})
	}

	fn build_target(
		&self,
		id: &tg::target::Id,
		arg: tg::target::build::Arg,
	) -> impl Future<Output = tg::Result<tg::target::build::Output>> + Send {
		self.try_build_target(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("expected a build")))
		})
	}
}

impl<T> Ext for T where T: tg::Handle {}
