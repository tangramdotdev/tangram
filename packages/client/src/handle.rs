use crate as tg;
use bytes::Bytes;
use futures::{
	future,
	stream::{self, BoxStream},
	Future, FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use num::ToPrimitive;
use std::{
	io::SeekFrom,
	sync::{Arc, Mutex},
};
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite};

mod either;

pub trait Handle: Clone + Unpin + Send + Sync + 'static {
	fn check_in_artifact(
		&self,
		arg: tg::artifact::checkin::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checkin::Output>> + Send;

	fn check_out_artifact(
		&self,
		id: &tg::artifact::Id,
		arg: tg::artifact::checkout::Arg,
	) -> impl Future<Output = tg::Result<tg::artifact::checkout::Output>> + Send;

	fn create_blob(
		&self,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::blob::create::Output>> + Send;

	fn try_get_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<Option<tg::build::get::Output>>> + Send;

	fn get_build(
		&self,
		id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<tg::build::get::Output>> + Send {
		self.try_get_build(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn put_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::put::Arg,
	) -> impl Future<Output = tg::Result<tg::build::put::Output>> + Send;

	fn push_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::build::push::Event>> + Send + 'static,
		>,
	> + Send;

	fn pull_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::build::pull::Event>> + Send + 'static,
		>,
	> + Send;

	fn try_dequeue_build(
		&self,
		arg: tg::build::dequeue::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::build::dequeue::Output>>> + Send;

	fn dequeue_build(
		&self,
		arg: tg::build::dequeue::Arg,
	) -> impl Future<Output = tg::Result<tg::build::dequeue::Output>> + Send {
		self.try_dequeue_build(arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to dequeue a build")))
		})
	}

	fn try_start_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::start::Arg,
	) -> impl Future<Output = tg::Result<Option<bool>>> + Send;

	fn start_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::start::Arg,
	) -> impl Future<Output = tg::Result<bool>> + Send {
		self.try_start_build(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to dequeue a build")))
		})
	}

	fn try_get_build_status_stream(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::status::Event>> + Send + 'static>,
		>,
	> + Send;

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
				Ok(tg::build::status::Event::Data(status)) => Ok(status),
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

	fn try_get_build_children_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::children::Event>> + Send + 'static>,
		>,
	> + Send;

	fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::children::Data>> + Send + 'static>,
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
				stream: Option<stream::BoxStream<'static, tg::Result<tg::build::children::Event>>>,
				arg: tg::build::children::Arg,
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
				future::ready(!matches!(event, Ok(tg::build::children::Event::End)))
			})
			.map(|event| match event {
				Ok(tg::build::children::Event::Data(chunk)) => Ok(chunk),
				Err(e) => Err(e),
				_ => unreachable!(),
			})
			.inspect_ok({
				let state = state.clone();
				move |chunk| {
					let mut state = state.lock().unwrap();

					// If the chunk is empty, then end the stream.
					if chunk.items.is_empty() {
						state.end = true;
						return;
					}

					// Update the length argument if necessary.
					if let Some(length) = &mut state.arg.length {
						*length -= chunk.items.len().to_u64().unwrap();
					}

					// Update the position argument.
					let position = chunk.position + chunk.items.len().to_u64().unwrap();
					state.arg.position = Some(SeekFrom::Start(position));
				}
			});
			Ok(Some(stream))
		}
	}

	fn get_build_children(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::build::children::Data>> + Send + 'static,
		>,
	> + Send {
		self.try_get_build_children(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn add_build_child(
		&self,
		id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_build_log_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::log::Event>> + Send + 'static>,
		>,
	> + Send;

	fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::log::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>,
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
				stream: Option<BoxStream<'static, tg::Result<tg::build::log::Event>>>,
				arg: tg::build::log::Arg,
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
			.take_while(|event| future::ready(!matches!(event, Ok(tg::build::log::Event::End))))
			.map(|event| match event {
				Ok(tg::build::log::Event::Data(chunk)) => Ok(chunk),
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
		arg: tg::build::log::Arg,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::build::log::Chunk>> + Send + 'static>,
	> + Send {
		self.try_get_build_log(id, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the build")))
		})
	}

	fn add_build_log(
		&self,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_build_outcome_future(
		&self,
		id: &tg::build::Id,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
		>,
	> + Send;

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

	fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn touch_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn heartbeat_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::heartbeat::Arg,
	) -> impl Future<Output = tg::Result<tg::build::heartbeat::Output>> + Send;

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<tg::object::Metadata>> + Send {
		self.try_get_object_metadata(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the object")))
		})
	}

	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> + Send;

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> + Send;

	fn get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<tg::object::get::Output>> + Send {
		self.try_get_object(id).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the object")))
		})
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<tg::object::put::Output>> + Send;

	fn push_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::object::push::Event>> + Send + 'static,
		>,
	> + Send;

	fn pull_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::object::pull::Event>> + Send + 'static,
		>,
	> + Send;

	fn list_packages(
		&self,
		arg: tg::package::list::Arg,
	) -> impl Future<Output = tg::Result<tg::package::list::Output>> + Send;

	fn try_get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::package::get::Output>>> + Send;

	fn get_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::get::Arg,
	) -> impl Future<Output = tg::Result<tg::package::get::Output>> + Send {
		self.try_get_package(dependency, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the package")))
		})
	}

	fn check_package(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::check::Arg,
	) -> impl Future<Output = tg::Result<Vec<tg::Diagnostic>>> + Send;

	fn try_get_package_doc(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::doc::Arg,
	) -> impl Future<Output = tg::Result<Option<serde_json::Value>>> + Send;

	fn get_package_doc(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::doc::Arg,
	) -> impl Future<Output = tg::Result<serde_json::Value>> + Send {
		self.try_get_package_doc(dependency, arg).map(|result| {
			result.and_then(|option| option.ok_or_else(|| tg::error!("failed to get the package")))
		})
	}

	fn format_package(
		&self,
		dependency: &tg::Dependency,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_package_outdated(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::outdated::Arg,
	) -> impl Future<Output = tg::Result<tg::package::outdated::Output>> + Send;

	fn publish_package(
		&self,
		id: &tg::artifact::Id,
		arg: tg::package::publish::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::versions::Arg,
	) -> impl Future<Output = tg::Result<Option<Vec<String>>>> + Send;

	fn get_package_versions(
		&self,
		dependency: &tg::Dependency,
		arg: tg::package::versions::Arg,
	) -> impl Future<Output = tg::Result<tg::package::versions::Output>> + Send {
		self.try_get_package_versions(dependency, arg)
			.map(|result| {
				result.and_then(|option| {
					option.ok_or_else(|| tg::error!("failed to get the package"))
				})
			})
	}

	fn yank_package(
		&self,
		id: &tg::artifact::Id,
		arg: tg::package::yank::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> + Send;

	fn try_get_remote(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> + Send;

	fn put_remote(
		&self,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_remote(&self, name: &str) -> impl Future<Output = tg::Result<()>> + Send;

	fn list_roots(
		&self,
		arg: tg::root::list::Arg,
	) -> impl Future<Output = tg::Result<tg::root::list::Output>> + Send;

	fn try_get_root(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::root::get::Output>>> + Send;

	fn put_root(
		&self,
		name: &str,
		arg: tg::root::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_root(&self, name: &str) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_js_runtime_doc(&self) -> impl Future<Output = tg::Result<serde_json::Value>> + Send;

	fn health(&self) -> impl Future<Output = tg::Result<tg::server::Health>> + Send;

	fn clean(&self) -> impl Future<Output = tg::Result<()>> + Send;

	fn build_target(
		&self,
		id: &tg::target::Id,
		arg: tg::target::build::Arg,
	) -> impl Future<Output = tg::Result<tg::target::build::Output>> + Send;

	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> + Send;
}
