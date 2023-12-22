use super::Server;
use crate::{
	BuildQueueTaskMessage, BuildState, BuildStateInner, BuildStatus, ChildrenState, LogState,
	OutcomeState, StopState,
};
use async_recursion::async_recursion;
use bytes::Bytes;
use futures::{
	stream::{self, BoxStream, FuturesUnordered},
	StreamExt, TryStreamExt,
};
use std::sync::Arc;
use tangram_client as tg;
use tangram_error::{return_error, Result, Wrap, WrapErr};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_stream::wrappers::BroadcastStream;

impl Server {
	pub(crate) async fn run_build_queue(
		&self,
		mut receiver: tokio::sync::mpsc::UnboundedReceiver<BuildQueueTaskMessage>,
	) -> Result<()> {
		loop {
			loop {
				// Get the highest priority item from the queue.
				let Some(item) = self.inner.build_queue.lock().unwrap().pop() else {
					break;
				};

				// If the build is at a unique depth, then start it.
				if self
					.inner
					.build_state
					.read()
					.unwrap()
					.values()
					.filter(|state| *state.inner.status.lock().unwrap() == BuildStatus::Building)
					.all(|state| state.inner.depth != item.depth)
				{
					// Set the build's status to building.
					{
						let mut state = self.inner.build_state.write().unwrap();
						if let Some(state) = state.get_mut(&item.build) {
							*state.inner.status.lock().unwrap() = BuildStatus::Building;
						};
					}

					// Start the build.
					self.start_build(None, &item.build, item.depth, item.retry, None);

					continue;
				}

				// Otherwise, attempt to acquire a permit for it.
				match self.inner.build_permits.clone().try_acquire_owned() {
					Ok(permit) => {
						// Set the build's status to building.
						{
							let mut state = self.inner.build_state.write().unwrap();
							let state = state.get_mut(&item.build).unwrap();
							*state.inner.status.lock().unwrap() = BuildStatus::Building;
						}

						// Start the build.
						self.start_build(None, &item.build, item.depth, item.retry, Some(permit));

						continue;
					},
					// If there are no permits available, then add the item back to the queue and break.
					Err(tokio::sync::TryAcquireError::NoPermits) => {
						self.inner.build_queue.lock().unwrap().push(item);
						break;
					},
					Err(tokio::sync::TryAcquireError::Closed) => {
						return_error!("The build queue task was closed.");
					},
				};
			}

			// Wait for a message on the channel.
			let message = receiver.recv().await.unwrap();
			match message {
				// If this is a build added or finished message, then attempt to start
				BuildQueueTaskMessage::BuildAdded | BuildQueueTaskMessage::BuildFinished => {
					continue;
				},

				// If this is a stop message, then break.
				BuildQueueTaskMessage::Stop => return Ok(()),
			}
		}
	}

	pub async fn run_build_queue_remote(
		&self,
		mut receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
	) -> Result<()> {
		// Get the remote.
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(());
		};

		// Loop until a message is received.
		while receiver.try_recv().is_err() {
			// If the queue is full, then sleep and continue.
			let len = self.inner.build_queue.lock().unwrap().len();
			if len >= 10 {
				tokio::time::sleep(std::time::Duration::from_secs(1)).await;
				continue;
			}

			// Attempt to get an item from the remote. If none is available, then sleep and continue.
			let Some(item) = remote.get_build_from_queue(None, None).await.ok().flatten() else {
				tokio::time::sleep(std::time::Duration::from_secs(1)).await;
				continue;
			};

			// Add the item to the queue.
			self.inner.build_queue.lock().unwrap().push(item);

			// Send a message to the build queue task that the item has been added.
			self.inner
				.build_queue_task_sender
				.send(BuildQueueTaskMessage::BuildAdded)
				.unwrap();
		}

		Ok(())
	}

	/// Attempt to get the build for a target.
	pub async fn try_get_build_for_target(
		&self,
		id: &tg::target::Id,
	) -> Result<Option<tg::build::Id>> {
		// Attempt to get the build for the target from the build assignments.
		'a: {
			let Some(build_id) = self
				.inner
				.build_assignments
				.read()
				.unwrap()
				.get(id)
				.cloned()
			else {
				break 'a;
			};
			return Ok(Some(build_id));
		}

		// Attempt to get the build for the target from the database.
		'a: {
			let Some(build_id) = self.inner.database.try_get_build_for_target(id)? else {
				break 'a;
			};
			return Ok(Some(build_id));
		}

		// Attempt to get the build for the target from the remote.
		'a: {
			// Get the remote.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};

			// Get the build for the target from the remote.
			let Some(build_id) = remote.try_get_build_for_target(id).await? else {
				break 'a;
			};

			// Add the assignment to the database.
			self.inner.database.set_build_for_target(id, &build_id)?;

			return Ok(Some(build_id));
		}

		Ok(None)
	}

	/// Get or create a build for a target.
	pub async fn get_or_create_build_for_target(
		&self,
		user: Option<&tg::User>,
		id: &tg::target::Id,
		depth: u64,
		retry: tg::build::Retry,
	) -> Result<tg::build::Id> {
		let target = tg::Target::with_id(id.clone());
		let host = target.host(self).await?.clone();

		// Attempt to get the build for the target.
		if let Some(build_id) = self.try_get_build_for_target(id).await? {
			let build = tg::build::Build::with_id(build_id.clone());
			if let Some(object) = build.try_get_object(self).await? {
				let retry = retry >= object.outcome.retry();
				if !retry {
					return Ok(build_id);
				}
			} else {
				return Ok(build_id);
			}
		}

		// Decide whether to attempt to escalate the build.
		let escalate = true;

		// Attempt to escalate the build.
		if escalate {
			if let Some(remote) = self.inner.remote.as_ref() {
				let object = tg::object::Handle::with_id(id.clone().into());
				let result = object.push(self, remote.as_ref()).await;
				if result.is_ok() {
					if let Ok(build_id) = remote
						.get_or_create_build_for_target(user, id, depth, retry)
						.await
					{
						return Ok(build_id);
					}
				}
			}
		}

		// Otherwise, create a new build.
		let build_id = tg::build::Id::new();

		// Create the status.
		let status = std::sync::Mutex::new(BuildStatus::Queued);

		// Create the stop state.
		let (sender, receiver) = tokio::sync::watch::channel(false);
		let stop = StopState { sender, receiver };

		// Create the children state.
		let children = std::sync::Mutex::new(ChildrenState {
			children: Vec::new(),
			sender: Some(tokio::sync::broadcast::channel(1024).0),
		});

		// Create the log state.
		let log = Arc::new(tokio::sync::Mutex::new(LogState {
			file: tokio::fs::File::from_std(
				tempfile::tempfile().wrap_err("Failed to create the temporary file.")?,
			),
			sender: Some(tokio::sync::broadcast::channel(1024).0),
		}));

		// Create the result state.
		let (sender, receiver) = tokio::sync::watch::channel(None);
		let outcome = OutcomeState { sender, receiver };

		// Create the build state.
		let state = BuildState {
			inner: Arc::new(BuildStateInner {
				status,
				depth,
				stop,
				target,
				children,
				log,
				outcome,
			}),
		};

		// Add the state to the server.
		self.inner
			.build_state
			.write()
			.unwrap()
			.insert(build_id.clone(), state.clone());

		// Add the assignment.
		self.inner
			.build_assignments
			.write()
			.unwrap()
			.insert(id.clone(), build_id.clone());

		// Add the build to the queue.
		self.inner
			.build_queue
			.lock()
			.unwrap()
			.push(tg::build::queue::Item {
				build: build_id.clone(),
				host,
				depth,
				retry,
			});

		// Send a message to the build queue task that the item has been added.
		self.inner
			.build_queue_task_sender
			.send(BuildQueueTaskMessage::BuildAdded)
			.unwrap();

		Ok(build_id)
	}

	pub(crate) fn start_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		depth: u64,
		retry: tg::build::Retry,
		permit: Option<tokio::sync::OwnedSemaphorePermit>,
	) {
		tokio::spawn({
			let server = self.clone();
			let user = user.cloned();
			let id = id.clone();
			async move {
				if let Err(error) = server
					.start_build_inner(user.as_ref(), &id, depth, retry, permit)
					.await
				{
					tracing::error!(?error, "The build failed.");
				}
			}
		});
	}

	async fn start_build_inner(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		depth: u64,
		retry: tg::build::Retry,
		permit: Option<tokio::sync::OwnedSemaphorePermit>,
	) -> Result<()> {
		tracing::info!(?id, "Starting build.");

		let build = tg::Build::with_id(id.clone());
		let target = build.target(self).await?;
		let stop = self
			.inner
			.build_state
			.read()
			.unwrap()
			.get(id)
			.unwrap()
			.inner
			.stop
			.receiver
			.clone();

		// Build the target with the appropriate runtime.
		let result = match target.host(self).await?.os() {
			tg::system::Os::Js => {
				// Build the target on the server's local pool because it is a `!Send` future.
				self.inner
					.local_pool
					.spawn_pinned({
						let server = self.clone();
						let build = build.clone();
						let main_runtime_handle = tokio::runtime::Handle::current();
						move || async move {
							tangram_runtime::js::build(
								&server,
								&build,
								depth,
								retry,
								stop,
								main_runtime_handle,
							)
							.await
						}
					})
					.await
					.wrap_err("Failed to join the build task.")?
			},
			tg::system::Os::Darwin => {
				#[cfg(target_os = "macos")]
				{
					tangram_runtime::darwin::build(self, &build, retry, stop, self.path()).await
				}
				#[cfg(not(target_os = "macos"))]
				{
					return_error!("Cannot build a darwin target on this host.");
				}
			},
			tg::system::Os::Linux => {
				#[cfg(target_os = "linux")]
				{
					tangram_runtime::linux::build(self, &build, retry, stop, self.path()).await
				}
				#[cfg(not(target_os = "linux"))]
				{
					return_error!("Cannot build a linux target on this host.");
				}
			},
		};

		// Create the outcome.
		let outcome = match result {
			// If there's no result then we can't do anything else. We assume this means the build was canceled and build.finish() was already called.
			Ok(None) => return Ok(()),
			Ok(Some(value)) => tg::build::Outcome::Succeeded(value),
			Err(error) => tg::build::Outcome::Failed(error),
		};

		// If the build failed, add a message to the build's log.
		if let tg::build::Outcome::Failed(error) = &outcome {
			build
				.add_log(self, error.trace().to_string().into())
				.await?;
		}

		// Finish the build.
		build.finish(self, user, outcome).await?;

		// Send a message to the build queue task that the build has finished.
		self.inner
			.build_queue_task_sender
			.send(BuildQueueTaskMessage::BuildFinished)
			.unwrap();

		// Drop the permit.
		drop(permit);

		Ok(())
	}

	pub async fn get_build_from_queue(
		&self,
		user: Option<&tg::User>,
		hosts: Option<Vec<tg::System>>,
	) -> Result<Option<tg::build::queue::Item>> {
		// Attempt to get a build from the queue from the remote.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			let Some(item) = remote.get_build_from_queue(user, hosts).await? else {
				break 'a;
			};
			return Ok(Some(item));
		}

		return_error!("Failed to get a build from the queue.");
	}

	pub async fn try_get_build_target(&self, id: &tg::build::Id) -> Result<Option<tg::target::Id>> {
		// Attempt to get the target from the state.
		let state = self.inner.build_state.read().unwrap().get(id).cloned();
		if let Some(state) = state {
			return Ok(Some(state.inner.target.id(self).await?.clone()));
		}

		// Attempt to get the target from the object.
		'a: {
			let build = tg::Build::with_id(id.clone());
			let Some(object) = build.try_get_object(self).await? else {
				break 'a;
			};
			return Ok(Some(object.target.id(self).await?.clone()));
		}

		// Attempt to get the target from the remote.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			let Some(target) = remote.try_get_build_target(id).await? else {
				break 'a;
			};
			return Ok(Some(target));
		}

		Ok(None)
	}

	pub async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Id>>>> {
		// Attempt to get the children from the state.
		'a: {
			// Get the state.
			let Some(state) = self.inner.build_state.read().unwrap().get(id).cloned() else {
				break 'a;
			};

			// Lock the children state.
			let state = state.inner.children.lock().unwrap();

			// Get the old children.
			let old = stream::iter(state.children.clone()).map(Ok);

			// Get a stream of the new children.
			let new = if let Some(sender) = state.sender.as_ref() {
				BroadcastStream::new(sender.subscribe())
					.map_err(|err| err.wrap("Failed to create the stream."))
					.boxed()
			} else {
				stream::empty().boxed()
			};

			// Create the complete children stream.
			let children = old.chain(new).map_ok(|build| build.id().clone()).boxed();

			return Ok(Some(children));
		}

		// Attempt to get the children from the object.
		'a: {
			let build = tg::Build::with_id(id.clone());
			let Some(object) = build.try_get_object(self).await? else {
				break 'a;
			};
			return Ok(Some(
				stream::iter(object.children.clone())
					.map(|build| Ok(build.id().clone()))
					.boxed(),
			));
		}

		// Attempt to get the children from the remote.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			let Some(children) = remote.try_get_build_children(id).await? else {
				break 'a;
			};
			return Ok(Some(children));
		}

		Ok(None)
	}

	pub async fn add_build_child(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> Result<()> {
		// Attempt to add the child to the state.
		'a: {
			// Get the state.
			let Some(state) = self
				.inner
				.build_state
				.read()
				.unwrap()
				.get(build_id)
				.cloned()
			else {
				break 'a;
			};

			// Check if the build is stopped.
			if *state.inner.stop.receiver.borrow() {
				return_error!("The build is stopped.");
			}

			// Add the child.
			let child = tg::Build::with_id(child_id.clone());
			let mut state = state.inner.children.lock().unwrap();
			if let Some(sender) = state.sender.as_ref().cloned() {
				state.children.push(child.clone());
				sender.send(child.clone()).ok();
			}

			return Ok(());
		}

		// Attempt to add the child to the remote.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			remote.add_build_child(user, build_id, child_id).await?;
			return Ok(());
		}

		return_error!("Failed to find the build.");
	}

	pub async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<BoxStream<'static, Result<Bytes>>>> {
		// Attempt to get the log from the state.
		'a: {
			// Get the state.
			let Some(state) = self.inner.build_state.read().unwrap().get(id).cloned() else {
				break 'a;
			};

			// Lock the log state.
			let mut state = state.inner.log.lock().await;

			// Rewind the log.
			state
				.file
				.rewind()
				.await
				.wrap_err("Failed to rewind the log file.")?;

			// Read the existing log.
			let mut old = Vec::new();
			state
				.file
				.read_to_end(&mut old)
				.await
				.wrap_err("Failed to read the log.")?;
			let old = stream::once(async move { Ok(old.into()) });

			// Get the new log stream.
			let new = if let Some(sender) = state.sender.as_ref() {
				BroadcastStream::new(sender.subscribe())
					.map_err(|err| err.wrap("Failed to create the stream."))
					.boxed()
			} else {
				stream::empty().boxed()
			};

			// Create the complete log stream.
			let log = old.chain(new).boxed();

			return Ok(Some(log));
		}

		// Attempt to get the log from the object.
		'a: {
			let build = tg::Build::with_id(id.clone());
			let Some(object) = build.try_get_object(self).await? else {
				break 'a;
			};
			let bytes = object.log.bytes(self).await?;
			return Ok(Some(stream::once(async move { Ok(bytes.into()) }).boxed()));
		}

		// Attempt to get the log from the remote.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			let Some(log) = remote.try_get_build_log(id).await? else {
				break 'a;
			};
			return Ok(Some(log));
		}

		Ok(None)
	}

	pub async fn add_build_log(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<()> {
		// Attempt to add the log to the state.
		'a: {
			// Get the state.
			let Some(state) = self.inner.build_state.read().unwrap().get(id).cloned() else {
				break 'a;
			};

			// Check if the build is stopped.
			if *state.inner.stop.receiver.borrow() {
				return_error!("The build is stopped.");
			}

			// Lock the log state.
			let mut state = state.inner.log.lock().await;

			// Get the log sender.
			let sender = state.sender.as_ref().cloned().unwrap();

			// Rewind the log.
			state
				.file
				.seek(std::io::SeekFrom::End(0))
				.await
				.wrap_err("Failed to seek.")?;

			// Write the log.
			state
				.file
				.write_all(&bytes)
				.await
				.wrap_err("Failed to write the log.")?;

			// Send the log.
			sender.send(bytes).ok();

			return Ok(());
		}

		// Attempt to add the log to the remote.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			remote.add_build_log(user, id, bytes).await?;
			return Ok(());
		}

		return_error!("Failed to find the build.");
	}

	pub async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::Outcome>> {
		// Attempt to await the outcome from the state.
		'a: {
			let Some(state) = self.inner.build_state.read().unwrap().get(id).cloned() else {
				break 'a;
			};
			return Ok(Some(
				state
					.inner
					.outcome
					.receiver
					.clone()
					.wait_for(Option::is_some)
					.await
					.unwrap()
					.clone()
					.unwrap(),
			));
		}

		// Attempt to get the outcome from the object.
		'a: {
			let build = tg::Build::with_id(id.clone());
			let Some(object) = build.try_get_object(self).await? else {
				break 'a;
			};
			return Ok(Some(object.outcome.clone()));
		}

		// Attempt to await the outcome from the remote.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			let Some(result) = remote.try_get_build_outcome(id).await? else {
				break 'a;
			};
			return Ok(Some(result));
		}

		Ok(None)
	}

	#[async_recursion]
	pub async fn cancel_build(
		&self,
		user: Option<&'async_recursion tg::User>,
		id: &tg::build::Id,
	) -> Result<()> {
		// Attempt to finish the build on the state.
		'a: {
			// Get the state.
			let Some(state) = self.inner.build_state.read().unwrap().get(id).cloned() else {
				break 'a;
			};

			// Mark the build as stopped.
			state.inner.stop.sender.send(true).unwrap();

			// Cancel the children.
			let children = state.inner.children.lock().unwrap().children.clone();
			children
				.iter()
				.map(|child| async move { self.cancel_build(user, child.id()).await })
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;

			// Finish the build as canceled.
			self.finish_build(user, id, tg::build::Outcome::Canceled)
				.await?;

			return Ok(());
		}

		// Attempt to cancel the build on the remote.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			remote.cancel_build(user, id).await?;
			return Ok(());
		}

		Ok(())
	}

	pub async fn finish_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		// Attempt to finish the build on the state.
		'a: {
			// Get the state.
			let Some(state) = self.inner.build_state.read().unwrap().get(id).cloned() else {
				break 'a;
			};

			// Get the target.
			let target = state.inner.target.clone();
			let target_id = target.id(self).await?.clone();

			// Get the children.
			let children = {
				let mut state = state.inner.children.lock().unwrap();
				state.sender.take();
				state.children.clone()
			};

			// Get the log.
			let log = {
				let mut state = state.inner.log.lock().await;
				state.sender.take();
				state.file.rewind().await.wrap_err("Failed to seek.")?;
				tg::Blob::with_reader(self, &mut state.file).await?
			};

			// Check if any of the children have been canceled.
			let outcome = if children
				.iter()
				.map(|child| child.outcome(self))
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?
				.into_iter()
				.any(|outcome| outcome.try_unwrap_canceled_ref().is_ok())
			{
				tg::build::Outcome::Canceled
			} else {
				outcome
			};

			// Create the build.
			tg::Build::new(self, id.clone(), target, children, log, outcome.clone()).await?;

			// Add the assignment to the database.
			self.inner.database.set_build_for_target(&target_id, id)?;

			// Set the outcome.
			state
				.inner
				.outcome
				.sender
				.send(Some(outcome.clone()))
				.unwrap();

			// Remove the build's state.
			self.inner.build_state.write().unwrap().remove(id);

			return Ok(());
		}

		// Attempt to finish the build on the remote.
		'a: {
			// Get the remote.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};

			// Push the outcome.
			if let tg::build::Outcome::Succeeded(value) = &outcome {
				if let Some(object) = value.object() {
					object.push(self, remote.as_ref()).await?;
				}
			}

			// Finish the build.
			remote.finish_build(user, id, outcome).await?;

			return Ok(());
		}

		Ok(())
	}
}
