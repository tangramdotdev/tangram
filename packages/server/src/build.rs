use super::Server;
use crate::{
	Build, ChildrenState, LocalBuildState, LocalBuildStateInner, LocalQueueTaskMessage, LogState,
	OutcomeState, RemoteBuildState, RemoteBuildStateInner,
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
use tg::Handle;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_stream::wrappers::BroadcastStream;

impl Server {
	pub(crate) async fn local_queue_task(
		&self,
		mut receiver: tokio::sync::mpsc::UnboundedReceiver<LocalQueueTaskMessage>,
	) -> Result<()> {
		loop {
			loop {
				// Get the highest priority item from the queue.
				let Some(item) = self.inner.local_queue.lock().unwrap().pop() else {
					break;
				};

				// If the build is at a unique depth, then start it.
				let unique = self
					.inner
					.local_builds
					.read()
					.unwrap()
					.values()
					.filter(|state| {
						*state.inner.status.lock().unwrap() == tg::build::Status::Running
					})
					.all(|state| state.inner.item.depth != item.depth);
				if unique {
					self.start_local_build(None, &item.build, item.depth, item.retry, None)
						.await?;
					continue;
				}

				// Otherwise, attempt to acquire a permit for it.
				match self.inner.semaphore.clone().try_acquire_owned() {
					// If a permit is available, then start the build.
					Ok(permit) => {
						self.start_local_build(
							None,
							&item.build,
							item.depth,
							item.retry,
							Some(permit),
						)
						.await?;
						continue;
					},

					// If there are no permits available, then add the item back to the queue and break.
					Err(tokio::sync::TryAcquireError::NoPermits) => {
						self.inner.local_queue.lock().unwrap().push(item);
						break;
					},

					Err(tokio::sync::TryAcquireError::Closed) => {
						unreachable!()
					},
				};
			}

			// Wait for a message on the channel.
			let message = receiver.recv().await.unwrap();
			match message {
				// If this is an added or finished message, then go back to the top.
				LocalQueueTaskMessage::Added | LocalQueueTaskMessage::Finished => {
					continue;
				},

				// If this is a stop message, then break.
				LocalQueueTaskMessage::Stop => return Ok(()),
			}
		}
	}

	pub(crate) async fn remote_queue_task(
		&self,
		mut receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
	) -> Result<()> {
		// Get the remote handle.
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(());
		};

		// Loop until the stop message is received.
		while receiver.try_recv().is_err() {
			// If the queue is not empty, then sleep and continue.
			let empty = self.inner.local_queue.lock().unwrap().is_empty();
			if !empty {
				tokio::time::sleep(std::time::Duration::from_secs(1)).await;
				continue;
			}

			// Get a permit.
			let permit = match self.inner.semaphore.clone().try_acquire_owned() {
				Ok(permit) => permit,
				Err(tokio::sync::TryAcquireError::NoPermits) => {
					tokio::time::sleep(std::time::Duration::from_secs(1)).await;
					continue;
				},
				Err(tokio::sync::TryAcquireError::Closed) => {
					unreachable!()
				},
			};

			// Attempt to get an item from the remote server's queue. If none is available, then sleep and continue.
			let Some(item) = remote.try_get_queue_item(None, None).await.ok().flatten() else {
				drop(permit);
				tokio::time::sleep(std::time::Duration::from_secs(1)).await;
				continue;
			};

			// Create the task state.
			let task = std::sync::Mutex::new(None);

			// Create the remote build state.
			let state = RemoteBuildState {
				inner: Arc::new(RemoteBuildStateInner { task }),
			};
			self.inner
				.remote_builds
				.write()
				.unwrap()
				.insert(item.build.clone(), state.clone());

			// Start the build.
			self.start_remote_build(None, &item.build, item.depth, item.retry, Some(permit));
		}

		Ok(())
	}

	/// Attempt to get the assignment.
	pub async fn try_get_assignment(
		&self,
		target_id: &tg::target::Id,
	) -> Result<Option<tg::build::Id>> {
		// Attempt to get the assignment from the database.
		'a: {
			let db = self.inner.database.pool.get().await;
			let mut statement = db
				.prepare_cached(
					"
						select build 
						from assignments 
						where target = ?1;
					",
				)
				.wrap_err("Failed to prepare the query.")?;
			let mut rows = statement
				.query([target_id.to_string()])
				.wrap_err("Failed to execute the query.")?;
			let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
				break 'a;
			};
			let id = row.get_unwrap::<_, String>(0).parse()?;
			return Ok(Some(id));
		}

		// Attempt to get the assignment from the remote server.
		'a: {
			// Get the remote handle.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};

			// Get the assignment from the remote server.
			let Some(build_id) = remote.try_get_assignment(target_id).await? else {
				break 'a;
			};

			// Add the assignment to the database.
			let db = self.inner.database.pool.get().await;
			let mut statement = db
				.prepare_cached(
					"
						insert into assignments (target, build)
						values (?1, ?2)
						on conflict (target) do update set build = ?2;
					",
				)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute([target_id.to_string(), build_id.to_string()])
				.wrap_err("Failed to execute the query.")?;

			return Ok(Some(build_id));
		}

		Ok(None)
	}

	/// Get or create a build.
	#[allow(clippy::too_many_lines)]
	pub async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		target_id: &tg::target::Id,
		depth: u64,
		retry: tg::build::Retry,
	) -> Result<tg::build::Id> {
		let target = tg::Target::with_id(target_id.clone());
		let host = target.host(self).await?.clone();

		// Return an existing build if one exists and it satisfies the retry constraint.
		'a: {
			let Some(build_id) = self.try_get_assignment(target_id).await? else {
				break 'a;
			};
			let build = tg::build::Build::with_id(build_id.clone());
			let status = build.status(self).await?;
			if status != tg::build::Status::Finished {
				break 'a;
			}
			let outcome = build.outcome(self).await?;
			let retry = retry >= outcome.retry();
			if !retry {
				return Ok(build_id);
			}
		}

		// Decide whether to attempt to escalate the build.
		let escalate = true;

		// Attempt to escalate the build.
		if escalate {
			if let Some(remote) = self.inner.remote.as_ref() {
				let object = tg::object::Handle::with_id(target_id.clone().into());
				let result = object.push(self, remote.as_ref()).await;
				if result.is_ok() {
					if let Ok(build_id) = remote
						.get_or_create_build(user, target_id, depth, retry)
						.await
					{
						return Ok(build_id);
					}
				}
			}
		}

		// Otherwise, create a new build.
		let build_id = tg::build::Id::new();

		// Create the children state.
		let children = std::sync::Mutex::new(ChildrenState {
			children: Vec::new(),
			sender: Some(tokio::sync::broadcast::channel(1024).0),
		});

		// Create the item.
		let item = tg::build::queue::Item {
			build: build_id.clone(),
			host,
			depth,
			retry,
		};

		// Create the log state.
		let log = tokio::sync::Mutex::new(LogState {
			file: tokio::fs::File::from_std(
				tempfile::tempfile().wrap_err("Failed to create the temporary file.")?,
			),
			sender: Some(tokio::sync::broadcast::channel(1024).0),
		});

		// Create the outcome state.
		let (sender, _) = tokio::sync::watch::channel(None);
		let outcome = OutcomeState { sender };

		// Create the status state.
		let status = std::sync::Mutex::new(tg::build::Status::Queued);

		// Create the task state.
		let task = std::sync::Mutex::new(None);

		// Create the build state.
		let state = LocalBuildState {
			inner: Arc::new(LocalBuildStateInner {
				children,
				item: item.clone(),
				log,
				outcome,
				status,
				task,
			}),
		};
		self.inner
			.local_builds
			.write()
			.unwrap()
			.insert(build_id.clone(), state.clone());

		// Add the build to the database.
		{
			let build = Build {
				status: tg::build::Status::Queued,
				target: target_id.clone(),
				children: Vec::new(),
				log: None,
				outcome: None,
			};
			let json =
				serde_json::to_string(&build).wrap_err("Failed to serialize the build data.")?;
			let db = self.inner.database.pool.get().await;
			let mut statement = db
				.prepare_cached(
					"
						insert into builds (id, json)
						values (?1, ?2);
					",
				)
				.wrap_err("Failed to prepare the query.")?;
			let params = rusqlite::params![build_id.to_string(), json];
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Add the assignment to the database.
		{
			let mut db = self.inner.database.pool.get().await;
			let txn = db
				.transaction()
				.wrap_err("Failed to begin the transaction.")?;
			let mut statement = txn
				.prepare_cached(
					"
						insert into assignments (target, build)
						values (?1, ?2)
						on conflict (target) do update set build = ?2;
					",
				)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute([target_id.to_string(), build_id.to_string()])
				.wrap_err("Failed to execute the query.")?;
			drop(statement);
			txn.commit().wrap_err("Failed to commit the transaction.")?;
		}

		// Add the build to the queue.
		self.inner.local_queue.lock().unwrap().push(item);

		// Send a message to the build queue task that the item has been added.
		self.inner
			.local_queue_task_sender
			.send(LocalQueueTaskMessage::Added)
			.wrap_err("Failed to send the message to the local queue task.")?;

		Ok(build_id)
	}

	async fn start_local_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		depth: u64,
		retry: tg::build::Retry,
		permit: Option<tokio::sync::OwnedSemaphorePermit>,
	) -> Result<()> {
		// Spawn the task.
		let task = tokio::spawn({
			let server = self.clone();
			let user = user.cloned();
			let id = id.clone();
			async move {
				if let Err(error) = server
					.build_task(user.as_ref(), &id, depth, retry, permit)
					.await
				{
					tracing::error!(?error, "The build failed.");
				}
			}
		});

		// Update the state.
		{
			let mut state = self.inner.local_builds.write().unwrap();
			let state = state.get_mut(id).unwrap();
			*state.inner.status.lock().unwrap() = tg::build::Status::Running;
			*state.inner.task.lock().unwrap() = Some(task);
		}

		// Update the database.
		{
			let db = self.inner.database.pool.get().await;
			let mut statement = db
				.prepare_cached(
					"
						update builds
						set json = json_set(json, '$.status', 'running')
						where id = ?1;
					",
				)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute([id.to_string()])
				.wrap_err("Failed to execute the query.")?;
		}

		Ok(())
	}

	fn start_remote_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		depth: u64,
		retry: tg::build::Retry,
		permit: Option<tokio::sync::OwnedSemaphorePermit>,
	) {
		// Spawn the task.
		let task = tokio::spawn({
			let server = self.clone();
			let user = user.cloned();
			let id = id.clone();
			async move {
				if let Err(error) = server
					.build_task(user.as_ref(), &id, depth, retry, permit)
					.await
				{
					tracing::error!(?error, "The build failed.");
				}
			}
		});

		// Update the state.
		{
			let mut state = self.inner.remote_builds.write().unwrap();
			let state = state.get_mut(id).unwrap();
			*state.inner.task.lock().unwrap() = Some(task);
		}
	}

	async fn build_task(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		depth: u64,
		retry: tg::build::Retry,
		permit: Option<tokio::sync::OwnedSemaphorePermit>,
	) -> Result<()> {
		let build = tg::Build::with_id(id.clone());
		let target = build.target(self).await?;

		// Build the target with the appropriate runtime.
		let result = match target.host(self).await?.os() {
			tg::system::Os::Js => {
				// Build the target on the server's local task pool because it is a `!Send` future.
				self.inner
					.local_task_pool_handle
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
					tangram_runtime::darwin::build(self, &build, retry, self.path()).await
				}
				#[cfg(not(target_os = "macos"))]
				{
					return_error!("Cannot build a darwin target on this host.");
				}
			},
			tg::system::Os::Linux => {
				#[cfg(target_os = "linux")]
				{
					tangram_runtime::linux::build(self, &build, retry, self.path()).await
				}
				#[cfg(not(target_os = "linux"))]
				{
					return_error!("Cannot build a linux target on this host.");
				}
			},
		};

		// If an error occurred, add the error to the build's log.
		if let Err(error) = result.as_ref() {
			build
				.add_log(self, error.trace().to_string().into())
				.await?;
		}

		// Create the outcome.
		let outcome = match result {
			Ok(value) => tg::build::Outcome::Succeeded(value),
			Err(error) => tg::build::Outcome::Failed(error),
		};

		// Finish the build.
		build.finish(self, user, outcome).await?;

		// Drop the permit.
		drop(permit);

		// Send a message to the build queue task that the build has finished.
		self.inner
			.local_queue_task_sender
			.send(LocalQueueTaskMessage::Finished)
			.wrap_err("Failed to send the message to the local queue task.")?;

		Ok(())
	}

	pub async fn try_get_queue_item(
		&self,
		user: Option<&tg::User>,
		hosts: Option<Vec<tg::System>>,
	) -> Result<Option<tg::build::queue::Item>> {
		// Attempt to get a build from the remote server's queue.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			let Some(item) = remote.try_get_queue_item(user, hosts).await? else {
				break 'a;
			};
			return Ok(Some(item));
		}

		Ok(None)
	}

	pub async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::Status>> {
		// Attempt to get the status from a local build.
		let state = self.inner.local_builds.read().unwrap().get(id).cloned();
		if let Some(state) = state {
			return Ok(Some(*state.inner.status.lock().unwrap()));
		}

		// Attempt to get the status from the database.
		'a: {
			let db = self.inner.database.pool.get().await;
			let mut statement = db
				.prepare_cached(
					"
						select json_extract(json, '$.status')
						from builds
						where id = ?1;
					",
				)
				.wrap_err("Failed to prepare the query.")?;
			let mut rows = statement
				.query([id.to_string()])
				.wrap_err("Failed to execute the query.")?;
			let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
				break 'a;
			};
			let status = row.get_unwrap::<_, String>(0).parse()?;
			return Ok(Some(status));
		}

		// Attempt to get the status from the remote server.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			let Some(status) = remote.try_get_build_status(id).await? else {
				break 'a;
			};
			return Ok(Some(status));
		}

		Ok(None)
	}

	pub async fn try_get_build_target(&self, id: &tg::build::Id) -> Result<Option<tg::target::Id>> {
		// Attempt to get the target from the database.
		'a: {
			let db = self.inner.database.pool.get().await;
			let mut statement = db
				.prepare_cached(
					"
						select json_extract(json, '$.target')
						from builds
						where id = ?1;
					",
				)
				.wrap_err("Failed to prepare the query.")?;
			let mut rows = statement
				.query([id.to_string()])
				.wrap_err("Failed to execute the query.")?;
			let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
				break 'a;
			};
			let target_id = row.get_unwrap::<_, String>(0).parse()?;
			return Ok(Some(target_id));
		}

		// Attempt to get the target from the remote server.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			let Some(target_id) = remote.try_get_build_target(id).await? else {
				break 'a;
			};
			return Ok(Some(target_id));
		}

		Ok(None)
	}

	pub async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Id>>>> {
		// Attempt to get the children from a local build.
		'a: {
			// Get the state.
			let Some(state) = self.inner.local_builds.read().unwrap().get(id).cloned() else {
				break 'a;
			};

			// Get the children state.
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
			let children = old.chain(new).map_ok(|id| id.clone()).boxed();

			return Ok(Some(children));
		}

		// Attempt to get the children from the database.
		'a: {
			let db = self.inner.database.pool.get().await;
			let mut statement = db
				.prepare_cached(
					"
						select json_extract(json, '$.children')
						from builds
						where id = ?1;
					",
				)
				.wrap_err("Failed to prepare the query.")?;
			let mut rows = statement
				.query([id.to_string()])
				.wrap_err("Failed to execute the query.")?;
			let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
				break 'a;
			};
			let children = row.get_unwrap::<_, String>(0);
			let children: Vec<tg::build::Id> =
				serde_json::from_str(&children).wrap_err("Failed to deserialize the children.")?;
			let children = stream::iter(children).map(Ok).boxed();
			return Ok(Some(children));
		}

		// Attempt to get the children from the remote server.
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
		// Attempt to add the child to a local build.
		'a: {
			// Get the state.
			let Some(state) = self
				.inner
				.local_builds
				.read()
				.unwrap()
				.get(build_id)
				.cloned()
			else {
				break 'a;
			};

			// Add the child to the state.
			{
				let mut state = state.inner.children.lock().unwrap();
				if let Some(sender) = state.sender.as_ref().cloned() {
					state.children.push(child_id.clone());
					sender.send(child_id.clone()).ok();
				}
			}

			// Add the child to the build in the database.
			{
				let db = self.inner.database.pool.get().await;
				let mut statement = db
					.prepare_cached(
						"
							update builds
							set json = json_set(json, '$.children[#]', ?1)
							where id = ?2;
						",
					)
					.wrap_err("Failed to prepare the query.")?;
				statement
					.execute([child_id.to_string(), build_id.to_string()])
					.wrap_err("Failed to execute the query.")?;
			}

			return Ok(());
		}

		// Attempt to add the child to a remote build.
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
		// Attempt to get the log from a local build.
		'a: {
			// Get the state.
			let Some(state) = self.inner.local_builds.read().unwrap().get(id).cloned() else {
				break 'a;
			};

			// Get the log state.
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

			// Create the log stream.
			let log = old.chain(new).boxed();

			return Ok(Some(log));
		}

		// Attempt to get the log from the database.
		'a: {
			let log = {
				let db = self.inner.database.pool.get().await;
				let mut statement = db
					.prepare_cached(
						"
							select json_extract(json, '$.log')
							from builds
							where id = ?1;
						",
					)
					.wrap_err("Failed to prepare the query.")?;
				let mut rows = statement
					.query([id.to_string()])
					.wrap_err("Failed to execute the query.")?;
				let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
					break 'a;
				};
				row.get_unwrap::<_, String>(0).parse()?
			};
			let bytes = tg::Blob::with_id(log).bytes(self).await?;
			return Ok(Some(stream::once(async move { Ok(bytes.into()) }).boxed()));
		}

		// Attempt to get the log from the remote server.
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
		// Attempt to add the log to a local build.
		'a: {
			// Get the state.
			let Some(state) = self.inner.local_builds.read().unwrap().get(id).cloned() else {
				break 'a;
			};

			// Get the log state.
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

		// Attempt to add the log to a remote build.
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
		// Attempt to await the outcome from a local build.
		'a: {
			// Get the state.
			let Some(state) = self.inner.local_builds.read().unwrap().get(id).cloned() else {
				break 'a;
			};

			return Ok(Some(
				state
					.inner
					.outcome
					.sender
					.subscribe()
					.wait_for(Option::is_some)
					.await
					.unwrap()
					.clone()
					.unwrap(),
			));
		}

		// Attempt to get the outcome from the database.
		'a: {
			let db = self.inner.database.pool.get().await;
			let mut statement = db
				.prepare_cached(
					"
						select json_extract(json, '$.outcome')
						from builds
						where id = ?1;
					",
				)
				.wrap_err("Failed to prepare the query.")?;
			let mut rows = statement
				.query([id.to_string()])
				.wrap_err("Failed to execute the query.")?;
			let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
				break 'a;
			};
			let outcome = row.get_unwrap::<_, String>(0);
			let outcome: tg::build::outcome::Data =
				serde_json::from_str(&outcome).wrap_err("Failed to deserialize the children.")?;
			let outcome = tg::build::Outcome::try_from(outcome)?;
			return Ok(Some(outcome));
		}

		// Attempt to await the outcome from the remote server.
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
		// Attempt to cancel a local build.
		'a: {
			// Get the state.
			let Some(state) = self.inner.local_builds.read().unwrap().get(id).cloned() else {
				break 'a;
			};

			// Cancel the children.
			let children = state.inner.children.lock().unwrap().children.clone();
			children
				.iter()
				.map(|child_id| async move { self.cancel_build(user, child_id).await })
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;

			// Finish the build as canceled.
			self.finish_build(user, id, tg::build::Outcome::Canceled)
				.await?;

			return Ok(());
		}

		// Attempt to cancel a remote build.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			remote.cancel_build(user, id).await?;
			return Ok(());
		}

		return_error!("Failed to find the build.");
	}

	pub async fn finish_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		// Attempt to finish a local build.
		'a: {
			// Get the state.
			let Some(state) = self.inner.local_builds.read().unwrap().get(id).cloned() else {
				break 'a;
			};

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
				.map(|child_id| self.get_build_outcome(child_id))
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

			// Update the database.
			{
				let status = tg::build::Status::Finished;
				let log = log.id(self).await?.clone();
				let outcome = serde_json::to_string(&outcome.data(self).await?)
					.wrap_err("Failed to serialize the outcome.")?;
				let db = self.inner.database.pool.get().await;
				let mut statement = db
					.prepare_cached(
						"
							update builds
							set json = json_set(
								json,
								'$.status', ?1,
								'$.log', ?2,
								'$.outcome', json(?3)
							)
							where id = ?4;
						",
					)
					.wrap_err("Failed to prepare the query.")?;
				let params =
					rusqlite::params![status.to_string(), log.to_string(), outcome, id.to_string()];
				statement
					.execute(params)
					.wrap_err("Failed to execute the query.")?;
			}

			// Send the outcome.
			state
				.inner
				.outcome
				.sender
				.send_replace(Some(outcome.clone()));

			// Remove the build's state.
			self.inner.local_builds.write().unwrap().remove(id);

			return Ok(());
		}

		// Attempt to finish a remote build.
		'a: {
			// Get the remote handle.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};

			// Push the outcome.
			if let tg::build::Outcome::Succeeded(value) = &outcome {
				value.push(self, remote.as_ref()).await?;
			}

			// Finish the build.
			remote.finish_build(user, id, outcome).await?;

			// Remove the build's state.
			self.inner.remote_builds.write().unwrap().remove(id);

			return Ok(());
		}

		return_error!("Failed to find the build.");
	}
}
