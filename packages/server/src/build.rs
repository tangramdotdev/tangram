use super::Server;
use crate::{Build, Channels, LocalQueueTaskMessage};
use async_recursion::async_recursion;
use bytes::Bytes;
use futures::{
	stream::{self, BoxStream, FuturesUnordered},
	StreamExt, TryStreamExt,
};
use num::ToPrimitive;
use rusqlite::params;
use std::sync::Arc;
use tangram_client as tg;
use tangram_error::{return_error, Error, Result, WrapErr};
use tg::Handle;
use tokio_stream::wrappers::WatchStream;

impl Server {
	pub(crate) async fn local_queue_task(
		&self,
		mut receiver: tokio::sync::mpsc::UnboundedReceiver<LocalQueueTaskMessage>,
	) -> Result<()> {
		loop {
			loop {
				// Get the highest priority item from the queue.
				let item: tg::build::queue::Item = {
					let db = self.inner.database.pool.get().await;
					let statement = "
						delete from queue
						where rowid in (
							select rowid
							from queue
							order by json->'depth' desc
							limit 1
						)
						returning json;
					";
					let mut statement = db
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the query.")?;
					let mut rows = statement
						.query([])
						.wrap_err("Failed to execute the query.")?;
					let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
						break;
					};
					let json = row.get_unwrap::<_, String>(0);
					serde_json::from_str(&json).wrap_err("Failed to deserialize the item.")?
				};

				// If the build is at a unique depth, then start it.
				let unique = {
					let db = self.inner.database.pool.get().await;
					let statement = "
						select ?1 not in (
							select distinct json->'depth'
							from builds
							where json->'status' = 'running'
						);
					";
					let mut statement = db
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the query.")?;
					let params = params![item.depth];
					let mut rows = statement
						.query(params)
						.wrap_err("Failed to execute the query.")?;
					let row = rows
						.next()
						.wrap_err("Failed to get the row.")?
						.wrap_err("Expected one row.")?;
					row.get::<_, bool>(0).wrap_err("Expected a bool.")?
				};
				if unique {
					self.start_build(None, &item.build, item.depth, item.retry, None)
						.await?;
					continue;
				}

				// Otherwise, attempt to acquire a permit for it.
				match self.inner.semaphore.clone().try_acquire_owned() {
					// If a permit is available, then start the build.
					Ok(permit) => {
						self.start_build(None, &item.build, item.depth, item.retry, Some(permit))
							.await?;
						continue;
					},

					// If there are no permits available, then add the item back to the queue and break.
					Err(tokio::sync::TryAcquireError::NoPermits) => {
						let json = serde_json::to_string(&item)
							.wrap_err("Failed to serialize the item.")?;
						let db = self.inner.database.pool.get().await;
						let statement = "
							insert into queue (json)
							values (?1);
						";
						let mut statement = db
							.prepare_cached(statement)
							.wrap_err("Failed to prepare the query.")?;
						let params = params![json];
						statement
							.execute(params)
							.wrap_err("Failed to execute the query.")?;
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
			let empty = {
				let db = self.inner.database.pool.get().await;
				let statement = "
					select count(*) = 0
					from queue;
				";
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let mut rows = statement
					.query([])
					.wrap_err("Failed to execute the query.")?;
				let row = rows
					.next()
					.wrap_err("Failed to get the row.")?
					.wrap_err("Expected one row.")?;
				row.get::<_, bool>(0).wrap_err("Expected a bool.")?
			};
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

			// Start the build.
			self.start_build(None, &item.build, item.depth, item.retry, Some(permit))
				.await?;
		}

		Ok(())
	}

	pub async fn try_get_assignment(
		&self,
		target_id: &tg::target::Id,
	) -> Result<Option<tg::build::Id>> {
		// Attempt to get the assignment from the database.
		'a: {
			let db = self.inner.database.pool.get().await;
			let statement = "
				select build 
				from assignments 
				where target = ?1;
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let params = params![target_id.to_string()];
			let mut rows = statement
				.query(params)
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
			let statement = "
				insert into assignments (target, build)
				values (?1, ?2)
				on conflict (target) do update set build = ?2;
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let params = params![target_id.to_string(), build_id.to_string()];
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;

			return Ok(Some(build_id));
		}

		Ok(None)
	}

	async fn build_is_local(&self, id: &tg::build::Id) -> Result<bool> {
		let db = self.inner.database.pool.get().await;
		let statement = "
					select count(*) != 0
					from builds
					where id = ?1;
				";
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let params = params![id.to_string()];
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let row = rows
			.next()
			.wrap_err("Failed to get the row.")?
			.wrap_err("Expected one row.")?;
		let exists = row.get::<_, bool>(0).wrap_err("Expected a bool.")?;
		Ok(exists)
	}

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

		// Create the channels.
		let (children, _) = tokio::sync::watch::channel(());
		let (log, _) = tokio::sync::watch::channel(());
		let (outcome, _) = tokio::sync::watch::channel(());
		let channels = Arc::new(Channels {
			children,
			log,
			outcome,
		});
		self.inner
			.channels
			.write()
			.unwrap()
			.insert(build_id.clone(), channels);

		// Add the build to the database.
		{
			let build = Build {
				children: Vec::new(),
				depth,
				outcome: None,
				status: tg::build::Status::Queued,
				target: target_id.clone(),
			};
			let json =
				serde_json::to_string(&build).wrap_err("Failed to serialize the build data.")?;
			let db = self.inner.database.pool.get().await;
			let statement = "
				insert into builds (id, json)
				values (?1, ?2);
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let params = params![build_id.to_string(), json];
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Add the assignment to the database.
		{
			let db = self.inner.database.pool.get().await;
			let statement = "
				insert into assignments (target, build)
				values (?1, ?2)
				on conflict (target) do update set build = ?2;
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let params = params![target_id.to_string(), build_id.to_string()];
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Create the item.
		let item = tg::build::queue::Item {
			build: build_id.clone(),
			host,
			depth,
			retry,
		};

		// Add the item to the queue.
		{
			let json = serde_json::to_string(&item).wrap_err("Failed to serialize the item.")?;
			let db = self.inner.database.pool.get().await;
			let statement = "
				insert into queue (json)
				values (?1);
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let params = params![json];
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Send a message to the build queue task that the item has been added.
		self.inner
			.local_queue_task_sender
			.send(LocalQueueTaskMessage::Added)
			.wrap_err("Failed to send the message to the local queue task.")?;

		Ok(build_id)
	}

	async fn start_build(
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
					let trace = error.trace();
					tracing::error!(%trace, "The build failed.");
				}
			}
		});

		// Add the task.
		self.inner.tasks.write().unwrap().insert(id.clone(), task);

		// Update the status.
		self.set_build_status(user, id, tg::build::Status::Running)
			.await?;

		Ok(())
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
		// Attempt to get the status from the database.
		'a: {
			let db = self.inner.database.pool.get().await;
			let statement = "
				select json_extract(json, '$.status')
				from builds
				where id = ?1;
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let params = params![id.to_string()];
			let mut rows = statement
				.query(params)
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

	pub async fn set_build_status(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<()> {
		// Attempt to set the status of a local build.
		'a: {
			let db = self.inner.database.pool.get().await;
			let statement = "
				update builds
				set json = json_set(json, '$.status', 'running')
				where id = ?1;
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let params = params![id.to_string()];
			let n = statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
			if n == 0 {
				break 'a;
			}
			return Ok(());
		}

		// Attempt to set the status of a remote build.
		'a: {
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};
			remote.set_build_status(user, id, status).await?;
			return Ok(());
		}

		return_error!("Failed to find the build.");
	}

	pub async fn try_get_build_target(&self, id: &tg::build::Id) -> Result<Option<tg::target::Id>> {
		// Attempt to get the target from the database.
		'a: {
			let db = self.inner.database.pool.get().await;
			let statement = "
				select json_extract(json, '$.target')
				from builds
				where id = ?1;
			";
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let params = params![id.to_string()];
			let mut rows = statement
				.query(params)
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

	#[allow(clippy::too_many_lines)]
	pub async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Id>>>> {
		// Attempt to get the children from a local build.
		'a: {
			// Verify the build is local.
			if !self.build_is_local(id).await? {
				break 'a;
			}

			// Create the children stream.
			struct State {
				server: Server,
				id: tg::build::Id,
				index: usize,
				first: bool,
				last: bool,
				channel: Option<BoxStream<'static, ()>>,
			}
			let channels = self.inner.channels.read().unwrap().get(id).cloned();
			let channel = channels.as_ref().map(|channels| {
				let children = WatchStream::new(channels.children.subscribe());
				let outcome = WatchStream::new(channels.outcome.subscribe());
				stream::select(children, outcome).boxed()
			});
			let state = State {
				server: self.clone(),
				id: id.clone(),
				index: 0,
				first: true,
				last: false,
				channel,
			};
			let children = stream::try_unfold(state, |mut state| async move {
				if state.first {
					state.first = false;
				} else if !state.last {
					let Some(channel) = state.channel.as_mut() else {
						return_error!("No channel.");
					};
					channel.next().await;
				} else {
					return Ok(None);
				}
				let (status, children) = {
					let db = state.server.inner.database.pool.get().await;
					let statement = "
						select
							json->>'status' as status,
							(
								select coalesce(json_group_array(value), '[]')
								from (
									select value
									from json_each(builds.json->'children')
									limit -1
									offset ?1
								)
							) as children
						from builds
						where id = ?2;
					";
					let mut statement = db
						.prepare_cached(statement)
						.wrap_err("Failed to prepare the statement.")?;
					let params = params![state.index.to_i64().unwrap(), state.id.to_string()];
					let mut rows = statement
						.query(params)
						.wrap_err("Failed to execute the statement.")?;
					let row = rows
						.next()
						.wrap_err("Failed to get the row.")?
						.wrap_err("Expected a row.")?;
					let status: tg::build::Status = row
						.get::<_, String>(0)
						.unwrap()
						.parse()
						.wrap_err("Invalid status.")?;
					let children = row.get::<_, String>(1).unwrap();
					let children: Vec<tg::build::Id> = serde_json::from_str(&children)
						.wrap_err("Failed to deseriaize the children.")?;
					(status, children)
				};
				if matches!(status, tg::build::Status::Finished) {
					state.last = true;
				}
				let len = children.len();
				let children = stream::iter(children).map(Ok);
				state.index += len;
				Ok::<_, Error>(Some((children, state)))
			})
			.try_flatten();

			return Ok(Some(children.boxed()));
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
			// Verify the build is local.
			if !self.build_is_local(build_id).await? {
				break 'a;
			}

			// Add the child to the build in the database.
			{
				let db = self.inner.database.pool.get().await;
				let statement = "
					update builds
					set json = json_set(json, '$.children[#]', ?1)
					where id = ?2;
				";
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				statement
					.execute([child_id.to_string(), build_id.to_string()])
					.wrap_err("Failed to execute the query.")?;
			}

			// Notify subscribers that a child has been added.
			if let Some(channels) = self.inner.channels.read().unwrap().get(build_id).cloned() {
				channels.children.send_replace(());
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
			// Verify the build is local.
			if !self.build_is_local(id).await? {
				break 'a;
			}

			return Ok(Some(stream::empty().boxed()));
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
			// Verify the build is local.
			if !self.build_is_local(id).await? {
				break 'a;
			}

			// Add the log to the database.
			{
				let db = self.inner.database.pool.get().await;
				let statement = "
					insert into logs (build, position, bytes)
					values (
						?1,
						(
							select coalesce(
								(
									select position + length(bytes)
									from logs
									where build = ?1
									order by position desc
									limit 1
								), 
								0
							)
						),
						?2
					);
				";
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let params = params![id.to_string(), bytes.to_vec()];
				statement
					.execute(params)
					.wrap_err("Failed to execute the query.")?;
			}

			// Notify subscribers that the log has been added to.
			if let Some(channels) = self.inner.channels.read().unwrap().get(id).cloned() {
				channels.log.send_replace(());
			}

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
		// Attempt to get the outcome from a local build.
		'a: {
			// Verify the build is local.
			if !self.build_is_local(id).await? {
				break 'a;
			}

			// Get the outcome.
			let channels = self.inner.channels.read().unwrap().get(id).cloned();
			let mut subscriber = channels
				.as_ref()
				.map(|channels| channels.outcome.subscribe());
			let mut first = true;
			loop {
				if !first {
					let Some(subscriber) = subscriber.as_mut() else {
						break 'a;
					};
					subscriber.changed().await.unwrap();
				}
				first = false;
				let db = self.inner.database.pool.get().await;
				let statement = "
					select coalesce(json_extract(json, '$.outcome'), json('null'))
					from builds
					where id = ?1;
				";
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let params = params![id.to_string()];
				let mut rows = statement
					.query(params)
					.wrap_err("Failed to execute the query.")?;
				let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
					break 'a;
				};
				let outcome = row.get_unwrap::<_, String>(0);
				let outcome: Option<tg::build::outcome::Data> = serde_json::from_str(&outcome)
					.wrap_err("Failed to deserialize the children.")?;
				let outcome: Option<tg::build::Outcome> =
					outcome.map(TryInto::try_into).transpose()?;
				if let Some(outcome) = outcome {
					return Ok(Some(outcome));
				}
			}
		}

		// Attempt to get the outcome from the remote server.
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

	#[allow(clippy::too_many_lines)]
	#[async_recursion]
	pub async fn finish_build(
		&self,
		user: Option<&'async_recursion tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		// Attempt to finish a local build.
		'a: {
			// Verify the build is local.
			{
				let db = self.inner.database.pool.get().await;
				let statement = "
					select count(*) != 0
					from builds
					where id = ?1;
				";
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let params = params![id.to_string()];
				let mut rows = statement
					.query(params)
					.wrap_err("Failed to execute the query.")?;
				let row = rows
					.next()
					.wrap_err("Failed to get the row.")?
					.wrap_err("Expected one row.")?;
				let exists = row.get::<_, bool>(0).wrap_err("Expected a bool.")?;
				if !exists {
					break 'a;
				}
			}

			// Get the children.
			let children: Vec<tg::build::Id> = {
				let db = self.inner.database.pool.get().await;
				let statement = "
					select json_extract(json, '$.children')
					from builds
					where id = ?1;
				";
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let params = params![id.to_string()];
				let mut rows = statement
					.query(params)
					.wrap_err("Failed to execute the query.")?;
				let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
					break 'a;
				};
				let children = row.get::<_, String>(0).unwrap();
				serde_json::from_str(&children).wrap_err("Failed to deserialize the children.")?
			};

			// If the outcome is canceled, then cancel the children.
			if matches!(outcome, tg::build::Outcome::Canceled) {
				children
					.iter()
					.map(|child| async move {
						self.finish_build(user, child, tg::build::Outcome::Canceled)
							.await?;
						Ok::<_, Error>(())
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?;
			}

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
				let outcome = serde_json::to_string(&outcome.data(self).await?)
					.wrap_err("Failed to serialize the outcome.")?;
				let db = self.inner.database.pool.get().await;
				let statement = "
					update builds
					set json = json_set(
						json,
						'$.status', ?1,
						'$.outcome', json(?2)
					)
					where id = ?3;
				";
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let params = params![status.to_string(), outcome, id.to_string()];
				statement
					.execute(params)
					.wrap_err("Failed to execute the query.")?;
			}

			// Notify subscribers that the outcome has been set.
			if let Some(channels) = self.inner.channels.read().unwrap().get(id).cloned() {
				channels.outcome.send_replace(());
			}

			// Remove the build's channels.
			self.inner.channels.write().unwrap().remove(id);

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

			// Remove the build's task.
			self.inner.tasks.write().unwrap().remove(id);

			return Ok(());
		}

		return_error!("Failed to find the build.");
	}
}
