use super::Server;
use crate::{database::Json, params, BuildContext};
use async_recursion::async_recursion;
use bytes::Bytes;
use futures::{
	future,
	stream::{self, BoxStream, FuturesUnordered},
	FutureExt, StreamExt, TryStreamExt,
};
use itertools::Itertools;
use num::ToPrimitive;
use std::sync::Arc;
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};
use tg::Handle;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_stream::wrappers::WatchStream;
use tokio_util::either::Either;

impl Server {
	pub async fn try_list_builds(&self, arg: tg::build::ListArg) -> Result<tg::build::ListOutput> {
		let db = self.inner.database.get().await?;
		let order = match arg.sort {
			tg::build::ListSort::Timestamp => "state->>'timestamp' desc",
		};
		let statement = &format!(
			"
				select state
				from builds
				where state->>'target' = ?1
				order by {order}
				limit ?2
				offset ?3;
			"
		);
		let params = params![arg.target.to_string(), arg.limit, 0];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let values = rows
			.and_then(|row| row.get::<_, Json<tg::build::State>>(0).map(|json| json.0))
			.try_collect()
			.wrap_err("Failed to deserialize the rows.")?;
		let output = tg::build::ListOutput { values };
		Ok(output)
	}

	pub async fn get_build_exists(&self, id: &tg::build::Id) -> Result<bool> {
		if self.get_build_exists_local(id).await? {
			return Ok(true);
		}
		if self.get_build_exists_remote(id).await? {
			return Ok(true);
		}
		Ok(false)
	}

	async fn get_build_exists_local(&self, id: &tg::build::Id) -> Result<bool> {
		let db = self.inner.database.get().await?;
		let statement = "
			select count(*) != 0
			from builds
			where id = ?1;
		";
		let params = params![id.to_string()];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let row = rows
			.next()
			.wrap_err("Failed to retrieve the row.")?
			.wrap_err("Expected a row.")?;
		let exists = row
			.get::<_, bool>(0)
			.wrap_err("Failed to deserialize the column.")?;
		Ok(exists)
	}

	async fn get_build_exists_remote(&self, id: &tg::build::Id) -> Result<bool> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};
		let exists = remote.get_build_exists(id).await?;
		Ok(exists)
	}

	pub async fn try_get_build(&self, id: &tg::build::Id) -> Result<Option<tg::build::GetOutput>> {
		if let Some(output) = self.try_get_build_local(id).await? {
			Ok(Some(output))
		} else if let Some(output) = self.try_get_build_remote(id).await? {
			Ok(Some(output))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_local(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::GetOutput>> {
		let db = self.inner.database.get().await?;
		let statement = "
			select state
			from builds
			where id = ?1;
		";
		let params = params![id.to_string()];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let Some(row) = rows.next().wrap_err("Failed to retrieve the row.")? else {
			return Ok(None);
		};
		let state = row
			.get::<_, Json<tg::build::State>>(0)
			.wrap_err("Failed to deserialize the column.")?
			.0;
		let output = tg::build::GetOutput { state };
		Ok(Some(output))
	}

	async fn try_get_build_remote(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::GetOutput>> {
		// Get the remote.
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};

		// Get the build from the remote server.
		let Some(output) = remote.try_get_build(id).await? else {
			return Ok(None);
		};

		// Add the build to the database.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into builds (id, state)
				values (?1, ?2)
				on conflict (id) do update set state = ?2;
			";
			let params = params![id.to_string(), Json(output.state.clone())];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		Ok(Some(output))
	}

	pub async fn try_put_build(
		&self,
		_user: Option<&tg::User>,
		id: &tg::build::Id,
		state: &tg::build::State,
	) -> Result<tg::build::PutOutput> {
		// Verify the build is finished.
		if state.status != tg::build::Status::Finished {
			return Err(error!("The build is not finished."));
		}

		// Get the missing builds.
		let builds = stream::iter(state.children.clone())
			.map(Ok)
			.try_filter_map(|id| async move {
				let exists = self.get_build_exists(&id).await?;
				Ok::<_, Error>(if exists { None } else { Some(id) })
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Get the missing objects.
		let objects = stream::iter(state.objects())
			.map(Ok)
			.try_filter_map(|id| async move {
				let exists = self.get_object_exists(&id).await?;
				Ok::<_, Error>(if exists { None } else { Some(id) })
			})
			.try_collect::<Vec<_>>()
			.await?;

		// Insert the build if there are no missing builds or objects.
		if builds.is_empty() && objects.is_empty() {
			let db = self.inner.database.get().await?;
			let statement = "
				insert into builds (id, state)
				values (?1, ?2)
				on conflict do update set state = ?2;
			";
			let params = params![id.to_string(), Json(state.clone())];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		let output = tg::build::PutOutput {
			missing: tg::build::Missing { builds, objects },
		};

		Ok(output)
	}

	#[allow(clippy::too_many_lines)]
	pub async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput> {
		// Get a local build if one exists that satisfies the retry constraint.
		let build = 'a: {
			// Find a build.
			let list_arg = tg::build::ListArg {
				limit: 1,
				sort: tg::build::ListSort::Timestamp,
				target: arg.target.clone(),
			};
			let Some(build) = self
				.try_list_builds(list_arg)
				.await?
				.values
				.first()
				.cloned()
				.map(|state| tg::Build::with_id(state.id))
			else {
				break 'a None;
			};

			// Verify the build satisfies the retry constraint.
			let status = build.status(self).await?;
			if status == tg::build::Status::Finished {
				let outcome = build.outcome(self).await?;
				if arg.options.retry >= outcome.retry() {
					break 'a None;
				}
			}

			Some(build)
		};

		// Get a remote build if one exists that satisfies the retry constraint.
		let build = 'a: {
			if let Some(build) = build {
				break 'a Some(build);
			}

			// Get the remote.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a None;
			};

			// Find a build.
			let list_arg = tg::build::ListArg {
				limit: 1,
				sort: tg::build::ListSort::Timestamp,
				target: arg.target.clone(),
			};
			let Some(build) = remote
				.try_list_builds(list_arg)
				.await?
				.values
				.first()
				.cloned()
				.map(|state| tg::Build::with_id(state.id))
			else {
				break 'a None;
			};

			// Verify the build satisfies the retry constraint.
			let status = build.status(self).await?;
			if status == tg::build::Status::Finished {
				let outcome = build.outcome(self).await?;
				if arg.options.retry >= outcome.retry() {
					break 'a None;
				}
			}

			Some(build)
		};

		// If a local or remote build was found that satisfies the retry constraint, then return it.
		if let Some(build) = build {
			// Update the queue with the depth if it is greater.
			let updated = {
				let db = self.inner.database.get().await?;
				let statement = "
					update queue
					set
						options = json_set(options, '$.depth', (select json(max(depth, ?1)))),
						depth = (select max(depth, ?1))
					where build = ?2;
				";
				let params = params![arg.options.depth, build.id().to_string()];
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let n = statement
					.execute(params)
					.wrap_err("Failed to execute the query.")?;
				n > 0
			};
			if updated {
				self.inner.local_queue_task_wake_sender.send_replace(());
			}

			// Add the build as a child of the parent.
			if let Some(parent) = arg.options.parent.as_ref() {
				self.add_build_child(user, parent, build.id()).await?;
			}

			let output = tg::build::GetOrCreateOutput {
				id: build.id().clone(),
			};

			return Ok(output);
		}

		// If the build has a parent and it is not local, or if the build has no parent and the remote flag is set, then attempt to get or create a remote build.
		'a: {
			// Determine if the build should be remote.
			let remote = if let Some(parent) = arg.options.parent.as_ref() {
				self.get_build_exists_local(parent).await?
			} else {
				arg.options.remote
			};
			if !remote {
				break 'a;
			}

			// Get the remote.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};

			// Push the target.
			let object = tg::object::Handle::with_id(arg.target.clone().into());
			let Ok(()) = object.push(self, remote.as_ref()).await else {
				break 'a;
			};

			// Get or create the build on the remote.
			let options = tg::build::Options {
				remote: false,
				..arg.options.clone()
			};
			let arg = tg::build::GetOrCreateArg {
				target: arg.target.clone(),
				options,
			};
			let Ok(output) = remote.get_or_create_build(user, arg).await else {
				break 'a;
			};

			return Ok(output);
		}

		// Otherwise, create a new build.
		let build_id = tg::build::Id::new();

		// Get the host.
		let target = tg::Target::with_id(arg.target.clone());
		let host = target.host(self).await?;

		// Create the build context.
		let (children, _) = tokio::sync::watch::channel(());
		let (log, _) = tokio::sync::watch::channel(());
		let (outcome, _) = tokio::sync::watch::channel(());
		let (stop, _) = tokio::sync::watch::channel(false);
		let context = Arc::new(BuildContext {
			build: build_id.clone(),
			children: Some(children),
			depth: arg.options.depth,
			log: Some(log),
			outcome: Some(outcome),
			task: std::sync::Mutex::new(None),
			stop,
		});
		self.inner
			.build_context
			.write()
			.unwrap()
			.insert(build_id.clone(), context);

		// Add the build to the database.
		{
			let state = tg::build::State {
				children: Vec::new(),
				id: build_id.clone(),
				log: None,
				outcome: None,
				status: tg::build::Status::Queued,
				target: arg.target.clone(),
				timestamp: time::OffsetDateTime::now_utc(),
			};
			let db = self.inner.database.get().await?;
			let statement = "
				insert into builds (id, state)
				values (?1, ?2);
			";
			let params = params![build_id.to_string(), Json(state)];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Add the build to the queue.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into queue (build, options, host, depth)
				values (?1, ?2, ?3, ?4);
			";
			let params = params![
				build_id.to_string(),
				Json(arg.options.clone()),
				host.to_string(),
				arg.options.depth,
			];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Add the build to the parent.
		if let Some(parent) = arg.options.parent.as_ref() {
			self.add_build_child(user, parent, &build_id).await?;
		}

		// Send a message to the build queue task that the item has been added.
		self.inner.local_queue_task_wake_sender.send_replace(());

		let output = tg::build::GetOrCreateOutput { id: build_id };

		Ok(output)
	}

	#[allow(clippy::too_many_lines)]
	pub(crate) async fn local_queue_task(
		&self,
		mut wake: tokio::sync::watch::Receiver<()>,
		mut stop: tokio::sync::watch::Receiver<bool>,
	) -> Result<()> {
		loop {
			loop {
				// Get the highest priority item from the queue.
				let (id, options, host, depth) = {
					let db = self.inner.database.get().await?;
					let statement = "
						delete from queue
						where rowid in (
							select rowid
							from queue
							order by depth desc
							limit 1
						)
						returning build, options, host, depth;
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
					let id = row
						.get::<_, String>(0)
						.wrap_err("Failed to deserialize the column.")?
						.parse::<tg::build::Id>()
						.wrap_err("Failed to parse the ID.")?;
					let options = row
						.get::<_, Json<tg::build::Options>>(1)
						.wrap_err("Failed to deserialize the column.")?
						.0;
					let host = row
						.get::<_, String>(2)
						.wrap_err("Failed to deserialize the column.")?
						.parse::<tg::System>()?;
					let depth = row
						.get::<_, u64>(3)
						.wrap_err("Failed to deserialize the column.")?;
					(id, options, host, depth)
				};

				// If the build is at a unique depth or there is a permit available, then start it.
				let unique = self
					.inner
					.build_context
					.read()
					.unwrap()
					.values()
					.filter(|context| context.build != id)
					.all(|context| context.depth != options.depth);

				let permit = self.inner.build_semaphore.clone().try_acquire_owned();
				let permit = match (unique, permit) {
					(_, Ok(permit)) => Some(permit),
					(true, Err(tokio::sync::TryAcquireError::NoPermits)) => None,
					(false, Err(tokio::sync::TryAcquireError::NoPermits)) => {
						let db = self.inner.database.get().await?;
						let statement = "
							insert into queue (build, options, host, depth)
							values (?1, ?2, ?3, ?4);
						";
						let params =
							params![id.to_string(), Json(options), host.to_string(), depth];
						let mut statement = db
							.prepare_cached(statement)
							.wrap_err("Failed to prepare the query.")?;
						statement
							.execute(params)
							.wrap_err("Failed to execute the query.")?;
						break;
					},
					(_, Err(tokio::sync::TryAcquireError::Closed)) => {
						unreachable!()
					},
				};

				// Set the build's status to running.
				self.set_build_status(None, &id, tg::build::Status::Running)
					.await?;

				// Spawn the task.
				let task = tokio::spawn({
					let server = self.clone();
					let id = id.clone();
					let options = options.clone();
					async move {
						if let Err(error) = server.build(id, options, permit).await {
							let trace = error.trace();
							tracing::error!(%trace, "The build failed.");
						}
					}
				});

				// Set the task in the build context.
				self.inner
					.build_context
					.write()
					.unwrap()
					.get_mut(&id)
					.unwrap()
					.task
					.lock()
					.unwrap()
					.replace(task);
			}

			// Wait for a wake or stop signal.
			tokio::select! {
				// If a wake signal is received, then loop again.
				_ = wake.changed() => {
					continue;
				}

				// If a stop signal is received, then return.
				() = stop.wait_for(|stop| *stop).map(|_| ()) => {
					return Ok(())
				}
			}
		}
	}

	pub(crate) async fn remote_queue_task(
		&self,
		stop_receiver: tokio::sync::watch::Receiver<bool>,
	) -> Result<()> {
		// Get the remote handle.
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(());
		};

		// Loop until the stop flag is set.
		while !*stop_receiver.borrow() {
			// If the queue is not empty, then sleep and continue.
			let empty = {
				let db = self.inner.database.get().await?;
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
				row.get::<_, bool>(0)
					.wrap_err("Failed to deserialize the column.")?
			};
			if !empty {
				tokio::time::sleep(std::time::Duration::from_secs(1)).await;
				continue;
			}

			// Get a permit.
			let permit = match self.inner.build_semaphore.clone().try_acquire_owned() {
				Ok(permit) => permit,
				Err(tokio::sync::TryAcquireError::NoPermits) => {
					tokio::time::sleep(std::time::Duration::from_secs(1)).await;
					continue;
				},
				Err(tokio::sync::TryAcquireError::Closed) => {
					unreachable!()
				},
			};

			// Attempt to get a build from the remote server's queue. If none is available, then sleep and continue.
			let hosts = vec![
				tg::System::new(tg::system::Arch::Js, tg::system::Os::Js),
				tg::System::host()?,
			];
			let queue_arg = tg::build::DequeueArg { hosts: Some(hosts) };
			let Some(tg::build::DequeueOutput { build: id, options }) = remote
				.try_dequeue_build(None, queue_arg)
				.await
				.ok()
				.flatten()
			else {
				drop(permit);
				tokio::time::sleep(std::time::Duration::from_secs(1)).await;
				continue;
			};

			// Create the build context.
			let (stop, _) = tokio::sync::watch::channel(false);
			let context = Arc::new(BuildContext {
				build: id.clone(),
				children: None,
				depth: options.depth,
				log: None,
				outcome: None,
				task: std::sync::Mutex::new(None),
				stop,
			});
			self.inner
				.build_context
				.write()
				.unwrap()
				.insert(id.clone(), context);

			// Set the build's status to running.
			self.set_build_status(None, &id, tg::build::Status::Running)
				.await?;

			// Spawn the task.
			let task = tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				let options = options.clone();
				async move {
					if let Err(error) = server.build(id, options, Some(permit)).await {
						let trace = error.trace();
						tracing::error!(%trace, "The build failed.");
					}
				}
			});

			// Set the task in the build context.
			self.inner
				.build_context
				.write()
				.unwrap()
				.get_mut(&id)
				.unwrap()
				.task
				.lock()
				.unwrap()
				.replace(task);
		}

		Ok(())
	}

	async fn build(
		&self,
		id: tg::build::Id,
		options: tg::build::Options,
		permit: Option<tokio::sync::OwnedSemaphorePermit>,
	) -> Result<()> {
		let build = tg::Build::with_id(id.clone());
		let target = build.target(self).await?;

		// Get the stop signal.
		let stop = self
			.inner
			.build_context
			.read()
			.unwrap()
			.get(&id)
			.unwrap()
			.stop
			.subscribe();

		// Build the target with the appropriate runtime.
		let result = match target.host(self).await?.os() {
			tg::system::Os::Js => {
				// Build the target on the server's local pool because it is a `!Send` future.
				tangram_runtime::js::build(self, &build, &options, stop).await
			},
			tg::system::Os::Darwin => {
				#[cfg(target_os = "macos")]
				{
					// If there's no VFS, perform an internal checkout on the target.
					if self.inner.vfs.lock().unwrap().is_none() {
						target
							.data(self)
							.await?
							.children()
							.into_iter()
							.filter_map(|id| id.try_into().ok())
							.map(|id| async move {
								let artifact = tg::Artifact::with_id(id);
								artifact.check_out(self, None).await
							})
							.collect::<FuturesUnordered<_>>()
							.try_collect::<Vec<_>>()
							.await?;
					}
					tangram_runtime::darwin::build(self, &build, &options, stop, self.path()).await
				}
				#[cfg(not(target_os = "macos"))]
				{
					return Err(error!("Cannot build a darwin target on this host."));
				}
			},
			tg::system::Os::Linux => {
				#[cfg(target_os = "linux")]
				{
					// If there's no VFS, perform an internal checkout on the target.
					if self.inner.vfs.lock().unwrap().is_none() {
						target
							.data(self)
							.await?
							.children()
							.into_iter()
							.filter_map(|id| id.try_into().ok())
							.map(|id| async move {
								let artifact = tg::Artifact::with_id(id);
								artifact.check_out(self, None).await
							})
							.collect::<FuturesUnordered<_>>()
							.try_collect::<Vec<_>>()
							.await?;
					}
					tangram_runtime::linux::build(self, &build, &options, stop, self.path()).await
				}
				#[cfg(not(target_os = "linux"))]
				{
					return Err(error!("Cannot build a linux target on this host."));
				}
			},
		};

		// Create the outcome.
		let outcome = match result {
			Ok(Some(value)) => tg::build::Outcome::Succeeded(value),
			Ok(None) => tg::build::Outcome::Terminated,
			Err(error) => tg::build::Outcome::Failed(error),
		};

		// If the build failed, add a message to the build's log.
		if let tg::build::Outcome::Failed(error) = &outcome {
			build
				.add_log(self, error.trace().to_string().into())
				.await?;
		}

		// Set the outcome.
		build.set_outcome(self, None, outcome).await?;

		// Drop the permit.
		drop(permit);

		// Wake the local queue task.
		self.inner.local_queue_task_wake_sender.send_replace(());

		Ok(())
	}

	#[allow(clippy::unused_async)]
	pub async fn try_dequeue_build(
		&self,
		_user: Option<&tg::User>,
		_arg: tg::build::DequeueArg,
	) -> Result<Option<tg::build::DequeueOutput>> {
		Ok(None)
	}

	pub async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::Status>> {
		if let Some(status) = self.try_get_build_status_local(id).await? {
			Ok(Some(status))
		} else if let Some(status) = self.try_get_build_status_remote(id).await? {
			Ok(Some(status))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_status_local(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::Status>> {
		let db = self.inner.database.get().await?;
		let statement = "
			select state->>'status' as status
			from builds
			where id = ?1;
		";
		let params = params![id.to_string()];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
			return Ok(None);
		};
		let status = row
			.get::<_, String>(0)
			.wrap_err("Failed to deserialize the column.")?
			.parse()?;
		Ok(Some(status))
	}

	async fn try_get_build_status_remote(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::Status>> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};
		let Some(status) = remote.try_get_build_status(id).await? else {
			return Ok(None);
		};
		Ok(Some(status))
	}

	pub async fn set_build_status(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<()> {
		if self.try_set_build_status_local(user, id, status).await? {
			return Ok(());
		}
		if self.try_set_build_status_remote(user, id, status).await? {
			return Ok(());
		}
		Err(error!("Failed to find the build."))
	}

	async fn try_set_build_status_local(
		&self,
		_user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<bool> {
		let db = self.inner.database.get().await?;
		let statement = "
			update builds
			set state = json_set(state, '$.status', ?1)
			where id = ?2;
		";
		let params = params![status.to_string(), id.to_string()];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let n = statement
			.execute(params)
			.wrap_err("Failed to execute the query.")?;
		Ok(n > 0)
	}

	async fn try_set_build_status_remote(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<bool> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};
		remote.set_build_status(user, id, status).await?;
		Ok(true)
	}

	pub async fn try_get_build_children(
		&self,
		id: &tg::build::Id,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Id>>>> {
		if let Some(children) = self.try_get_build_children_local(id, stop).await? {
			Ok(Some(children))
		} else if let Some(children) = self.try_get_build_children_remote(id).await? {
			Ok(Some(children))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_children_local(
		&self,
		id: &tg::build::Id,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Id>>>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		let context = self.inner.build_context.read().unwrap().get(id).cloned();
		let children = context.as_ref().map(|context| {
			WatchStream::new(context.children.as_ref().unwrap().subscribe())
				.fuse()
				.boxed()
		});
		let outcome = context.as_ref().map(|channels| {
			WatchStream::new(channels.outcome.as_ref().unwrap().subscribe())
				.fuse()
				.boxed()
		});

		// Create the children stream.
		struct State {
			server: Server,
			id: tg::build::Id,
			index: usize,
			first: bool,
			last: bool,
			children: Option<BoxStream<'static, ()>>,
			outcome: Option<BoxStream<'static, ()>>,
			stop: Option<tokio::sync::watch::Receiver<bool>>,
		}
		let state = State {
			server: self.clone(),
			id: id.clone(),
			index: 0,
			first: true,
			last: false,
			children,
			outcome,
			stop,
		};
		let children = stream::try_unfold(state, |mut state| async move {
			if state.first {
				state.first = false;
			} else if !state.last {
				tokio::select! {
					_ = state.children.as_mut().wrap_err("Expected the channel to exist.")?.next() => {}
					_ = state.outcome.as_mut().wrap_err("Expected the channel to exist.")?.next() => {}
					() = state.stop.as_mut().map(|stop| stop.wait_for(|stop| *stop).map(|_| ())).map_or_else(|| Either::Left(future::pending()), Either::Right) => {
						return Err(error!("The server was stopped."));
					}
				}
			} else {
				return Ok(None);
			}
			let (status, children) = {
				let db = state.server.inner.database.get().await?;
				let statement = "
					select
						state->>'status' as status,
						(
							select coalesce(json_group_array(value), '[]')
							from (
								select value
								from json_each(builds.state->'children')
								limit -1
								offset ?1
							)
						) as children
					from builds
					where id = ?2;
				";
				let params = params![state.index.to_i64().unwrap(), state.id.to_string()];
				let mut statement = db
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the statement.")?;
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
					.wrap_err("Failed to parse the status.")?;
				let children = row
					.get::<_, Json<Vec<tg::build::Id>>>(1)
					.wrap_err("Failed to deseriaize the children.")?
					.0;
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

	async fn try_get_build_children_remote(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Id>>>> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};
		let Some(children) = remote.try_get_build_children(id).await? else {
			return Ok(None);
		};
		Ok(Some(children))
	}

	pub async fn add_build_child(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> Result<()> {
		if self
			.try_add_build_child_local(user, build_id, child_id)
			.await?
		{
			return Ok(());
		}
		if self
			.try_add_build_child_remote(user, build_id, child_id)
			.await?
		{
			return Ok(());
		}
		Err(error!("Failed to find the build."))
	}

	async fn try_add_build_child_local(
		&self,
		_user: Option<&tg::User>,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> Result<bool> {
		// Verify the build is local.
		if !self.get_build_exists_local(build_id).await? {
			return Ok(false);
		}

		// Add the child to the build in the database.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				update builds
				set state = json_set(state, '$.children[#]', ?1)
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
		if let Some(children) = self
			.inner
			.build_context
			.read()
			.unwrap()
			.get(build_id)
			.unwrap()
			.children
			.as_ref()
		{
			children.send_replace(());
		}

		Ok(true)
	}

	async fn try_add_build_child_remote(
		&self,
		user: Option<&tg::User>,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> Result<bool> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};
		tg::Build::with_id(child_id.clone())
			.push(user, self, remote.as_ref())
			.await?;
		remote.add_build_child(user, build_id, child_id).await?;
		Ok(true)
	}

	pub async fn try_get_build_log(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetLogArg,
	) -> Result<Option<BoxStream<'static, Result<tg::build::LogChunk>>>> {
		if let Some(log) = self.try_get_build_log_local(id, arg.clone()).await? {
			Ok(Some(log))
		} else if let Some(log) = self.try_get_build_log_remote(id, arg.clone()).await? {
			Ok(Some(log))
		} else {
			Ok(None)
		}
	}

	#[allow(clippy::too_many_lines)]
	async fn try_get_build_log_local(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetLogArg,
	) -> Result<Option<BoxStream<'static, Result<tg::build::LogChunk>>>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Check if this build's log is stored as a blob.
		if let Some(id) = self.try_get_build_log_data_local(id).await? {
			let blob = tg::blob::Blob::with_id(id);
			let mut reader = blob
				.reader(self)
				.await
				.wrap_err("Failed to create blob reader.")?;
			let pos = arg.pos.unwrap_or(blob.size(self).await?);
			let (start, end) = match arg.len {
				Some(len) if len > 0 => (pos, pos.saturating_add(len.to_u64().unwrap())),
				Some(len) => (pos.saturating_sub(len.abs().to_u64().unwrap()), pos),
				None => (
					pos,
					blob.size(self).await.wrap_err("Failed to get blob size.")?,
				),
			};
			reader
				.seek(std::io::SeekFrom::Start(start))
				.await
				.wrap_err("Failed to seek.")?;
			let stream = stream::once(async move {
				let mut bytes = vec![0; (end - start).to_usize().unwrap()];
				reader
					.read_exact(&mut bytes)
					.await
					.wrap_err("Failed to read blob.")?;
				let pos = start;
				let bytes = bytes.into();
				let mut chunk = tg::build::LogChunk { pos, bytes };
				if chunk.pos < start {
					let offset = (start - chunk.pos).to_usize().unwrap();
					chunk.pos = start;
					chunk.bytes = chunk.bytes.slice(offset..);
				}
				if (chunk.pos + chunk.bytes.len().to_u64().unwrap()) > end {
					let length = (end - chunk.pos).to_usize().unwrap();
					chunk.bytes = chunk.bytes.slice(0..length);
				}
				Ok(chunk)
			})
			.boxed();
			return Ok(Some(stream));
		}

		// Otherwise get the starting position of the log stream. If pos is None, return the end.
		let start_pos = if let Some(pos) = arg.pos {
			pos
		} else {
			let db = self.inner.database.get().await?;
			let statement = "
					select coalesce(max(position) + length(bytes), 0)
					from logs
					where build = ?1
				";
			let params = params![id.to_string()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare statement.")?;
			let mut query = statement
				.query(params)
				.wrap_err("Failed to perform query.")?;
			let row = query
				.next()
				.wrap_err("Failed to get row.")?
				.wrap_err("Expected a row.")?;
			row.get(0).wrap_err("Expected a position.")?
		};

		// Get the start and end positions
		let (start, end) = match arg.len {
			None => (start_pos, None),
			Some(len) if len > 0 => (
				start_pos,
				Some(start_pos.saturating_add(len.to_u64().unwrap())),
			),
			Some(len) => (
				start_pos.saturating_sub(len.abs().to_u64().unwrap()),
				arg.pos,
			),
		};

		// Create the stream.
		let offset = 0;
		let count = 0;
		let server = self.clone();
		let id = id.clone();
		let stream = stream::try_unfold(
			(start, end, count, offset, id, server),
			|(start, end, count, offset, id, server)| async move {
				// Check if the end has been reached.
				match end {
					Some(end) if start + count >= end => return Ok(None),
					_ => (),
				}

				// Get the next log chunk.
				let mut chunk = loop {
					let build_status = server
						.get_build_status(&id)
						.await
						.wrap_err("Failed to get the build status.")?;
					let db = server.inner.database.get().await?;
					let statement = "
						select position, bytes
						from logs
						where
							build = ?1 and (
								position >= ?2 or (
									position < ?2 and
									position + length(bytes) > ?2
								)
							)
						order by position
						limit 1
						offset ?3;
					";
					let params = params![id.to_string(), start, offset];
					let mut statement = db
						.prepare_cached(statement)
						.wrap_err("Failed to prepare statement.")?;
					let mut query = statement
						.query(params)
						.wrap_err("Failed to perform query.")?;
					let chunk = query.next().wrap_err("Expected a row.")?.map(|row| {
						let pos = row.get(0).unwrap();
						let bytes = row.get::<_, Vec<u8>>(1).unwrap().into();
						tg::build::LogChunk { pos, bytes }
					});
					match (build_status, chunk) {
						(tg::build::Status::Finished, None) => return Ok(None),
						(_, Some(chunk)) => break chunk,
						_ => continue,
					}
				};

				// Truncate the chunk.
				if chunk.pos < start {
					let offset = (start - chunk.pos).to_usize().unwrap();
					chunk.pos = start;
					chunk.bytes = chunk.bytes.slice(offset..);
				}
				match end {
					Some(end) if (chunk.pos + chunk.bytes.len().to_u64().unwrap()) > end => {
						let length = (end - chunk.pos).to_usize().unwrap();
						chunk.bytes = chunk.bytes.slice(0..length);
					},
					_ => (),
				}
				let offset = offset + 1;
				let count = count + chunk.bytes.len().to_u64().unwrap();
				Ok(Some((chunk, (start, end, count, offset, id, server))))
			},
		);
		Ok(Some(stream.boxed()))
	}

	async fn try_get_build_log_data_local(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::blob::Id>> {
		let db = self.inner.database.get().await?;
		let statement = "
			select state->>'log' as log
			from builds
			where id = ?1;
		";
		let params = params![id.to_string()];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
			return Ok(None);
		};
		let Some(log) = row
			.get::<_, Option<String>>(0)
			.wrap_err("Failed to deserialize the column.")?
		else {
			return Ok(None);
		};
		let log = log.parse()?;
		Ok(Some(log))
	}

	async fn try_get_build_log_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetLogArg,
	) -> Result<Option<BoxStream<'static, Result<tg::build::LogChunk>>>> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};
		let Some(log) = remote.try_get_build_log(id, arg).await? else {
			return Ok(None);
		};
		Ok(Some(log))
	}

	pub async fn add_build_log(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<()> {
		if self
			.try_add_build_log_local(user, id, bytes.clone())
			.await?
		{
			return Ok(());
		}
		if self
			.try_add_build_log_remote(user, id, bytes.clone())
			.await?
		{
			return Ok(());
		}
		Err(error!("Failed to find the build."))
	}

	async fn try_add_build_log_local(
		&self,
		_user: Option<&tg::User>,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<bool> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(false);
		}

		// Add the log to the database.
		{
			let db = self.inner.database.get().await?;
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
			let params = params![id.to_string(), bytes.to_vec()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Notify subscribers that the log has been added to.
		if let Some(log) = self
			.inner
			.build_context
			.read()
			.unwrap()
			.get(id)
			.unwrap()
			.log
			.as_ref()
		{
			log.send_replace(());
		}

		Ok(true)
	}

	async fn try_add_build_log_remote(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		bytes: Bytes,
	) -> Result<bool> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};
		remote.add_build_log(user, id, bytes).await?;
		Ok(true)
	}

	pub async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<tg::build::Outcome>> {
		if let Some(outcome) = self.try_get_build_outcome_local(id, stop).await? {
			Ok(Some(outcome))
		} else if let Some(outcome) = self.try_get_build_outcome_remote(id).await? {
			Ok(Some(outcome))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_outcome_local(
		&self,
		id: &tg::build::Id,
		mut stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<tg::build::Outcome>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Get the outcome.
		let context = self.inner.build_context.read().unwrap().get(id).cloned();
		let mut outcome = context
			.as_ref()
			.map(|context| WatchStream::new(context.outcome.as_ref().unwrap().subscribe()));
		let mut first = true;
		loop {
			if first {
				first = false;
			} else {
				tokio::select! {
					_ = outcome.as_mut().wrap_err("Expected the channel to exist.")?.next() => {}
					() = stop.as_mut().map(|stop| stop.wait_for(|stop| *stop).map(|_| ())).map_or_else(|| Either::Left(future::pending()), Either::Right) => {
						return Err(error!("The server was stopped."));
					}
				}
			}
			let db = self.inner.database.get().await?;
			let statement = "
				select state->'outcome' as outcome
				from builds
				where id = ?1;
			";
			let params = params![id.to_string()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let mut rows = statement
				.query(params)
				.wrap_err("Failed to execute the query.")?;
			let row = rows
				.next()
				.wrap_err("Failed to get the row.")?
				.wrap_err("Expected a row.")?;
			let outcome = row
				.get::<_, Json<Option<tg::build::Outcome>>>(0)
				.wrap_err("Failed to deserialize the column.")?
				.0;
			if let Some(outcome) = outcome {
				return Ok(Some(outcome));
			}
		}
	}

	async fn try_get_build_outcome_remote(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::Outcome>> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};
		let Some(result) = remote.try_get_build_outcome(id).await? else {
			return Ok(None);
		};
		Ok(Some(result))
	}

	#[allow(clippy::too_many_lines)]
	#[async_recursion]
	pub async fn set_build_outcome(
		&self,
		user: Option<&'async_recursion tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		if self
			.try_set_build_outcome_local(user, id, outcome.clone())
			.await?
		{
			return Ok(());
		}
		if self
			.try_set_build_outcome_remote(user, id, outcome.clone())
			.await?
		{
			return Ok(());
		}
		Err(error!("Failed to find the build."))
	}

	async fn try_set_build_outcome_local(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<bool> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(false);
		}

		// If the build is finished, then return.
		let status = self
			.try_get_build_status_local(id)
			.await?
			.wrap_err("Expected the build to exist.")?;
		if status == tg::build::Status::Finished {
			return Ok(true);
		}

		// Get the children.
		let children: Vec<tg::build::Id> = {
			let db = self.inner.database.get().await?;
			let statement = "
				select json_extract(state, '$.children')
				from builds
				where id = ?1;
			";
			let params = params![id.to_string()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			let mut rows = statement
				.query(params)
				.wrap_err("Failed to execute the query.")?;
			let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
				return Ok(false);
			};
			row.get::<_, Json<_>>(0)
				.wrap_err("Failed to deserialize the column.")?
				.0
		};

		// If the build was canceled, then cancel the children.
		if matches!(outcome, tg::build::Outcome::Canceled) {
			children
				.iter()
				.map(|child| async move {
					self.set_build_outcome(user, child, tg::build::Outcome::Canceled)
						.await?;
					Ok::<_, Error>(())
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect()
				.await?;
		}

		// If any of the children were canceled, then this build should be canceled.
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

		// Update the state.
		{
			let status = tg::build::Status::Finished;
			let outcome = outcome.data(self).await?;
			let db = self.inner.database.get().await?;
			let statement = "
				update builds
				set state = json_set(
					state,
					'$.status', ?1,
					'$.outcome', json(?2)
				)
				where id = ?3;
			";
			let params = params![status.to_string(), Json(outcome), id.to_string()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Notify subscribers that the outcome has been set.
		if let Some(outcome) = self
			.inner
			.build_context
			.read()
			.unwrap()
			.get(id)
			.unwrap()
			.outcome
			.as_ref()
		{
			outcome.send_replace(());
		}

		// Remove the build context.
		self.inner.build_context.write().unwrap().remove(id);

		Ok(true)
	}

	async fn try_set_build_outcome_remote(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<bool> {
		// Get the remote handle.
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};

		// Push the outcome.
		if let tg::build::Outcome::Succeeded(value) = &outcome {
			value.push(self, remote.as_ref()).await?;
		}

		// Set the outcome.
		remote.set_build_outcome(user, id, outcome).await?;

		// Remove the build context.
		self.inner.build_context.write().unwrap().remove(id);

		Ok(true)
	}
}
