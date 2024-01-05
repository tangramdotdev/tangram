use super::Server;
use crate::{database::Json, params, Channels};
use async_recursion::async_recursion;
use bytes::Bytes;
use futures::{
	future,
	stream::{self, BoxStream, FuturesUnordered},
	FutureExt, StreamExt, TryStreamExt,
};
use num::ToPrimitive;
use std::sync::Arc;
use tangram_client as tg;
use tangram_error::{error, return_error, Error, Result, WrapErr};
use tg::Handle;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_stream::wrappers::WatchStream;
use tokio_util::either::Either;

impl Server {
	pub async fn try_get_assignment(
		&self,
		target_id: &tg::target::Id,
	) -> Result<Option<tg::build::Id>> {
		if let Some(id) = self.try_get_assignment_local(target_id).await? {
			Ok(Some(id))
		} else if let Some(id) = self.try_get_assignment_remote(target_id).await? {
			Ok(Some(id))
		} else {
			Ok(None)
		}
	}

	async fn try_get_assignment_local(
		&self,
		target_id: &tg::target::Id,
	) -> Result<Option<tg::build::Id>> {
		let db = self.inner.database.get().await?;
		let statement = "
			select build
			from assignments
			where target = ?1;
		";
		let params = params![target_id.to_string()];
		let mut statement = db
			.prepare_cached(statement)
			.wrap_err("Failed to prepare the query.")?;
		let mut rows = statement
			.query(params)
			.wrap_err("Failed to execute the query.")?;
		let Some(row) = rows.next().wrap_err("Failed to get the row.")? else {
			return Ok(None);
		};
		let id = row
			.get::<_, String>(0)
			.wrap_err("Failed to deserialize the column.")?
			.parse()?;
		Ok(Some(id))
	}

	async fn try_get_assignment_remote(
		&self,
		target_id: &tg::target::Id,
	) -> Result<Option<tg::build::Id>> {
		// Get the remote handle.
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};

		// Get the assignment from the remote server.
		let Some(build_id) = remote.try_get_assignment(target_id).await? else {
			return Ok(None);
		};

		// Add the assignment to the database.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into assignments (target, build)
				values (?1, ?2)
				on conflict (target) do update set build = ?2;
			";
			let params = params![target_id.to_string(), build_id.to_string()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		Ok(Some(build_id))
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
		let exists = row.get::<_, bool>(0).wrap_err("Expected a bool.")?;
		Ok(exists)
	}

	async fn get_build_exists_remote(&self, id: &tg::build::Id) -> Result<bool> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};
		let exists = remote.get_build_exists(id).await?;
		Ok(exists)
	}

	pub async fn try_get_build(&self, id: &tg::build::Id) -> Result<Option<tg::build::Data>> {
		if let Some(build) = self.try_get_build_local(id).await? {
			Ok(Some(build))
		} else if let Some(build) = self.try_get_build_remote(id).await? {
			Ok(Some(build))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_local(&self, id: &tg::build::Id) -> Result<Option<tg::build::Data>> {
		let db = self.inner.database.get().await?;
		let statement = "
			select json
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
		let build = row
			.get::<_, Json<tg::build::Data>>(0)
			.wrap_err("Failed to deserialize the column.")?
			.0;
		Ok(Some(build))
	}

	async fn try_get_build_remote(&self, id: &tg::build::Id) -> Result<Option<tg::build::Data>> {
		// Get the remote.
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};

		// Get the build from the remote server.
		let Some(build) = remote.try_get_build(id).await? else {
			return Ok(None);
		};

		// Add the build to the database.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into builds (id, json)
				values (?1, ?2)
				on conflict (id) do update set json = ?2;
			";
			let params = params![id.to_string(), Json(build.clone())];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		Ok(Some(build))
	}

	pub async fn try_put_build(
		&self,
		_user: Option<&tg::User>,
		id: &tg::build::Id,
		data: &tg::build::Data,
	) -> Result<tg::build::PutOutput> {
		// Verify the build is finished.
		if data.status != tg::build::Status::Finished {
			return Err(error!("The build is not finished."));
		}

		// Check if there are any missing children, log, outcome, or target.
		let children = stream::iter(data.children.clone())
			.map(Ok)
			.try_filter_map(|id| async move {
				let exists = self.get_build_exists(&id).await?;
				Ok::<_, Error>(if exists { None } else { Some(id) })
			})
			.try_collect::<Vec<_>>()
			.await?;
		let log = if let Some(log) = &data.log {
			self.get_object_exists(&log.clone().into()).await?
		} else {
			false
		};
		let outcome = if let Some(tg::build::outcome::Data::Succeeded(value)) = &data.outcome {
			stream::iter(value.children())
				.map(Ok)
				.and_then(|id| async move { self.get_object_exists(&id).await })
				.try_all(|exists| async move { exists })
				.await?
		} else {
			false
		};
		let target = self.get_object_exists(&data.target.clone().into()).await?;
		if !children.is_empty() || log || outcome || target {
			return Ok(tg::build::PutOutput {
				missing: tg::build::Missing {
					children,
					log,
					outcome,
					target,
				},
			});
		}

		// Insert the build.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into builds (id, json)
				values (?1, ?2)
				on conflict do update set json = ?2;
			";
			let params = params![id.to_string(), Json(data.clone())];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Add an assignment if one does not exist.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into assignments (target, build)
				values (?1, ?2)
				on conflict do nothing;
			";
			let params = params![data.target.to_string(), id.to_string()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		Ok(tg::build::PutOutput::default())
	}

	#[allow(clippy::too_many_lines)]
	pub async fn get_or_create_build(
		&self,
		target_id: &tg::target::Id,
		options: tg::build::Options,
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
			if options.retry >= outcome.retry() {
				break 'a;
			}
			if let Some(parent) = options.parent.as_ref() {
				self.add_build_child(options.user.as_ref(), parent.id(), &build_id)
					.await?;
			}
			return Ok(build_id);
		}

		// If the build has a parent and it is not local, or if the build has no parent and the remote flag is set, then attempt to get or create the build on the remote.
		'a: {
			// Determine if the build should be remote.
			let remote = if let Some(parent) = options.parent.as_ref() {
				self.build_is_local(parent.id()).await?
			} else {
				options.remote
			};
			if !remote {
				break 'a;
			}

			// Get the remote.
			let Some(remote) = self.inner.remote.as_ref() else {
				break 'a;
			};

			// Push the target.
			let object = tg::object::Handle::with_id(target_id.clone().into());
			let Ok(()) = object.push(self, remote.as_ref()).await else {
				break 'a;
			};

			// If there is a local assignment, then push the build.
			if let Some(build_id) = self.try_get_assignment_local(target_id).await? {
				tg::Build::with_id(build_id.clone())
					.push(options.user.as_ref(), self, remote.as_ref())
					.await?;
			}

			// Get or create the build on the remote.
			let options = tg::build::Options {
				remote: false,
				..options.clone()
			};
			let Ok(build_id) = remote.get_or_create_build(target_id, options).await else {
				break 'a;
			};

			return Ok(build_id);
		}

		// Otherwise, create a new build.
		let build_id = tg::build::Id::new();

		// Add the build to the channels.
		let (children, _) = tokio::sync::watch::channel(());
		let (log, _) = tokio::sync::watch::channel(());
		let (outcome, _) = tokio::sync::watch::channel(());
		let (stop, _) = tokio::sync::watch::channel(false);
		let channels = Arc::new(Channels {
			children,
			log,
			outcome,
			stop,
		});
		self.inner
			.channels
			.write()
			.unwrap()
			.insert(build_id.clone(), channels);

		// Add the build to the depths.
		self.inner
			.depths
			.write()
			.unwrap()
			.insert(build_id.clone(), options.depth);

		// Add the build to the database.
		{
			let data = tg::build::Data {
				children: Vec::new(),
				log: None,
				outcome: None,
				target: target_id.clone(),
				status: tg::build::Status::Queued,
			};
			let db = self.inner.database.get().await?;
			let statement = "
				insert into builds (id, json)
				values (?1, ?2);
			";
			let params = params![build_id.to_string(), Json(data)];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Add the assignment to the database.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into assignments (target, build)
				values (?1, ?2)
				on conflict (target) do update set build = ?2;
			";
			let params = params![target_id.to_string(), build_id.to_string()];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Create the item.
		let item = tg::build::queue::Item {
			build: build_id.clone(),
			host,
			depth: options.depth,
			retry: options.retry,
		};

		// Add the item to the queue.
		{
			let db = self.inner.database.get().await?;
			let statement = "
				insert into queue (json)
				values (?1);
			";
			let params = params![Json(item)];
			let mut statement = db
				.prepare_cached(statement)
				.wrap_err("Failed to prepare the query.")?;
			statement
				.execute(params)
				.wrap_err("Failed to execute the query.")?;
		}

		// Send a message to the build queue task that the item has been added.
		self.inner.local_queue_task_wake_sender.send_replace(());

		// Add the build to the parent.
		if let Some(parent) = options.parent.as_ref() {
			self.add_build_child(options.user.as_ref(), parent.id(), &build_id)
				.await?;
		}

		Ok(build_id)
	}

	pub(crate) async fn local_queue_task(
		&self,
		mut wake: tokio::sync::watch::Receiver<()>,
		mut stop: tokio::sync::watch::Receiver<bool>,
	) -> Result<()> {
		loop {
			loop {
				// Get the highest priority item from the queue.
				let item: tg::build::queue::Item = {
					let db = self.inner.database.get().await?;
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
					row.get::<_, Json<_>>(0)
						.wrap_err("Failed to deserialize the column.")?
						.0
				};

				// If the build is at a unique depth, then start it.
				let unique = self
					.inner
					.depths
					.read()
					.unwrap()
					.values()
					.all(|depth| depth != &item.depth);
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
						let db = self.inner.database.get().await?;
						let statement = "
							insert into queue (json)
							values (?1);
						";
						let params = params![Json(item)];
						let mut statement = db
							.prepare_cached(statement)
							.wrap_err("Failed to prepare the query.")?;
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

			// Wait for a wake or stop signal.
			tokio::select! {
				// If a wake signal is received, the loop again.
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

	async fn start_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		depth: u64,
		retry: tg::build::Retry,
		permit: Option<tokio::sync::OwnedSemaphorePermit>,
	) -> Result<()> {
		// Update the status.
		self.set_build_status(user, id, tg::build::Status::Running)
			.await?;

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

		// Set the task for the build.
		self.inner
			.tasks
			.lock()
			.unwrap()
			.as_mut()
			.unwrap()
			.insert(id.clone(), task);

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
		let stop = self
			.inner
			.channels
			.read()
			.unwrap()
			.get(id)
			.unwrap()
			.stop
			.subscribe();

		// Build the target with the appropriate runtime.
		let result = match target.host(self).await?.os() {
			tg::system::Os::Js => {
				// Build the target on the server's local pool because it is a `!Send` future.
				tangram_runtime::js::build(self, &build, depth, retry, stop).await
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

		// Finish the build.
		build.finish(self, user, outcome).await?;

		// Drop the permit.
		drop(permit);

		// Wake the local queue task.
		self.inner.local_queue_task_wake_sender.send_replace(());

		Ok(())
	}

	#[allow(clippy::unused_async)]
	pub async fn try_get_queue_item(
		&self,
		_user: Option<&tg::User>,
		_hosts: Option<Vec<tg::System>>,
	) -> Result<Option<tg::build::queue::Item>> {
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

	async fn try_get_build_log_data_local(&self, id: &tg::build::Id) -> Result<Option<tg::blob::Id>> {
		let db = self.inner.database.get().await?;
		let statement = "
			select json->>'log' as log
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

	async fn try_get_build_status_local(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::Status>> {
		let db = self.inner.database.get().await?;
		let statement = "
			select json->>'status' as status
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
			set json = json_set(json, '$.status', ?1)
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

	pub async fn try_get_build_target(&self, id: &tg::build::Id) -> Result<Option<tg::target::Id>> {
		if let Some(target) = self.try_get_build_target_local(id).await? {
			Ok(Some(target))
		} else if let Some(target) = self.try_get_build_target_remote(id).await? {
			Ok(Some(target))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_target_local(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::target::Id>> {
		let db = self.inner.database.get().await?;
		let statement = "
			select json->>'target' as target
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
		let target_id = row
			.get::<_, String>(0)
			.wrap_err("Failed to deserialize the column.")?
			.parse()?;
		Ok(Some(target_id))
	}

	async fn try_get_build_target_remote(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::target::Id>> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};
		let Some(target_id) = remote.try_get_build_target(id).await? else {
			return Ok(None);
		};
		Ok(Some(target_id))
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
		if !self.build_is_local(id).await? {
			return Ok(None);
		}

		let channels = self.inner.channels.read().unwrap().get(id).cloned();
		let children = channels.as_ref().map(|channels| {
			WatchStream::new(channels.children.subscribe())
				.fuse()
				.boxed()
		});
		let outcome = channels.as_ref().map(|channels| {
			WatchStream::new(channels.outcome.subscribe())
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
						return_error!("The server was stopped.");
					}
				}
			} else {
				return Ok(None);
			}
			let (status, children) = {
				let db = state.server.inner.database.get().await?;
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
		if !self.build_is_local(build_id).await? {
			return Ok(false);
		}

		// Add the child to the build in the database.
		{
			let db = self.inner.database.get().await?;
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
		pos: Option<u64>,
		len: Option<i64>,
	) -> Result<Option<BoxStream<'static, Result<tg::log::Entry>>>> {
		if let Some(log) = self.try_get_build_log_local(id, pos, len).await? {
			Ok(Some(log))
		} else if let Some(log) = self.try_get_build_log_remote(id, pos, len).await? {
			Ok(Some(log))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_log_local(
		&self,
		id: &tg::build::Id,
		pos: Option<u64>,
		len: Option<i64>,
	) -> Result<Option<BoxStream<'static, Result<tg::log::Entry>>>> {
		// Verify the build is local.
		if !self.build_is_local(id).await? {
			return Ok(None);
		}

		// Check if we have this build's log stored as a blob.
		if let Some(id) = self.try_get_build_log_data_local(id).await? {
			let blob = tg::blob::Blob::with_id(id);
			let mut reader = blob
				.reader(self)
				.await
				.wrap_err("Failed to create blob reader.")?;
			let pos = pos.unwrap_or(blob.size(self).await?);
			let (start, end) = match len {
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
				let mut entry = tg::log::Entry { pos, bytes };
				if entry.pos < start {
					let offset = (start - entry.pos).to_usize().unwrap();
					entry.pos = start;
					entry.bytes = entry.bytes.slice(offset..);
				}
				if (entry.pos + entry.bytes.len().to_u64().unwrap()) > end {
					let length = (end - entry.pos) as usize;
					entry.bytes = entry.bytes.slice(0..length);
				}
				Ok(entry)
			})
			.boxed();
			return Ok(Some(stream));
		}

		// Otherwise get the starting position of the log stream. If pos is None, return the end.
		let start_pos = match pos {
			Some(pos) => pos,
			None => {
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
			},
		};

		// Get the start and end positions
		let (start, end) = match len {
			None => (start_pos, None),
			Some(len) if len > 0 => (
				start_pos,
				Some(start_pos.saturating_add(len.to_u64().unwrap())),
			),
			Some(len) => (start_pos.saturating_sub(len.abs().to_u64().unwrap()), pos), // Note: not Some(start_pos).
		};

		// Create the stream.
		let offset = 0;
		let count = 0;
		let server = self.clone();
		let id = id.clone();
		let stream = stream::try_unfold(
			(start, end, count, offset, id, server),
			|(start, end, count, offset, id, server)| async move {
				// Check if we've reached the end condition.
				match end {
					Some(end) if start + count >= end => return Ok(None),
					_ => (),
				}

				// Get the next log entry.
				let mut entry = loop {
					let build_status = server
						.get_build_status(&id)
						.await
						.wrap_err("Failed to get build status.")?;
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
					let entry = query.next().wrap_err("Expected a row.")?.map(|row| {
						let pos = row.get(0).unwrap();
						let bytes = row.get::<_, Vec<u8>>(1).unwrap().into();
						tg::log::Entry { pos, bytes }
					});
					match (build_status, entry) {
						(tg::build::Status::Finished, None) => return Ok(None),
						(_, Some(entry)) => break entry,
						_ => continue,
					}
				};

				// Truncate the entry.
				if entry.pos < start {
					let offset = (start - entry.pos).to_usize().unwrap();
					entry.pos = start;
					entry.bytes = entry.bytes.slice(offset..);
				}
				match end {
					Some(end) if (entry.pos + entry.bytes.len().to_u64().unwrap()) > end => {
						let length = (end - entry.pos) as usize;
						entry.bytes = entry.bytes.slice(0..length);
					},
					_ => (),
				}
				let offset = offset + 1;
				let count = count + entry.bytes.len().to_u64().unwrap();
				Ok(Some((entry, (start, end, count, offset, id, server))))
			},
		);
		Ok(Some(stream.boxed()))
	}

	async fn try_get_build_log_remote(
		&self,
		id: &tg::build::Id,
		pos: Option<u64>,
		len: Option<i64>,
	) -> Result<Option<BoxStream<'static, Result<tg::log::Entry>>>> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};
		let Some(log) = remote.try_get_build_log(id, pos, len).await? else {
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
		if !self.build_is_local(id).await? {
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
		if let Some(channels) = self.inner.channels.read().unwrap().get(id).cloned() {
			channels.log.send_replace(());
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
		if !self.build_is_local(id).await? {
			return Ok(None);
		}

		// Get the outcome.
		let channels = self.inner.channels.read().unwrap().get(id).cloned();
		let mut outcome = channels
			.as_ref()
			.map(|channels| WatchStream::new(channels.outcome.subscribe()));
		let mut first = true;
		loop {
			if first {
				first = false;
			} else {
				tokio::select! {
					_ = outcome.as_mut().wrap_err("Expected the channel to exist.")?.next() => {}
					() = stop.as_mut().map(|stop| stop.wait_for(|stop| *stop).map(|_| ())).map_or_else(|| Either::Left(future::pending()), Either::Right) => {
						return_error!("The server was stopped.");
					}
				}
			}
			let db = self.inner.database.get().await?;
			let statement = "
				select json->'outcome' as outcome
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
	pub async fn finish_build(
		&self,
		user: Option<&'async_recursion tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<()> {
		if self
			.try_finish_build_local(user, id, outcome.clone())
			.await?
		{
			return Ok(());
		}
		if self
			.try_finish_build_remote(user, id, outcome.clone())
			.await?
		{
			return Ok(());
		}
		Err(error!("Failed to find the build."))
	}

	async fn try_finish_build_local(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		outcome: tg::build::Outcome,
	) -> Result<bool> {
		// Verify the build is local.
		{
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
				.wrap_err("Failed to get the row.")?
				.wrap_err("Expected one row.")?;
			let exists = row.get::<_, bool>(0).wrap_err("Expected a bool.")?;
			if !exists {
				return Ok(false);
			}
		}

		// Get the children.
		let children: Vec<tg::build::Id> = {
			let db = self.inner.database.get().await?;
			let statement = "
				select json_extract(json, '$.children')
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

		// If this build was canceled, then cancel the children.
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

		// Update the database.
		{
			let status = tg::build::Status::Finished;
			let outcome = outcome.data(self).await?;
			let db = self.inner.database.get().await?;
			let statement = "
				update builds
				set json = json_set(
					json,
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
		if let Some(channels) = self.inner.channels.read().unwrap().get(id).cloned() {
			channels.outcome.send_replace(());
		}

		// Remove the build from the channels.
		self.inner.channels.write().unwrap().remove(id);

		// Remove the build from the depths.
		self.inner.depths.write().unwrap().remove(id);

		Ok(true)
	}

	async fn try_finish_build_remote(
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

		// Finish the build.
		remote.finish_build(user, id, outcome).await?;

		// Remove the build's task.
		self.inner
			.tasks
			.lock()
			.unwrap()
			.as_mut()
			.unwrap()
			.remove(id);

		Ok(true)
	}

	async fn build_is_local(&self, id: &tg::build::Id) -> Result<bool> {
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
			.wrap_err("Failed to get the row.")?
			.wrap_err("Expected one row.")?;
		let exists = row
			.get::<_, bool>(0)
			.wrap_err("Failed to deserialize the column.")?;
		Ok(exists)
	}
}
