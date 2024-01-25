use crate::{database::Json, params, BuildContext, Server};
use futures::{stream::FuturesUnordered, FutureExt, TryStreamExt};
use http_body_util::BodyExt;
use std::sync::Arc;
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};
use tangram_util::http::{full, not_found, Incoming, Outgoing};

impl Server {
	#[allow(clippy::unused_async)]
	pub async fn try_dequeue_build(
		&self,
		_user: Option<&tg::User>,
		_arg: tg::build::queue::DequeueArg,
	) -> Result<Option<tg::build::queue::DequeueOutput>> {
		Ok(None)
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
						delete from build_queue
						where rowid in (
							select rowid
							from build_queue
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
							insert into build_queue (build, options, host, depth)
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
					from build_queue;
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
			let queue_arg = tg::build::queue::DequeueArg { hosts: Some(hosts) };
			let Some(tg::build::queue::DequeueOutput { build: id, options }) = remote
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
				status: None,
				stop,
				task: std::sync::Mutex::new(None),
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
					// If the VFS is disabled, then perform an internal checkout.
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
					// If the VFS is disabled, then perform an internal checkout.
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
}

impl Server {
	pub async fn handle_dequeue_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let arg = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

		// Attempt to dequeue the build.
		let Some(output) = self.try_dequeue_build(user.as_ref(), arg).await? else {
			return Ok(not_found());
		};

		// Create the response.
		let body = serde_json::to_vec(&output).wrap_err("Failed to serialize the response.")?;
		let response = http::Response::builder().body(full(body)).unwrap();

		Ok(response)
	}
}
