use super::Server;
use crate::{database::Database, postgres_params, sqlite_params, Permit};
use either::Either;
use futures::{future, stream::FuturesUnordered, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use std::pin::pin;
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};

mod children;
mod create;
mod get;
mod list;
mod log;
mod outcome;
mod pull;
mod push;
mod put;
mod status;

impl Server {
	pub(crate) async fn build_queue_task(
		&self,
		mut stop: tokio::sync::watch::Receiver<bool>,
	) -> Result<()> {
		let task = self.build_queue_task_inner();
		let stop = stop.wait_for(|stop| *stop);
		future::select(pin!(task), pin!(stop))
			.map(|output| match output {
				future::Either::Left((Err(error), _)) => Err(error),
				_ => Ok(()),
			})
			.await?;
		Ok(())
	}

	async fn build_queue_task_inner(&self) -> Result<()> {
		loop {
			// If the messenger is nats, then wait for a permit.
			let permit = match self.inner.messenger.kind() {
				crate::messenger::Kind::Channel => None,
				crate::messenger::Kind::Nats => {
					let Ok(permit) = self.inner.build_semaphore.clone().acquire_owned().await
					else {
						return Ok(());
					};
					Some(permit)
				},
			};

			// Wait for a message that a build was created.
			self.inner
				.messenger
				.subscribe_to_build_created()
				.await?
				.next()
				.await;

			// Attempt to get a build.
			let arg = tg::build::ListArg {
				limit: Some(1),
				order: Some(tg::build::Order::CreatedAt),
				status: Some(tg::build::Status::Created),
				target: None,
			};
			let output = self.list_builds(arg).await?;
			let Some(build) = output.items.first() else {
				continue;
			};
			let id = build.id.clone();

			let task = tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				async move {
					// Update the status to queued.
					server
						.set_build_status(None, &id, tg::build::Status::Queued)
						.await?;

					// If the build does not have a permit, then wait for one, either from the semaphore or one of the build's parents.
					let permit = if let Some(permit) = permit {
						Permit(Either::Left(permit))
					} else {
						let semaphore = server
							.inner
							.build_semaphore
							.clone()
							.acquire_owned()
							.map(|result| Permit(Either::Left(result.unwrap())));

						let parent = server.try_get_build_parent(&id).await?;
						let state = parent.and_then(|parent| {
							server
								.inner
								.build_state
								.read()
								.unwrap()
								.get(&parent)
								.cloned()
						});
						let parent = if let Some(state) = state {
							state
								.permit
								.clone()
								.lock_owned()
								.map(|guard| Permit(Either::Right(guard)))
								.left_future()
						} else {
							future::pending().right_future()
						};

						match future::select(pin!(semaphore), pin!(parent)).await {
							future::Either::Left((permit, _))
							| future::Either::Right((permit, _)) => permit,
						}
					};

					// Set the permit in the build state.
					let state = server
						.inner
						.build_state
						.write()
						.unwrap()
						.get_mut(&id)
						.unwrap()
						.clone();
					state.permit.lock().await.replace(permit);

					// Update the status to started.
					server
						.set_build_status(None, &id, tg::build::Status::Started)
						.await?;

					let build = tg::Build::with_id(id.clone());
					let target = build.target(&server).await?;

					// Get the stop receiver.
					let stop = server
						.inner
						.build_state
						.read()
						.unwrap()
						.get(&id)
						.unwrap()
						.stop
						.subscribe();

					// Build the target with the appropriate runtime.
					let result = match target.host(&server).await?.os() {
						tg::system::Os::Js => {
							// Build the target on the server's local pool because it is a `!Send` future.
							tangram_runtime::js::build(&server, &build, stop).await
						},
						tg::system::Os::Darwin => {
							#[cfg(target_os = "macos")]
							{
								// If the VFS is disabled, then perform an internal checkout.
								if server.inner.vfs.lock().unwrap().is_none() {
									target
										.data(&server)
										.await?
										.children()
										.into_iter()
										.filter_map(|id| id.try_into().ok())
										.map(|id| {
											let server = server.clone();
											async move {
												let artifact = tg::Artifact::with_id(id);
												artifact.check_out(&server, None).await
											}
										})
										.collect::<FuturesUnordered<_>>()
										.try_collect::<Vec<_>>()
										.await?;
								}
								tangram_runtime::darwin::build(
									&server,
									&build,
									stop,
									&server.inner.path,
								)
								.await
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
								if server.inner.vfs.lock().unwrap().is_none() {
									target
										.data(&server)
										.await?
										.children()
										.into_iter()
										.filter_map(|id| id.try_into().ok())
										.map(|id| {
											let server = server.clone();
											async move {
												let artifact = tg::Artifact::with_id(id);
												artifact.check_out(&server, None).await
											}
										})
										.collect::<FuturesUnordered<_>>()
										.try_collect::<Vec<_>>()
										.await?;
								}
								tangram_runtime::linux::build(
									&server,
									&build,
									stop,
									&server.inner.path,
								)
								.await
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
							.add_log(&server, error.trace().to_string().into())
							.await?;
					}

					// Set the outcome.
					build.set_outcome(&server, None, outcome).await?;

					// Remove the build state.
					server.inner.build_state.write().unwrap().remove(&id);

					Ok::<_, Error>(())
				}
				.inspect_err(|error| {
					let trace = error.trace();
					tracing::error!(%trace, "The build task failed.");
				})
				.map(|_| ())
			});

			// Set the task in the build state.
			self.inner
				.build_state
				.write()
				.unwrap()
				.get_mut(&id)
				.unwrap()
				.task
				.lock()
				.unwrap()
				.replace(task);
		}
	}

	async fn try_get_build_parent(&self, id: &tg::build::Id) -> Result<Option<tg::build::Id>> {
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					select build
					from build_children
					where child = ?1
					limit 1;
				";
				let id = id.to_string();
				let params = sqlite_params![id];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the statement.")?;
				let mut rows = statement
					.query(params)
					.wrap_err("Failed to execute the statement.")?;
				let row = rows.next().wrap_err("Failed to get the row.")?;
				let id = row
					.map(|row| {
						row.get::<_, String>(0)
							.wrap_err("Failed to deserialize the column.")
							.and_then(|id| id.parse().wrap_err("Failed to deserialize the column."))
					})
					.transpose()?;
				Ok(id)
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let statement = "
					select build
					from build_children
					where child = $1;
					limit 1;
				";
				let id = id.to_string();
				let params = postgres_params![id];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the statement.")?;
				let rows = connection
					.query(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
				let row = rows.first();
				let id = row
					.map(|row| {
						row.try_get::<_, String>(0)
							.wrap_err("Failed to deserilaize the column.")
							.and_then(|id| id.parse().wrap_err("Failed to deserialize the column."))
					})
					.transpose()?;
				Ok(id)
			},
		}
	}

	pub(crate) async fn get_build_exists_local(&self, id: &tg::build::Id) -> Result<bool> {
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					select count(*) != 0
					from builds
					where id = ?1;
				";
				let params = sqlite_params![id.to_string()];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				let mut rows = statement
					.query(params)
					.wrap_err("Failed to execute the statement.")?;
				let row = rows
					.next()
					.wrap_err("Failed to retrieve the row.")?
					.wrap_err("Expected a row.")?;
				let exists = row
					.get::<_, bool>(0)
					.wrap_err("Failed to deserialize the column.")?;
				Ok(exists)
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let statement = "
					select count(*) != 0
					from builds
					where id = $1;
				";
				let params = postgres_params![id.to_string()];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the query.")?;
				let row = connection
					.query_one(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
				let exists = row
					.try_get::<_, bool>(0)
					.wrap_err("Failed to deserialize the column.")?;
				Ok(exists)
			},
		}
	}
}
