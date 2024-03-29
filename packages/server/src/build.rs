use super::Server;
use crate::Permit;
use either::Either;
use futures::{future, FutureExt, StreamExt, TryFutureExt};
use indoc::formatdoc;
use std::pin::pin;
use tangram_client as tg;
use tangram_database as db;
use tangram_error::{error, Error, Result};

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
		// Subscribe to messages that builds were created.
		let mut subscription = self.inner.messenger.subscribe_to_build_created().await?;

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
			subscription.next().await;

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

			// Update the build's status to queued. If the update fails, then continue.
			let result = self
				.set_build_status(None, &id, tg::build::Status::Queued)
				.await;
			if result.is_err() {
				continue;
			}

			let task = tokio::spawn({
				let server = self.clone();
				let build = tg::Build::with_id(build.id.clone());
				let mut stop = self
					.inner
					.build_state
					.read()
					.unwrap()
					.get(build.id())
					.unwrap()
					.stop
					.subscribe();
				async move {
					let outcome = server.start_build(build.clone(), permit);
					let stop = stop.wait_for(|b| *b);
					let outcome = match future::select(pin!(outcome), pin!(stop)).await {
						future::Either::Left((outcome, _)) => outcome?,
						future::Either::Right(_) => tg::build::Outcome::Canceled,
					};
					build.set_outcome(&server, None, outcome).await?;
					server.inner.build_state.write().unwrap().remove(build.id());
					Ok::<_, Error>(())
				}
				.inspect_err(|error| {
					tracing::error!(?error, "failed to run the build");
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

	async fn start_build(
		&self,
		build: tg::Build,
		permit: Option<tokio::sync::OwnedSemaphorePermit>,
	) -> Result<tg::build::Outcome> {
		let id = build.id();

		// If the build does not have a permit, then wait for one, either from the semaphore or one of the build's parents. We must handle the stop signal here to ensure the task isn't blocked waiting for a permit when it is stopped.
		let permit = if let Some(permit) = permit {
			Permit(Either::Left(permit))
		} else {
			let semaphore = self
				.inner
				.build_semaphore
				.clone()
				.acquire_owned()
				.map(|result| Permit(Either::Left(result.unwrap())));
			let parent = self.try_get_build_parent(id).await?;
			let state = parent
				.and_then(|parent| self.inner.build_state.read().unwrap().get(&parent).cloned());
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
				future::Either::Left((permit, _)) | future::Either::Right((permit, _)) => permit,
			}
		};

		// Set the permit in the build state.
		let state = self
			.inner
			.build_state
			.write()
			.unwrap()
			.get_mut(id)
			.unwrap()
			.clone();
		state.permit.lock().await.replace(permit);

		// Update the build's status to started.
		self.set_build_status(None, id, tg::build::Status::Started)
			.await?;

		let build = tg::Build::with_id(id.clone());
		let target = build.target(self).await?;

		// Build the target with the appropriate runtime.
		let host = target.host(self).await?;
		let runtime = self
			.inner
			.runtimes
			.read()
			.unwrap()
			.get(host)
			.ok_or_else(|| error!(?id, ?host, "no runtime to build the target"))?
			.clone_box();
		let result = runtime.run(&build).await;

		// Log the error.
		if let Err(error) = &result {
			let options = &self.inner.options.advanced.error_trace_options;
			let trace = error.trace(options);
			let log = trace.to_string().into();
			build.add_log(self, log).await?;
		}

		// Create the outcome.
		let outcome = match result {
			Ok(value) => tg::build::Outcome::Succeeded(value),
			Err(error) => tg::build::Outcome::Failed(error),
		};

		Ok(outcome)
	}

	async fn try_get_build_parent(&self, id: &tg::build::Id) -> Result<Option<tg::build::Id>> {
		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

		// Get the parent.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select build
				from build_children
				where child = {p}1
				limit 1;
			"
		);
		let id = id.to_string();
		let params = db::params![id];
		let parent = connection
			.query_optional_scalar_into(statement, params)
			.await
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(parent)
	}

	pub(crate) async fn get_build_exists_local(&self, id: &tg::build::Id) -> Result<bool> {
		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

		// Check if the build exists.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count(*) != 0
				from builds
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let exists = connection
			.query_one_scalar_into(statement, params)
			.await
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(exists)
	}
}
