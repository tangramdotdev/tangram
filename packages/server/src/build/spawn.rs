use crate::{runtime::Trait as _, BuildPermit, Server};
use either::Either;
use futures::{future, FutureExt as _, TryFutureExt as _};
use std::{pin::pin, sync::Arc};
use tangram_client as tg;
use tangram_futures::task::{Stop, Task};
use tg::Handle as _;

impl Server {
	pub(crate) async fn spawn_build(
		&self,
		build: tg::Build,
		permit: BuildPermit,
		remote: Option<tg::Client>,
	) -> tg::Result<bool> {
		let handle = match remote.clone() {
			Some(remote) => Either::Left(remote),
			None => Either::Right(self.clone()),
		};

		// Attempt to start the build.
		if !handle.start_build(build.id()).await? {
			return Ok(false);
		};

		// Spawn the build task.
		self.builds.spawn(
			build.id().clone(),
			Task::spawn(|stop| {
				let server = self.clone();
				let build = build.clone();
				async move { server.build_task(build, permit, stop).await }
					.inspect_err(|error| {
						tracing::error!(?error, "the build task failed");
					})
					.map(|_| ())
			}),
		);

		// Spawn the heartbeat task.
		tokio::spawn({
			let server = self.clone();
			let build = build.clone();
			async move { server.heartbeat_task(build, remote).await }
				.inspect_err(|error| {
					tracing::error!(?error, "the heartbeat task failed");
				})
				.map(|_| ())
		});

		Ok(true)
	}

	async fn build_task(
		&self,
		build: tg::Build,
		permit: BuildPermit,
		stop: Stop,
	) -> tg::Result<()> {
		// Set the build's permit.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.build_permits.insert(build.id().clone(), permit);

		// Build.
		let outcome = match future::select(
			pin!(self.build_task_inner(build.clone())),
			pin!(stop.stopped()),
		)
		.await
		{
			future::Either::Left((result, _)) => Some(result),
			future::Either::Right(((), _)) => None,
		};
		let outcome = outcome.map(|result| match result {
			Ok(outcome) => outcome,
			Err(error) => tg::build::Outcome::Failed(error),
		});

		// Remove the build's permit.
		self.build_permits.remove(build.id());

		// If the build was not stopped, then finish the build.
		if let Some(outcome) = outcome {
			let outcome = outcome.data(self, None).await?;
			let arg = tg::build::finish::Arg { outcome };
			build
				.finish(self, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to finish the build"))?;
		}

		Ok::<_, tg::Error>(())
	}

	async fn build_task_inner(&self, build: tg::Build) -> tg::Result<tg::build::Outcome> {
		// Get the runtime.
		let target = build.target(self).await?;
		let host = target.host(self).await?;
		let runtime = self
			.runtimes
			.read()
			.unwrap()
			.get(&*host)
			.ok_or_else(
				|| tg::error!(?id = build.id(), ?host = &*host, "failed to find a runtime for the build"),
			)?
			.clone();

		// Build.
		let result = runtime.run(&build).await;

		// Log an error if one occurred.
		if let Err(error) = &result {
			let options = &self.options.advanced.error_trace_options;
			let trace = error.trace(options);
			let log = trace.to_string();
			build.add_log(self, log.into()).await?;
		}

		// Create the outcome.
		let outcome = match result {
			Ok(value) => tg::build::Outcome::Succeeded(value),
			Err(error) => tg::build::Outcome::Failed(error),
		};

		Ok(outcome)
	}

	async fn heartbeat_task(&self, build: tg::Build, remote: Option<tg::Client>) -> tg::Result<()> {
		let handle = match remote.clone() {
			Some(remote) => Either::Left(remote),
			None => Either::Right(self.clone()),
		};
		let interval = self.options.build.as_ref().unwrap().heartbeat_interval;
		loop {
			let result = build.heartbeat(&handle).await;
			if let Ok(output) = result {
				if output.stop {
					self.builds.stop(build.id());
					break;
				}
			}
			tokio::time::sleep(interval).await;
		}
		Ok(())
	}
}
