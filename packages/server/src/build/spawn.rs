use crate::{BuildPermit, Server};
use futures::{
	future, stream::FuturesUnordered, FutureExt as _, TryFutureExt as _, TryStreamExt as _,
};
use std::{sync::Arc, time::Duration};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_either::Either;
use tangram_futures::task::Task;

impl Server {
	pub(crate) async fn build_spawn_task(&self) {
		loop {
			// Wait for a permit.
			let permit = self.build_semaphore.clone().acquire_owned().await.unwrap();
			let permit = BuildPermit(Either::Left(permit));

			// Try to dequeue a build locally or from one of the remotes.
			let arg = tg::build::dequeue::Arg::default();
			let futures = std::iter::once(
				self.dequeue_build(arg)
					.map_ok(|output| (output, None))
					.boxed(),
			)
			.chain(
				self.config
					.build
					.as_ref()
					.unwrap()
					.remotes
					.iter()
					.filter_map(|name| {
						let remote = self.remotes.get(name)?.clone();
						let arg = tg::build::dequeue::Arg::default();
						let future = async move { remote.dequeue_build(arg).await }
							.map_ok(|output| (output, Some(name.clone())))
							.boxed();
						Some(future)
					}),
			);
			let (output, remote) = match future::select_ok(futures).await {
				Ok(((output, remote), _)) => (output, remote),
				Err(error) => {
					tracing::error!(?error, "failed to dequeue a build");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			// Spawn the build.
			let build = tg::Build::with_id(output.build);
			self.spawn_build(build.clone(), permit, remote).await.ok();
		}
	}

	pub(crate) async fn spawn_build(
		&self,
		build: tg::Build,
		permit: BuildPermit,
		remote: Option<String>,
	) -> tg::Result<()> {
		// Attempt to start the build.
		let arg = tg::build::start::Arg {
			remote: remote.clone(),
		};
		let started = self.try_start_build(build.id(), arg).await?;
		if !started {
			return Ok(());
		}

		// Spawn the build task.
		self.builds.spawn(
			build.id().clone(),
			Task::spawn(|_| {
				let server = self.clone();
				let build = build.clone();
				let remote = remote.clone();
				async move { server.build_task(build, permit, remote).await }
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
			let remote = remote.clone();
			async move { server.heartbeat_task(build, remote).await }
				.inspect_err(|error| {
					tracing::error!(?error, "the heartbeat task failed");
				})
				.map(|_| ())
		});

		Ok(())
	}

	async fn build_task(
		&self,
		build: tg::Build,
		permit: BuildPermit,
		remote: Option<String>,
	) -> tg::Result<()> {
		// Set the build's permit.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.build_permits.insert(build.id().clone(), permit);
		scopeguard::defer! {
			self.build_permits.remove(build.id());
		}

		// Build.
		let result = self.build_task_inner(build.clone(), remote.clone()).await;
		let outcome = match result {
			Ok(outcome) => outcome,
			Err(error) => tg::build::Outcome::Failure(tg::build::outcome::Failure { error }),
		};
		let outcome = outcome.data(self).await?;

		// Push the output if the build is remote.
		if let Some(remote) = remote.clone() {
			if let tg::build::outcome::Data::Success(success) = &outcome {
				let arg = tg::object::push::Arg { remote };
				tg::Value::try_from(success.value.clone())?
					.objects()
					.iter()
					.map(|object| object.push(self, arg.clone()))
					.collect::<FuturesUnordered<_>>()
					.try_collect::<Vec<_>>()
					.await?;
			}
		}

		// Finish the build.
		let arg = tg::build::finish::Arg {
			outcome,
			remote: remote.clone(),
		};
		build
			.finish(self, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to finish the build"))?;

		Ok::<_, tg::Error>(())
	}

	async fn build_task_inner(
		&self,
		build: tg::Build,
		remote: Option<String>,
	) -> tg::Result<tg::build::Outcome> {
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
		let result = runtime.build(&build, remote.clone()).await;

		// Log an error if one occurred.
		if let Err(error) = &result {
			let options = &self.config.advanced.error_trace_options;
			let trace = error.trace(options);
			let bytes = trace.to_string().into();
			let arg = tg::build::log::post::Arg {
				bytes,
				remote: remote.clone(),
			};
			build.add_log(self, arg).await?;
		}

		// Create the outcome.
		let outcome = match result {
			Ok(value) => tg::build::Outcome::Success(tg::build::outcome::Success { value }),
			Err(error) => tg::build::Outcome::Failure(tg::build::outcome::Failure { error }),
		};

		Ok(outcome)
	}

	async fn heartbeat_task(&self, build: tg::Build, remote: Option<String>) -> tg::Result<()> {
		let interval = self.config.build.as_ref().unwrap().heartbeat_interval;
		loop {
			let arg = tg::build::heartbeat::Arg {
				remote: remote.clone(),
			};
			let result = build.heartbeat(self, arg).await;
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
