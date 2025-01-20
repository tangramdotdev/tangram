use crate::{BuildPermit, Server};
use futures::{
	future, stream::FuturesUnordered, Future, FutureExt as _, TryFutureExt as _, TryStreamExt as _,
};
use std::{sync::Arc, time::Duration};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_either::Either;
use tangram_futures::task::Task;

impl Server {
	pub(crate) async fn process_spawn_task(&self) {
		loop {
			// Wait for a permit.
			let permit = self
				.process_semaphore
				.clone()
				.acquire_owned()
				.await
				.unwrap();
			let permit = BuildPermit(Either::Left(permit));

			// Try to dequeue a process locally or from one of the remotes.
			let arg = tg::process::dequeue::Arg::default();
			let futures = std::iter::once(
				self.dequeue_process(arg)
					.map_ok(|output| (output, None))
					.boxed(),
			)
			.chain(
				self.config
					.process
					.as_ref()
					.unwrap()
					.remotes
					.iter()
					.map(|name| {
						let server = self.clone();
						let remote = name.to_owned();
						async move {
							let client = server.get_remote_client(remote).await?;
							let arg = tg::process::dequeue::Arg::default();
							let process = client.dequeue_process(arg).await?;
							let output = (process, Some(name.to_owned()));
							Ok::<_, tg::Error>(output)
						}
						.boxed()
					}),
			);
			let (output, remote) = match future::select_ok(futures).await {
				Ok(((output, remote), _)) => (output, remote),
				Err(error) => {
					tracing::error!(?error, "failed to dequeue a process");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			// Spawn the process.
			self.spawn_process(output.process, permit, remote)
				.await
				.ok();
		}
	}

	pub(crate) fn spawn_process(
		&self,
		process: tg::process::Id,
		permit: BuildPermit,
		remote: Option<String>,
	) -> impl Future<Output = tg::Result<()>> + Send + 'static {
		let server = self.clone();
		async move {
			// Attempt to start the process.
			let arg = tg::process::start::Arg {
				remote: remote.clone(),
			};
			let started = server.try_start_process(&process, arg).await?.started;
			if !started {
				return Ok(());
			}

			// Spawn the process task.
			server.processes.spawn(
				process.clone(),
				Task::spawn(|_| {
					let server = server.clone();
					let process = process.clone();
					let remote = remote.clone();
					async move { server.process_task(process, permit, remote).await }
						.inspect_err(|error| {
							tracing::error!(?error, "the process task failed");
						})
						.map(|_| ())
				}),
			);

			// Spawn the heartbeat task.
			tokio::spawn({
				let server = server.clone();
				let remote = remote.clone();
				async move { server.heartbeat_task(process, remote).await }
					.inspect_err(|error| {
						tracing::error!(?error, "the heartbeat task failed");
					})
					.map(|_| ())
			});

			Ok(())
		}
	}

	async fn process_task(
		&self,
		process: tg::process::Id,
		permit: BuildPermit,
		remote: Option<String>,
	) -> tg::Result<()> {
		// Set the process's permit.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.process_permits.insert(process.clone(), permit);
		scopeguard::defer! {
			self.process_permits.remove(&process);
		}

		// Build.
		let result = self
			.process_task_inner(process.clone(), remote.clone())
			.await;
		let (output, status, error) = match result {
			Ok(output) => (Some(output), tg::process::Status::Succeeded, None),
			Err(error) => (None, tg::process::Status::Failed, Some(error)),
		};

		// Push the output if the process is remote.
		if let Some(remote) = remote.clone() {
			if status.is_succeeded() {
				let value = output.clone().unwrap();
				let arg = tg::object::push::Arg { remote };
				value
					.objects()
					.iter()
					.map(|object| object.push(self, arg.clone()))
					.collect::<FuturesUnordered<_>>()
					.try_collect::<Vec<_>>()
					.await?;
			}
		}

		// Get the output data.
		let output = match &output {
			Some(output) => Some(output.data(self).await?),
			None => None,
		};

		// Finish the process.
		self.try_finish_process_local(&process, error, output, status).await?;

		Ok::<_, tg::Error>(())
	}

	async fn process_task_inner(
		&self,
		process: tg::process::Id,
		remote: Option<String>,
	) -> tg::Result<tg::Value> {
		// Get the runtime.
		let command = self
			.try_get_process_local(&process)
			.await?
			.ok_or_else(|| tg::error!("expected a local process"))?
			.command;
		let command = tg::Command::with_id(command);
		let host = command.host(self).await?;
		let runtime = self
			.runtimes
			.read()
			.unwrap()
			.get(&*host)
			.ok_or_else(
				|| tg::error!(?id = process, ?host = &*host, "failed to find a runtime for the process"),
			)?
			.clone();

		// Build.
		let result = runtime.spawn(&process, &command, remote.clone()).await;

		// Log an error if one occurred.
		if let Err(error) = &result {
			let options = &self.config.advanced.error_trace_options;
			let trace = error.trace(options);
			let bytes = trace.to_string().into();
			let arg = tg::process::log::post::Arg {
				bytes,
				remote: remote.clone(),
			};
			if !self.try_add_process_log(&process, arg).await?.added {
				return Err(tg::error!("failed to add error to process log"));
			}
		}

		result
	}

	async fn heartbeat_task(&self, process: tg::process::Id, remote: Option<String>) -> tg::Result<()> {
		let interval = self.config.process.as_ref().unwrap().heartbeat_interval;
		loop {
			let arg = tg::process::heartbeat::Arg {
				remote: remote.clone(),
			};
			let result = self.heartbeat_process(&process, arg).await;
			if let Ok(output) = result {
				if output.status.is_finished() {
					self.processes.abort(&process);
					break;
				}
			}
			tokio::time::sleep(interval).await;
		}
		Ok(())
	}
}
