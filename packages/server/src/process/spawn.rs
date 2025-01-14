use crate::{runtime, ProcessPermit, Server};
use futures::{future, Future, FutureExt as _, TryFutureExt as _};
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
			let permit = ProcessPermit(Either::Left(permit));

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
		permit: ProcessPermit,
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
		permit: ProcessPermit,
		remote: Option<String>,
	) -> tg::Result<()> {
		// Set the process's permit.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.process_permits.insert(process.clone(), permit);
		scopeguard::defer! {
			self.process_permits.remove(&process);
		}

		// Run.
		let output = self
			.process_task_inner(process.clone(), remote.clone())
			.await?;

		// Compute the status.
		let status = match (&output.value, &output.exit, &output.error) {
			(_, _, Some(_)) | (_, Some(tg::process::Exit::Signal { signal: _ }), _) => {
				tg::process::Status::Failed
			},
			(_, Some(tg::process::Exit::Code { code }), _) if *code != 0 => {
				tg::process::Status::Failed
			},
			_ => tg::process::Status::Succeeded,
		};

		// Get the output data.
		let value = match &output.value {
			Some(output) => Some(output.data(self).await?),
			// If the process succeeded but had no output, mark it as having the value "null"
			None if status == tg::process::Status::Succeeded => Some(tg::value::Data::Null),
			None => None,
		};

		// Finish the process.
		self.try_finish_process_local(&process, output.error, value, output.exit, status)
			.await?;

		Ok::<_, tg::Error>(())
	}

	async fn process_task_inner(
		&self,
		process: tg::process::Id,
		remote: Option<String>,
	) -> tg::Result<runtime::Output> {
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
		Ok(runtime.run(&process, &command, remote.as_ref()).await)
	}

	async fn heartbeat_task(
		&self,
		process: tg::process::Id,
		remote: Option<String>,
	) -> tg::Result<()> {
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
