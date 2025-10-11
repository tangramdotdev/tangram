use {
	crate::{ProcessPermit, Server, runtime},
	futures::{FutureExt as _, TryFutureExt as _, future},
	std::{collections::BTreeSet, sync::Arc, time::Duration},
	tangram_client::{self as tg, prelude::*},
	tangram_either::Either,
};

impl Server {
	pub(crate) async fn runner_task(&self) {
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
					.map_ok(|output| tg::Process::new(output.process, None, None, None, None))
					.boxed(),
			)
			.chain(self.config.runner.iter().flat_map(|config| {
				config.remotes.iter().map(|name| {
					let server = self.clone();
					let remote = name.to_owned();
					async move {
						let client = server.get_remote_client(remote).await?;
						let arg = tg::process::dequeue::Arg::default();
						let output = client.dequeue_process(arg).await?;
						let process =
							tg::Process::new(output.process, None, Some(name.clone()), None, None);
						Ok::<_, tg::Error>(process)
					}
					.boxed()
				})
			}));
			let process = match future::select_ok(futures).await {
				Ok((process, _)) => process,
				Err(error) => {
					tracing::error!(?error, "failed to dequeue a process");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			// Attempt to start the process.
			let arg = tg::process::start::Arg {
				remote: process.remote().cloned(),
			};
			let result = self.start_process(process.id(), arg.clone()).await;
			if let Err(error) = result {
				tracing::trace!(?error, "failed to start the process");
				continue;
			}

			// Spawn the process task.
			self.spawn_process_task(&process, permit);
		}
	}

	pub(crate) fn spawn_process_task(&self, process: &tg::Process, permit: ProcessPermit) {
		// Spawn the process task.
		self.process_task_map.spawn(process.id().clone(), |_| {
			let server = self.clone();
			let process = process.clone();
			async move { server.process_task(&process, permit).await }
				.inspect_err(|error| {
					tracing::error!(?error, "the process task failed");
				})
				.map(|_| ())
		});

		// Spawn the heartbeat task.
		tokio::spawn({
			let server = self.clone();
			let process = process.clone();
			async move { server.heartbeat_task(&process).await }
				.inspect_err(|error| {
					tracing::error!(?error, "the heartbeat task failed");
				})
				.map(|_| ())
		});
	}

	async fn process_task(&self, process: &tg::Process, permit: ProcessPermit) -> tg::Result<()> {
		// Set the process's permit.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.process_permits.insert(process.id().clone(), permit);
		scopeguard::defer! {
			self.process_permits.remove(process.id());
		}

		// Run.
		let wait = self.process_task_inner(process).await?;

		// Store the output.
		let output = if let Some(output) = &wait.output {
			output.store(self).await?;
			let data = output.to_data();
			Some(data)
		} else {
			None
		};

		// If the process is remote, then push the output.
		if let Some(remote) = process.remote() {
			if let Some(output) = &output {
				let mut objects = BTreeSet::new();
				output.children(&mut objects);
				let arg = tg::push::Arg {
					items: objects.into_iter().map(Either::Right).collect(),
					remote: Some(remote.to_owned()),
					..Default::default()
				};
				let stream = self.push(arg).await?;
				self.log_progress_stream(process, stream).await?;
			}
		}

		// Finish the process.
		let arg = tg::process::finish::Arg {
			checksum: wait.checksum,
			error: wait.error.as_ref().map(tg::Error::to_data),
			exit: wait.exit,
			output,
			remote: process.remote().cloned(),
		};
		self.finish_process(process.id(), arg).await?;

		Ok::<_, tg::Error>(())
	}

	async fn process_task_inner(&self, process: &tg::Process) -> tg::Result<runtime::Output> {
		// Get the host.
		let command = process.command(self).await?;
		let host = command.host(self).await?;

		// Get the runtime.
		let runtime = self
			.runtimes
			.read()
			.unwrap()
			.get(&*host)
			.ok_or_else(
				|| tg::error!(?id = process, ?host = &*host, "failed to find a runtime for the process"),
			)?
			.clone();

		// Run the process.
		let output = runtime.run(process).await;

		Ok(output)
	}

	async fn heartbeat_task(&self, process: &tg::Process) -> tg::Result<()> {
		let config = self.config.runner.clone().unwrap_or_default();
		loop {
			let arg = tg::process::heartbeat::Arg {
				remote: process.remote().cloned(),
			};
			let result = self.heartbeat_process(process.id(), arg).await;
			if let Ok(output) = result {
				if output.status.is_finished() {
					self.process_task_map.abort(process.id());
					break;
				}
			}
			tokio::time::sleep(config.heartbeat_interval).await;
		}
		Ok(())
	}
}
