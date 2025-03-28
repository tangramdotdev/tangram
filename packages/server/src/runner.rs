use crate::{ProcessPermit, Server, runtime};
use futures::{FutureExt as _, TryFutureExt as _, future};
use std::{pin::pin, sync::Arc, time::Duration};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_either::Either;
use tangram_futures::task::Task;
use tokio_stream::StreamExt as _;

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
							tg::Process::new(output.process, Some(name.clone()), None, None, None);
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

			// Spawn the process task.
			self.spawn_process_task(&process, permit)
				.await
				.inspect_err(|error| {
					tracing::error!(?error, "the spawn process task failed");
				})
				.ok();
		}
	}

	pub(crate) fn spawn_process_task(
		&self,
		process: &tg::Process,
		permit: ProcessPermit,
	) -> impl Future<Output = tg::Result<()>> + Send + 'static {
		let server = self.clone();
		let process = process.clone();
		async move {
			// Attempt to start the process.
			let arg = tg::process::start::Arg {
				remote: process.remote().cloned(),
			};
			let started = server
				.try_start_process(process.id(), arg.clone())
				.await?
				.started;
			if !started {
				return Ok(());
			}

			// Spawn the process task.
			server.processes.spawn(
				process.id().clone(),
				Task::spawn(|_| {
					let server = server.clone();
					let process = process.clone();
					async move { server.process_task(&process, permit).await }
						.inspect_err(|error| {
							tracing::error!(?error, "the process task failed");
						})
						.map(|_| ())
				}),
			);

			// Spawn the heartbeat task.
			tokio::spawn({
				let server = server.clone();
				let process = process.clone();
				async move { server.heartbeat_task(&process).await }
					.inspect_err(|error| {
						tracing::error!(?error, "the heartbeat task failed");
					})
					.map(|_| ())
			});

			Ok(())
		}
	}

	async fn process_task(&self, process: &tg::Process, permit: ProcessPermit) -> tg::Result<()> {
		// Set the process's permit.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.process_permits.insert(process.id().clone(), permit);
		scopeguard::defer! {
			self.process_permits.remove(process.id());
		}

		// Run.
		let output = self.process_task_inner(process).await?;

		// Determine the status.
		let status = match (&output.output, &output.error, &output.exit) {
			(_, Some(_), _) | (_, _, Some(tg::process::Exit::Signal { signal: _ })) => {
				tg::process::Status::Finished
			},
			(_, _, Some(tg::process::Exit::Code { code })) if *code != 0 => {
				tg::process::Status::Finished
			},
			_ => tg::process::Status::Finished,
		};

		// Get the output data.
		let value = match &output.output {
			Some(output) => Some(output.data(self).await?),
			None if status == tg::process::Status::Finished => Some(tg::value::Data::Null),
			None => None,
		};

		// If the process is remote, then push the output.
		if let Some(remote) = process.remote() {
			if let Some(value) = &value {
				// Push the objects.
				let objects = value.children();
				let arg = tg::push::Arg {
					items: objects.into_iter().map(Either::Right).collect(),
					remote: remote.to_owned(),
					..Default::default()
				};
				let stream = self.push(arg).await?;

				// Consume the stream and log progress.
				let mut stream = pin!(stream);
				while let Some(event) = stream.try_next().await? {
					match event {
						tg::progress::Event::Start(indicator)
						| tg::progress::Event::Update(indicator) => {
							if indicator.name == "bytes" {
								let message = format!("{indicator}\n");
								let arg = tg::process::log::post::Arg {
									bytes: message.into(),
									remote: process.remote().cloned(),
									stream: tg::process::log::Stream::Stderr,
								};
								self.try_post_process_log(process.id(), arg).await.ok();
							}
						},
						tg::progress::Event::Output(()) => {
							break;
						},
						_ => {},
					}
				}
			}
		}

		// Finish the process.
		let arg = tg::process::finish::Arg {
			error: output.error,
			exit: output.exit,
			output: value,
			remote: process.remote().cloned(),
			status,
		};
		self.try_finish_process(process.id(), arg).await?;

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
					self.processes.abort(process.id());
					break;
				}
			}
			tokio::time::sleep(config.heartbeat_interval).await;
		}
		Ok(())
	}
}
