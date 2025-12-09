use {
	crate::{ProcessPermit, Server},
	futures::{FutureExt as _, StreamExt, TryFutureExt as _, future, stream::FuturesUnordered},
	std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration},
	tangram_client::prelude::*,
	tangram_either::Either,
};

mod common;
mod progress;
pub mod util;

pub mod builtin;
#[cfg(target_os = "macos")]
pub mod darwin;
#[cfg(feature = "js")]
pub mod js;
#[cfg(target_os = "linux")]
pub mod linux;

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	#[expect(clippy::struct_field_names)]
	pub output: Option<tg::Value>,
}

impl Server {
	pub(crate) async fn runner_task(&self) {
		if self.config().advanced.shared_process {
			let result = self.expire_unfinished_processes().await;
			if let Err(error) = result {
				tracing::error!(?error, "failed to expire unfinished processes");
			}
		}

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
		self.process_tasks.spawn(process.id().clone(), |_| {
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
		let wait = self.run(process).await?;

		// Store the output.
		let output = if let Some(output) = &wait.output {
			output.store(self).await?;
			let data = output.to_data();
			Some(data)
		} else {
			None
		};

		// If the process is remote, then push the output.
		if let Some(remote) = process.remote()
			&& let Some(output) = &output
		{
			let mut objects = BTreeSet::new();
			output.children(&mut objects);
			let arg = tg::push::Arg {
				items: objects.into_iter().map(Either::Left).collect(),
				remote: Some(remote.to_owned()),
				..Default::default()
			};
			let stream = self.push(arg).await?;
			self.log_progress_stream(process, stream).await?;
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

	async fn heartbeat_task(&self, process: &tg::Process) -> tg::Result<()> {
		let config = self.config.runner.clone().unwrap_or_default();
		loop {
			let arg = tg::process::heartbeat::Arg {
				remote: process.remote().cloned(),
			};
			let result = self.heartbeat_process(process.id(), arg).await;
			if let Ok(output) = result
				&& output.status.is_finished()
			{
				self.process_tasks.abort(process.id());
				break;
			}
			tokio::time::sleep(config.heartbeat_interval).await;
		}
		Ok(())
	}

	async fn run(&self, process: &tg::Process) -> tg::Result<Output> {
		// Ensure the process is loaded.
		let state = process.load(self).await.map_err(
			|source| tg::error!(!source, process = %process.id(), "failed to load the process"),
		)?;

		// Get the host.
		let command = process.command(self).await?;
		let host = command.host(self).await?;

		// Determine if the root is mounted.
		let root_mounted = state
			.mounts
			.iter()
			.any(|mount| mount.source == mount.target && mount.target == Path::new("/"));

		// Get the server's user.
		let whoami =
			util::whoami().map_err(|error| tg::error!(!error, "failed to get username"))?;

		// Determine if the process is unsandboxed.
		let unsandboxed = root_mounted
			&& (command.mounts(self).await?.is_empty() && state.mounts.len() == 1)
			&& state.network
			&& command
				.user(self)
				.await?
				.as_ref()
				.is_none_or(|user| user == &whoami);
		let sandboxed = !unsandboxed;

		let result = {
			match host.as_str() {
				"builtin" if sandboxed => self.run_builtin(process).await,

				#[cfg(feature = "js")]
				"js" if sandboxed => self.run_js(process).await,

				#[cfg(target_os = "macos")]
				"builtin" => self.run_darwin(process).await,

				#[cfg(target_os = "linux")]
				"builtin" => self.run_linux(process).await,

				#[cfg(all(feature = "js", target_os = "macos"))]
				"js" => self.run_darwin(process).await,

				#[cfg(all(feature = "js", target_os = "linux"))]
				"js" => self.run_linux(process).await,

				#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
				"aarch64-darwin" => self.run_darwin(process).await,

				#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
				"x86_64-darwin" => self.run_darwin(process).await,

				#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
				"aarch64-linux" => self.run_linux(process).await,

				#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
				"x86_64-linux" => self.run_linux(process).await,

				_ => Err(tg::error!(%host, "cannot run process with host")),
			}
		};

		let mut output = match result {
			Ok(output) => output,
			Err(error) => Output {
				checksum: None,
				error: Some(error),
				exit: 1,
				output: None,
			},
		};

		// Compute the checksum if necessary.
		if let (Some(checksum), None, Some(value)) =
			(&state.expected_checksum, &output.checksum, &output.output)
		{
			let algorithm = checksum.algorithm();
			let checksum = self.compute_checksum(value, algorithm).await?;
			output.checksum = Some(checksum);
		}

		Ok(output)
	}

	async fn compute_checksum(
		&self,
		value: &tg::Value,
		algorithm: tg::checksum::Algorithm,
	) -> tg::Result<tg::Checksum> {
		if let Ok(blob) = value.clone().try_into() {
			self.checksum_blob(&blob, algorithm).await
		} else if let Ok(artifact) = value.clone().try_into() {
			self.checksum_artifact(&artifact, algorithm).await
		} else {
			Err(tg::error!(
				"cannot checksum a value that is not a blob or an artifact"
			))
		}
	}

	async fn expire_unfinished_processes(&self) -> tg::Result<()> {
		let output = self
			.list_processes(tg::process::list::Arg::default())
			.await
			.map_err(|source| tg::error!(!source, "failed to list processes"))?;
		output
			.data
			.into_iter()
			.filter_map(|output| {
				if matches!(output.data.status, tg::process::Status::Finished) {
					return None;
				}
				let server = self.clone();
				Some(async move {
					let error = tg::Error {
						code: Some(tg::error::Code::HeartbeatExpiration),
						message: Some("heartbeat expired".into()),
						..tg::Error::default()
					};
					let error = Some(error.to_data());
					let arg = tg::process::finish::Arg {
						checksum: None,
						error,
						exit: 1,
						output: None,
						remote: None,
					};
					if let Err(error) = server.finish_process(&output.id, arg).await {
						tracing::error!(process = %output.id, ?error, "failed to finish the process");
					}
				})
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;
		Ok(())
	}
}
