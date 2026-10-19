use {
	super::CreateSandboxOutput,
	crate::{Session, Shutdown},
	futures::{StreamExt as _, stream::FuturesUnordered},
	std::{collections::VecDeque, sync::Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

pub(in crate::runner) struct Pool {
	inner: Mutex<VecDeque<Task<tg::Result<CreateSandboxOutput>>>>,
	size: usize,
}

impl Pool {
	#[must_use]
	pub(in crate::runner) fn new(size: usize) -> Self {
		Self {
			inner: Mutex::new(VecDeque::with_capacity(size)),
			size,
		}
	}

	pub(in crate::runner) fn start(&self, session: &Session) {
		let mut tasks = self.inner.lock().unwrap();
		assert!(tasks.is_empty(), "the sandbox pool was already started");
		tasks.extend((0..self.size).map(|_| Self::spawn(session)));
		if self.size > 0 {
			tracing::debug!(size = self.size, "started the sandbox pool");
		}
	}

	#[must_use]
	pub(super) fn take(
		&self,
		arg: &tg::sandbox::create::Arg,
		session: &Session,
	) -> Option<Task<tg::Result<CreateSandboxOutput>>> {
		if !Self::matches(arg, session) {
			return None;
		}
		let mut tasks = self.inner.lock().unwrap();
		let task = tasks.pop_front()?;
		tasks.push_back(Self::spawn(session));

		Some(task)
	}

	pub(in crate::runner) async fn shutdown(&self, shutdown: Shutdown) {
		let tasks = self.inner.lock().unwrap().drain(..).collect::<Vec<_>>();
		match shutdown {
			Shutdown::Interrupt => {},
			Shutdown::Terminate => {
				for task in &tasks {
					task.abort();
				}
			},
		}
		tasks
			.into_iter()
			.map(|task| async move {
				match task.wait().await {
					Ok(Ok(output)) => {
						if let Err(error) = destroy(output).await {
							tracing::error!(
								error = %error.trace(),
								"failed to destroy a pooled sandbox",
							);
						}
					},
					Ok(Err(error)) => {
						tracing::error!(error = %error.trace(), "failed to warm a sandbox");
					},
					Err(error) if error.is_cancelled() => {},
					Err(error) => {
						tracing::error!(?error, "a sandbox pool task panicked");
					},
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;
	}

	/// Check whether a sandbox request can claim a warm sandbox, which has the default resource bounds.
	#[must_use]
	fn matches(arg: &tg::sandbox::create::Arg, session: &Session) -> bool {
		let warm = Self::arg(session);
		arg.cpu == warm.cpu
			&& arg.hostname.is_none()
			&& arg.memory == warm.memory
			&& arg.mounts.is_empty()
			&& arg.network.is_none()
			&& arg.storage == warm.storage
	}

	/// Get the arg of a warm sandbox, which resolves the default resource bounds.
	fn arg(session: &Session) -> tg::sandbox::create::Arg {
		let mut arg = tg::sandbox::create::Arg::default();
		let defaults = session.server.config.scheduler.default_capacity();
		crate::sandbox::resolve_sandbox_resources(&defaults, &mut arg);
		arg
	}

	fn spawn(session: &Session) -> Task<tg::Result<CreateSandboxOutput>> {
		Task::spawn({
			let session = session.clone();
			move |_| async move {
				let arg = Self::arg(&session);
				session
					.create_sandbox_inner(arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to warm a sandbox"))
			}
		})
	}
}

async fn destroy(output: CreateSandboxOutput) -> tg::Result<()> {
	let CreateSandboxOutput {
		guest_url: _,
		sandbox,
		serve_task,
		mut temp,
		#[cfg(target_os = "linux")]
		vfs,
	} = output;

	// Stop the VFS while the sandbox still owns its mount namespace.
	#[cfg(target_os = "linux")]
	if let Some(vfs) = vfs {
		vfs.stop();
		vfs.wait().await;
	}

	// Destroy the sandbox process.
	let sandbox_result = sandbox
		.destroy()
		.await
		.map_err(|error| tg::error!(!error, "failed to destroy the pooled sandbox process"));

	// Stop the serve task.
	serve_task.stop();
	let serve_result = serve_task
		.wait()
		.await
		.map_err(|error| tg::error!(!error, "the pooled sandbox serve task panicked"));

	// Remove the temp directory.
	let temp_result = temp
		.remove()
		.await
		.map_err(|error| tg::error!(!error, "failed to remove the pooled sandbox temp directory"));
	serve_result?;
	sandbox_result?;
	temp_result?;

	Ok(())
}
