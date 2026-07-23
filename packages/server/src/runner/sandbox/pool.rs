use {
	super::{CreateSandboxArg, CreateSandboxOutput},
	crate::Session,
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
		if !Self::matches(arg) {
			return None;
		}
		let mut tasks = self.inner.lock().unwrap();
		let task = tasks.pop_front()?;
		tasks.push_back(Self::spawn(session));

		Some(task)
	}

	pub(in crate::runner) async fn stop(&self) {
		let tasks = self.inner.lock().unwrap().drain(..).collect::<Vec<_>>();
		for task in tasks {
			task.abort();
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
		}
	}

	#[must_use]
	fn matches(arg: &tg::sandbox::create::Arg) -> bool {
		arg.cpu.is_none()
			&& arg.hostname.is_none()
			&& arg.isolation.is_none()
			&& arg.memory.is_none()
			&& arg.mounts.is_empty()
			&& arg.network.is_none()
	}

	fn spawn(session: &Session) -> Task<tg::Result<CreateSandboxOutput>> {
		Task::spawn({
			let session = session.clone();
			move |_| async move {
				session
					.create_sandbox_inner(CreateSandboxArg {
						arg: tg::sandbox::create::Arg::default(),
						expected_id: None,
					})
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

	// Stop the serve task.
	serve_task.stop();
	let serve_result = serve_task
		.wait()
		.await
		.map_err(|error| tg::error!(!error, "the pooled sandbox serve task panicked"));

	// Destroy the sandbox process.
	let sandbox_result = sandbox
		.destroy()
		.await
		.map_err(|error| tg::error!(!error, "failed to destroy the pooled sandbox process"));

	// Stop the VFS.
	#[cfg(target_os = "linux")]
	drop(vfs);

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
