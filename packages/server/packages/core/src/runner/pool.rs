use {
	crate::{Shutdown, temp::Temp},
	futures::{StreamExt as _, stream::FuturesUnordered},
	std::{collections::VecDeque, sync::Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

pub struct Pool {
	pub inner: Mutex<VecDeque<Task<tg::Result<CreateSandboxOutput>>>>,
	pub size: usize,
}

pub struct CreateSandboxOutput {
	pub guest_url: tangram_uri::Uri,

	pub sandbox: tangram_sandbox::Sandbox,

	pub serve_task: Task<()>,

	pub temp: Temp,

	#[cfg(target_os = "linux")]
	pub vfs: Option<Box<dyn crate::vfs::Server>>,
}

impl Pool {
	#[must_use]
	pub fn new(size: usize) -> Self {
		Self {
			inner: Mutex::new(VecDeque::with_capacity(size)),
			size,
		}
	}

	pub async fn shutdown(&self, shutdown: Shutdown) {
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
