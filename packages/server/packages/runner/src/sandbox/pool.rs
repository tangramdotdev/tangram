use {
	crate::Component,
	futures::{StreamExt as _, stream::FuturesUnordered},
	std::{collections::VecDeque, sync::Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_server_core::{Session, Shutdown, temp::Temp},
};

pub struct Pool {
	pub inner: Mutex<Option<Queue>>,
	pub size: usize,
}

pub struct Queue {
	key: Key,
	tasks: VecDeque<Task<tg::Result<CreateSandboxOutput>>>,
}

/// The settings fixed when a sandbox is warmed; `None` is configurable and `Some(None)` requires an absent request value.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
	clippy::option_option,
	reason = "the outer option marks configurable settings and the inner option preserves absent request values"
)]
pub struct Key {
	cpu: Option<Option<tg::sandbox::Cpu>>,
	host_network: bool,
	hostname: Option<Option<String>>,
	memory: Option<Option<tg::sandbox::Resource<u64>>>,
	mounts: Option<Vec<tg::sandbox::Mount>>,
	network: Option<Option<tg::sandbox::Network>>,
	storage: Option<Option<tg::sandbox::Resource<u64>>>,
}

pub struct CreateSandboxOutput {
	pub guest_url: tangram_uri::Uri,

	pub sandbox: tangram_sandbox::Sandbox,

	pub serve_task: Task<()>,

	pub temp: Temp,

	pub vfs: Option<Box<dyn tangram_server_vfs_core::Server>>,
}

pub(super) async fn destroy(output: CreateSandboxOutput) -> tg::Result<()> {
	let CreateSandboxOutput {
		guest_url: _,
		sandbox,
		serve_task,
		mut temp,
		vfs,
	} = output;

	// Stop the VFS while the sandbox still owns its mount namespace.
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

impl Pool {
	#[must_use]
	pub fn new(size: usize) -> Self {
		Self {
			inner: Mutex::new(None),
			size,
		}
	}

	pub async fn shutdown(&self, shutdown: Shutdown) {
		let tasks = self
			.inner
			.lock()
			.unwrap()
			.take()
			.map(|queue| queue.tasks)
			.unwrap_or_default();
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

	pub(crate) fn start(&self, component: &Component, session: &Session) {
		let mut queue = self.inner.lock().unwrap();
		assert!(queue.is_none(), "the sandbox pool was already started");
		let Ok(backend) = component.resolve_sandbox_backend(&session.server) else {
			return;
		};
		let arg = warm_arg(session);
		let key = Key::new(
			&arg,
			&backend,
			session.server.config.runner.backend.storage_limits(),
		);
		let tasks = (0..self.size).map(|_| spawn(component, session)).collect();
		*queue = Some(Queue { key, tasks });
		if self.size > 0 {
			tracing::debug!(size = self.size, "started the sandbox pool");
		}
	}

	pub(crate) fn take(
		&self,
		arg: &tg::sandbox::create::Arg,
		component: &Component,
		session: &Session,
	) -> Option<Task<tg::Result<CreateSandboxOutput>>> {
		let backend = component.resolve_sandbox_backend(&session.server).ok()?;
		let key = Key::new(
			arg,
			&backend,
			session.server.config.runner.backend.storage_limits(),
		);
		let mut queue = self.inner.lock().unwrap();
		let queue = queue.as_mut().filter(|queue| queue.key == key)?;
		let task = queue.tasks.pop_front()?;
		queue.tasks.push_back(spawn(component, session));

		Some(task)
	}
}

impl Key {
	#[must_use]
	fn new(
		arg: &tg::sandbox::create::Arg,
		backend: &tangram_sandbox::Backend,
		storage_limits: bool,
	) -> Self {
		let native = matches!(
			backend,
			tangram_sandbox::Backend::Builtin | tangram_sandbox::Backend::Bubblewrap { .. }
		);
		Self {
			cpu: (!native).then_some(arg.cpu),
			host_network: matches!(arg.network.as_ref(), Some(tg::sandbox::Network::Host)),
			hostname: (!native).then(|| arg.hostname.clone()),
			memory: (!native).then_some(arg.memory),
			mounts: (!native).then(|| arg.mounts.clone()),
			network: (!native).then(|| arg.network.clone()),
			storage: (!native && storage_limits).then_some(arg.storage),
		}
	}
}

/// Get the default resource maxima for a warm sandbox without reserving a container workload's minima.
fn warm_arg(session: &Session) -> tg::sandbox::create::Arg {
	let mut arg = tg::sandbox::create::Arg::default();
	let defaults = session.server.config.scheduler.default_capacity();
	tangram_server_sandbox_common::resolve_sandbox_resources(&defaults, &mut arg);
	let native = matches!(
		session.server.config.runner.backend,
		tangram_server_core::config::RunnerBackend::Builtin(_)
			| tangram_server_core::config::RunnerBackend::Bubblewrap(_)
	);
	if native {
		if let Some(cpu) = &mut arg.cpu {
			cpu.min = tg::sandbox::Amount::default();
		}
		if let Some(memory) = &mut arg.memory {
			memory.min = 0;
		}
	}
	if native && let Some(storage) = &mut arg.storage {
		storage.min = 0;
	}

	arg
}

fn spawn(component: &Component, session: &Session) -> Task<tg::Result<CreateSandboxOutput>> {
	Task::spawn({
		let component = component.clone();
		let session = session.clone();
		move |_| async move {
			let arg = warm_arg(&session);
			component
				.create_sandbox_inner(&session, arg, None)
				.await
				.map_err(|error| tg::error!(!error, "failed to warm a sandbox"))
		}
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn native_requests_share_a_queue_except_for_host_networking() {
		let backend = tangram_sandbox::Backend::Builtin;
		let warm = tg::sandbox::create::Arg::default();
		let mut request = warm.clone();
		request.cpu = Some(2.into());
		request.hostname = Some("configured".into());
		request.memory = Some(268_435_456.into());
		let mount = tg::sandbox::Mount {
			readonly: true,
			source: "/source".into(),
			target: "/target".into(),
		};
		request.mounts.push(mount);
		request.network = Some(tg::sandbox::Network::Default);
		request.storage = Some(134_217_728.into());
		let key = Key::new(&warm, &backend, true);
		assert_eq!(key, Key::new(&request, &backend, true));
		request.network = Some(tg::sandbox::Network::Host);
		assert_ne!(key, Key::new(&request, &backend, true));
	}

	#[test]
	fn gvisor_keeps_its_guest_configuration_fixed() {
		let backend = tangram_sandbox::Backend::Gvisor {
			path: "runsc".into(),
			platform: tangram_sandbox::Platform::Systrap,
		};
		let warm = tg::sandbox::create::Arg::default();
		let key = Key::new(&warm, &backend, true);
		let mut request = warm.clone();
		request.cpu = Some(2.into());
		request.memory = Some(268_435_456.into());
		assert_ne!(key, Key::new(&request, &backend, true));
		request.cpu = warm.cpu;
		request.memory = warm.memory;
		request.network = Some(tg::sandbox::Network::Default);
		assert_ne!(key, Key::new(&request, &backend, true));
		request.network = None;
		request.hostname = Some("configured".into());
		assert_ne!(key, Key::new(&request, &backend, true));
	}
}
