use {
	crate::Component,
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_server_core::{
		Session,
		runner::pool::{CreateSandboxOutput, Pool},
	},
};

pub(crate) trait Ext {
	fn start(&self, component: &Component, session: &Session);

	fn take(
		&self,
		arg: &tg::sandbox::create::Arg,
		component: &Component,
		session: &Session,
	) -> Option<Task<tg::Result<CreateSandboxOutput>>>;
}

impl Ext for Pool {
	fn start(&self, component: &Component, session: &Session) {
		let mut tasks = self.inner.lock().unwrap();
		assert!(tasks.is_empty(), "the sandbox pool was already started");
		tasks.extend((0..self.size).map(|_| spawn(component, session)));
		if self.size > 0 {
			tracing::debug!(size = self.size, "started the sandbox pool");
		}
	}

	fn take(
		&self,
		arg: &tg::sandbox::create::Arg,
		component: &Component,
		session: &Session,
	) -> Option<Task<tg::Result<CreateSandboxOutput>>> {
		if !matches(arg, session) {
			return None;
		}
		let mut tasks = self.inner.lock().unwrap();
		let task = tasks.pop_front()?;
		tasks.push_back(spawn(component, session));

		Some(task)
	}
}

/// Check whether a sandbox request can claim a warm sandbox, which has the default resource bounds.
#[must_use]
fn matches(arg: &tg::sandbox::create::Arg, session: &Session) -> bool {
	let warm = warm_arg(session);
	arg.cpu == warm.cpu
		&& arg.hostname.is_none()
		&& arg.memory == warm.memory
		&& arg.mounts.is_empty()
		&& arg.network.is_none()
		&& arg.storage == warm.storage
}

/// Get the arg of a warm sandbox, which resolves the default resource bounds.
fn warm_arg(session: &Session) -> tg::sandbox::create::Arg {
	let mut arg = tg::sandbox::create::Arg::default();
	let defaults = session.server.config.scheduler.default_capacity();
	tangram_server_core::sandbox::resolve_sandbox_resources(&defaults, &mut arg);
	arg
}

fn spawn(component: &Component, session: &Session) -> Task<tg::Result<CreateSandboxOutput>> {
	Task::spawn({
		let component = component.clone();
		let session = session.clone();
		move |_| async move {
			let arg = warm_arg(&session);
			component
				.create_sandbox_inner(&session, arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to warm a sandbox"))
		}
	})
}
