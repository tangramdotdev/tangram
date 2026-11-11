use {
	crate::Component,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_server_core::{Session, runner::Runner},
};

impl Component {
	pub(crate) async fn authorize_sandbox_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		tokens: &[tg::authorization::Token],
		permission: tg::authorization::permission::sandbox::Permission,
	) -> tg::Result<bool> {
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let permission = tg::authorization::Permission::Sandbox(permission);
		// Never use the sandbox's own authorization tokens to authorize a caller.
		let permissions = session
			.authorize(resource, permission)
			.await?
			.check_exhaustion()?;
		Ok(permissions.permissions.contains(permission))
	}

	/// Get this server's runner's state of a sandbox if the runner holds the sandbox's boot. The runner holds a boot that is not stopped, and a stopped boot only until the index records its stop or a later boot, because the index then holds a stopped sandbox, whose next boot may be reserved in the index or queued at a scheduler.
	pub(crate) async fn try_get_sandbox_boot_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		location: Option<&tg::location::Arg>,
	) -> tg::Result<Option<Runner>> {
		// Get the runner's state of the sandbox.
		let Some(runner) = session
			.server
			.components
			.runner
			.try_get_sandbox_runner(id, location)
		else {
			return Ok(None);
		};
		let stopped = session
			.server
			.components
			.runner
			.state()
			.sandboxes()
			.get(runner.index)
			.filter(|sandbox| sandbox.status.is_stopped())
			.map(|sandbox| sandbox.boot);
		let Some(boot) = stopped else {
			return Ok(Some(runner));
		};

		// Let the index hold a stopped boot once it records the stop or a later boot.
		let indexed =
			session.server.index.try_get_sandbox(id).await.map_err(
				|error| tg::error!(!error, %id, "failed to get the sandbox from the index"),
			)?;
		let held = indexed
			.and_then(|sandbox| sandbox.data)
			.is_some_and(|data| {
				data.data.boot > boot
					|| (data.data.boot == boot
						&& (data.data.status.is_stopped_or_destroyed()
							|| data.data.status.is_destroying()))
			});
		if held {
			return Ok(None);
		}

		Ok(Some(runner))
	}
}
