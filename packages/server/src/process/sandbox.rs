use {crate::Session, tangram_client::prelude::*};

impl Session {
	pub(crate) fn create_process_sandbox_permission_arg(
		&self,
		process: &tg::process::Id,
		sandbox: &tg::sandbox::Id,
		created_at: i64,
	) -> tg::Result<tangram_index::permission::put::Arg> {
		let time_to_live = i64::try_from(
			self.server
				.config
				.sandbox
				.process_permission_time_to_live
				.as_secs(),
		)
		.map_err(|error| tg::error!(!error, "failed to convert the permission time to live"))?;
		let expires_at = created_at
			.checked_add(time_to_live)
			.ok_or_else(|| tg::error!("the permission expiration overflowed"))?;
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
		);
		let arg = tangram_index::permission::put::Arg {
			created_at,
			creator: Some(self.context.principal.clone()),
			permissions: permission.into(),
			resource: process.clone().into(),
			source: tangram_index::permission::Source::Direct {
				expires_at: Some(expires_at),
			},
			subject: tg::authorization::Subject::Sandbox(sandbox.clone()),
			time_to_touch: Some(self.server.config.sandbox.process_permission_time_to_touch),
			version: None,
		};
		Ok(arg)
	}
}
