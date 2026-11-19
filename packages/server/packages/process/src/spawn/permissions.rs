use {crate::Component, tangram_client::prelude::*, tangram_server_core::Session};

impl Component {
	pub(super) fn spawn_process_create_creator_permission_arg(
		session: &Session,
		principal: &tg::Principal,
		id: &tg::process::Id,
		created_at: i64,
	) -> tg::Result<Option<tangram_index::permission::put::Arg>> {
		let config = &session.server.config.process;
		tangram_server_process_core::permission::creator_permission_arg(
			principal,
			id,
			created_at,
			config.permission_time_to_live,
			config.permission_time_to_touch,
		)
	}

	pub(super) fn spawn_process_create_public_permission_arg(
		session: &Session,
		id: &tg::process::Id,
		created_at: i64,
	) -> tg::Result<tangram_index::permission::put::Arg> {
		let config = &session.server.config.process;
		tangram_server_process_core::permission::public_permission_arg(
			id,
			created_at,
			config.permission_time_to_live,
			config.permission_time_to_touch,
		)
	}

	/// Put the public permission on a cached process that a public spawn returns, with one index write.
	pub(super) async fn spawn_process_put_public_permission_if_requested(
		&self,
		session: &Session,
		arg: &tg::process::spawn::Arg,
		output: &tg::process::spawn::Output,
	) -> tg::Result<()> {
		if !arg.public {
			return Ok(());
		}
		let tg::Either::Right(id) = &output.process else {
			return Ok(());
		};
		let now = session.server.clock.unix_timestamp();
		let permission = Self::spawn_process_create_public_permission_arg(session, id, now)?;
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::PutPermission(Box::new(permission))],
		};
		session
			.server
			.components
			.indexer
			.write(&session.server, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to write the public permission"))?;
		Ok(())
	}
}
