use {crate::Component, tangram_client::prelude::*, tangram_server_core::Session};

impl Component {
	pub(super) async fn stop_sandbox_control_request(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::control::StopClientRequestArg,
		created_at: i64,
		runner: Option<tg::runner::Id>,
	) -> tg::Result<tg::sandbox::control::StopServerResponseOutput> {
		if arg.data.data.id != *id {
			return Err(
				tg::error!(actual = %arg.data.data.id, expected = %id, "the sandbox id does not match"),
			);
		}
		if !arg.data.data.status.is_stopped() {
			return Err(tg::error!(%id, "expected a stopped sandbox"));
		}
		Self::spawn_publish_sandbox_status_task(&session.server, id);
		tangram_server_core::checkpoint!(session.server, "sandbox.control.stop", sandbox = %id)
			.await;
		Self::warn_missing_sandbox_usage(session, &arg.data.data);

		// Submit the stopped sandbox with the processes of its boot, which the index merges with those of earlier boots.
		let account = session
			.server
			.components
			.usage
			.usage_account(session, &arg.data.data.owner)
			.await?;
		let now = session.server.clock.unix_timestamp();
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let put_sandbox = tangram_index::sandbox::put::Arg {
			account,
			created_at,
			data: Some(arg.data),
			id: id.clone(),
			location: Some(location),
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(id.clone()),
			processes: Some(arg.processes),
			registration: None,
			runner,
			scheduled: None,
			stopped_usage: None,
			touched_at: now,
		};
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::PutSandbox(Box::new(put_sandbox))],
		};
		session
			.server
			.components
			.indexer
			.write(&session.server, arg)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to put the stopped sandbox in the index"),
			)?;

		let output = tg::sandbox::control::StopServerResponseOutput {};

		Ok(output)
	}
}
