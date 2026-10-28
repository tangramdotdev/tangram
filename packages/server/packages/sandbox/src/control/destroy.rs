use {crate::Component, tangram_client::prelude::*, tangram_server_core::Session};

impl Component {
	pub(super) async fn destroy_sandbox_control_request(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::control::DestroyClientRequestArg,
		created_at: i64,
		runner: Option<tg::runner::Id>,
	) -> tg::Result<tg::sandbox::control::DestroyServerResponseOutput> {
		if arg.data.data.id != *id {
			return Err(
				tg::error!(actual = %arg.data.data.id, expected = %id, "the sandbox id does not match"),
			);
		}
		if !arg.data.data.status.is_destroyed() {
			return Err(tg::error!(%id, "expected a destroyed sandbox"));
		}
		Self::spawn_publish_sandbox_status_task(&session.server, id);
		tangram_server_core::checkpoint!(session.server, "sandbox.control.destroy", sandbox = %id)
			.await;

		let account = match arg.data.data.owner.as_ref() {
			Some(owner) => {
				session
					.server
					.components
					.usage
					.usage_account(session, owner)
					.await?
			},
			None => None,
		};
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
			runner,
			touched_at: now,
		};
		let put_sandbox = Box::new(put_sandbox);
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(put_sandbox)],
		};
		session
			.server
			.components
			.indexer
			.index_batch(&session.server, arg)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to put the destroyed sandbox in the index"),
			)?;
		tangram_server_core::checkpoint!(session.server, "sandbox.control.destroy.submitted", sandbox = %id).await;

		Ok(tg::sandbox::control::DestroyServerResponseOutput {})
	}
}
