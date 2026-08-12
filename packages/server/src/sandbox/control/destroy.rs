use {crate::Session, tangram_client::prelude::*, tangram_index::prelude::*};

impl Session {
	pub(super) async fn destroy_sandbox_control_request(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::control::DestroyClientRequestArg,
		created_at: i64,
		runner: Option<tg::runner::Id>,
	) -> tg::Result<tg::sandbox::control::DestroyServerResponseOutput> {
		if arg.data.id != *id {
			return Err(tg::error!(%id, "the sandbox id does not match"));
		}
		if !arg.data.status.is_destroyed() {
			return Err(tg::error!(%id, "expected a destroyed sandbox"));
		}

		let account = match arg.data.owner.as_ref() {
			Some(owner) => self.usage_account(owner).await?,
			None => None,
		};
		let now = self.server.clock.unix_timestamp()?;
		let put_sandbox = tangram_index::sandbox::put::Arg {
			account,
			created_at,
			data: Some(arg.data),
			id: id.clone(),
			runner,
			touched_at: now,
		};
		self.server
			.index
			.batch(tangram_index::batch::Arg {
				items: vec![tangram_index::batch::Item::PutSandbox(put_sandbox)],
			})
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to put the destroyed sandbox in the index"),
			)?;
		self.server.spawn_publish_sandbox_status_task(id);

		Ok(tg::sandbox::control::DestroyServerResponseOutput {})
	}
}
