use {crate::Session, tangram_client::prelude::*};

impl Session {
	pub(super) async fn destroy_sandbox_control_request(
		&self,
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
		self.server.spawn_publish_sandbox_status_task(id);
		crate::checkpoint!(self.server, "sandbox.control.destroy", sandbox = %id).await;

		let account = match arg.data.data.owner.as_ref() {
			Some(owner) => self.usage_account(owner).await?,
			None => None,
		};
		let now = self.server.clock.unix_timestamp()?;
		let location = tg::Location::Local(tg::location::Local {
			region: self.server.config.region.clone(),
		});
		let put_sandbox = tangram_index::sandbox::put::Arg {
			account,
			created_at,
			data: Some(arg.data),
			id: id.clone(),
			location: Some(location),
			runner,
			touched_at: now,
		};
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(put_sandbox)],
		};
		self.server.index_batch(arg).await.map_err(
			|error| tg::error!(!error, %id, "failed to put the destroyed sandbox in the index"),
		)?;
		crate::checkpoint!(self.server, "sandbox.control.destroy.submitted", sandbox = %id).await;

		Ok(tg::sandbox::control::DestroyServerResponseOutput {})
	}
}
