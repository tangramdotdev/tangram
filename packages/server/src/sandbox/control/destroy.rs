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

		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let put_sandbox = tangram_index::sandbox::put::Arg {
			created_at,
			data: Some(arg.data),
			id: id.clone(),
			runner,
			touched_at: now,
		};
		self.server
			.index
			.batch(tangram_index::batch::Arg {
				put_sandboxes: vec![put_sandbox],
				..Default::default()
			})
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to put the destroyed sandbox in the index"),
			)?;
		self.server.enqueue_sandbox_finalization(id).await?;

		self.server.spawn_publish_sandbox_status_task(id);

		Ok(tg::sandbox::control::DestroyServerResponseOutput {})
	}
}
