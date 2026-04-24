use {
	crate::{Server, sandbox::finish::InnerArg},
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(super) async fn try_finish_sandbox_inner_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		id: &tg::sandbox::Id,
		arg: InnerArg,
	) -> tg::Result<bool> {
		let statement = indoc!(
			"
				with updated as (
					update sandboxes
					set
						finished_at = $1,
						heartbeat_at = null,
						status = $2
					where
						id = $3 and
						status != 'finished'
					returning id
				),
				enqueued as (
					insert into sandbox_finalize_queue (created_at, sandbox, status)
					select $1, id, $4
					from updated
					returning sandbox
				)
				select exists(select 1 from updated);
			"
		);
		let params = db::params![
			arg.now,
			tg::sandbox::Status::Finished.to_string(),
			id.to_string(),
			"created",
		];
		let finished = transaction
			.query_one_value_into::<bool>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(finished)
	}
}
