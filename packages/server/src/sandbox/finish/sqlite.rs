use {
	crate::{Server, sandbox::finish::InnerArg},
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database as db,
};

impl Server {
	pub(super) async fn try_finish_sandbox_inner_sqlite(
		&self,
		transaction: &db::sqlite::Transaction<'_>,
		id: &tg::sandbox::Id,
		arg: InnerArg,
	) -> tg::Result<bool> {
		let id = id.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::try_finish_sandbox_inner_sqlite_sync(transaction, &id, &arg)
			})
			.await
	}

	fn try_finish_sandbox_inner_sqlite_sync(
		transaction: &mut sqlite::Transaction<'_>,
		id: &tg::sandbox::Id,
		arg: &InnerArg,
	) -> tg::Result<bool> {
		let statement = indoc!(
			"
				update sandboxes
				set
					finished_at = ?1,
					heartbeat_at = null,
					status = ?2
				where
					id = ?3 and
					status != 'finished';
			"
		);
		let n = transaction
			.execute(
				statement,
				sqlite::params![
					arg.now,
					tg::sandbox::Status::Finished.to_string(),
					id.to_string(),
				],
			)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n != 1 {
			return Ok(false);
		}

		let statement = indoc!(
			"
				insert into sandbox_finalize_queue (created_at, sandbox, status)
				values (?1, ?2, ?3);
			"
		);
		transaction
			.execute(
				statement,
				sqlite::params![arg.now, id.to_string(), "created"],
			)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(true)
	}
}
