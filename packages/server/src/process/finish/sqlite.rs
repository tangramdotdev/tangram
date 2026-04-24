use {
	crate::{Server, process::finish::InnerArg},
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database as db,
};

impl Server {
	pub(super) async fn try_finish_process_inner_sqlite(
		&self,
		transaction: &db::sqlite::Transaction<'_>,
		id: &tg::process::Id,
		arg: InnerArg,
	) -> tg::Result<bool> {
		let id = id.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::try_finish_process_inner_sqlite_sync(transaction, &id, &arg)
			})
			.await
	}

	fn try_finish_process_inner_sqlite_sync(
		transaction: &mut sqlite::Transaction<'_>,
		id: &tg::process::Id,
		arg: &InnerArg,
	) -> tg::Result<bool> {
		let statement = indoc!(
			"
				update processes
				set
					actual_checksum = ?1,
					depth = null,
					error = ?2,
					error_code = ?3,
					finished_at = ?4,
					output = ?5,
					exit = ?6,
					status = ?7,
					stderr_open = case when stderr_open is null then null else false end,
					stdin_open = case when stdin_open is null then null else false end,
					stdout_open = case when stdout_open is null then null else false end,
					token_count = 0,
					touched_at = ?8
				where
					id = ?9 and
					status != 'finished';
			"
		);
		let n = transaction
			.execute(
				statement,
				sqlite::params![
					arg.checksum.as_deref(),
					arg.error.as_deref(),
					arg.error_code.as_deref(),
					arg.now,
					arg.output.as_deref(),
					arg.exit,
					tg::process::Status::Finished.to_string(),
					arg.now,
					id.to_string(),
				],
			)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n != 1 {
			return Ok(false);
		}

		let statement = indoc!(
			"
				delete from process_tokens
				where process = ?1;
			"
		);
		transaction
			.execute(statement, sqlite::params![id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		let statement = indoc!(
			"
				insert into process_finalize_queue (created_at, process, status)
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
