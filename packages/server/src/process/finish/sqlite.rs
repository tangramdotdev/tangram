use {
	crate::{
		Server,
		process::finish::{Condition, InnerArg},
	},
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
		let error_code = arg.error_code.map(|code| code.to_string());
		let error = arg.error.as_ref().map(|error| match error {
			tg::Either::Left(data) => serde_json::to_string(data).unwrap(),
			tg::Either::Right(id) => id.to_string(),
		});
		let output = arg
			.output
			.as_ref()
			.map(serde_json::to_string)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
		let (condition, max_depth) = match arg.condition {
			Some(Condition::DepthExceeded { max_depth }) => {
				(Some("depth_exceeded"), Some(max_depth))
			},
			Some(Condition::TokenCountZero) => (Some("token_count_zero"), None),
			None => (None, None),
		};
		let checksum = arg.checksum.as_ref().map(ToString::to_string);
		let statement = indoc!(
			"
				update processes
				set
					actual_checksum = ?1,
					depth = null,
					error = ?2,
					error_code = ?3,
					exit = ?4,
					finished_at = ?5,
					output = ?6,
					status = ?7,
					stderr_open = case when stderr_open is null then null else false end,
					stdin_open = case when stdin_open is null then null else false end,
					stdout_open = case when stdout_open is null then null else false end,
					stored_at = ?5,
					token_count = 0
				where
					id = ?8 and
					status != 'finished' and
					(
						?9 is null or
						(?9 = 'depth_exceeded' and depth > ?10) or
						(?9 = 'token_count_zero' and token_count = 0)
					);
			"
		);
		let n = transaction
			.execute(
				statement,
				sqlite::params![
					checksum.as_deref(),
					error.as_deref(),
					error_code.as_deref(),
					i64::from(arg.exit),
					arg.now,
					output.as_deref(),
					tg::process::Status::Finished.to_string(),
					id.to_string(),
					condition,
					max_depth,
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
