use {
	crate::{Server, Session},
	indoc::indoc,
	rusqlite::{self as sqlite, OptionalExtension as _},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn try_dequeue_sandbox_process_sqlite(
		&self,
		process_store: &db::sqlite::Database,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		let sandbox = sandbox.clone();
		process_store
			.run(move |transaction, _cache| {
				Self::try_dequeue_sandbox_process_sqlite_sync(transaction, &sandbox)
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the sandbox process"))
	}

	fn try_dequeue_sandbox_process_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<Option<tg::sandbox::process::queue::Output>, db::sqlite::Error>> {
		let statement = indoc!(
			"
				select id
				from processes
				where sandbox = ?1 and status = 'created'
				order by created_at, id
				limit 1;
			"
		);
		let params = sqlite::params![sandbox.to_string()];
		let result = transaction
			.query_row(statement, params, |row| row.get::<_, String>(0))
			.optional()
			.map_err(db::sqlite::Error::from);
		let process = crate::database::retry!(result, "failed to execute the statement")
			.map(|id| id.parse::<tg::process::Id>())
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;
		let Some(process) = process else {
			return Ok(ControlFlow::Break(None));
		};

		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let statement = indoc!(
			"
				update processes
				set
					started_at = ?1,
					status = 'started'
				where id = ?2 and sandbox = ?3 and status = 'created';
			"
		);
		let params = sqlite::params![now, process.to_string(), sandbox.to_string()];
		let result = transaction
			.execute(statement, params)
			.map_err(db::sqlite::Error::from);
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n == 0 {
			return Ok(ControlFlow::Break(None));
		}

		let token = Server::create_process_token_string();
		let statement = indoc!(
			"
				insert into process_tokens (process, token)
				values (?1, ?2);
			"
		);
		let result = transaction
			.execute(statement, sqlite::params![process.to_string(), &token])
			.map_err(db::sqlite::Error::from);
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(Some(
			tg::sandbox::process::queue::Output { process, token },
		)))
	}
}
