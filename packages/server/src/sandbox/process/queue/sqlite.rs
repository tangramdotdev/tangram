use {
	crate::{Server, Session},
	indoc::indoc,
	rusqlite::{self as sqlite, OptionalExtension as _},
	tangram_client::prelude::*,
	tangram_database::{self as db, Database as _},
};

impl Session {
	pub(crate) async fn try_dequeue_sandbox_process_sqlite(
		&self,
		process_store: &db::sqlite::Database,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		let connection = process_store
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let sandbox = sandbox.to_string();
		connection
			.with(move |connection, _cache| {
				Self::try_dequeue_sandbox_process_sqlite_sync(connection, &sandbox)
			})
			.await
	}

	fn try_dequeue_sandbox_process_sqlite_sync(
		connection: &mut sqlite::Connection,
		sandbox: &str,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		let transaction = connection
			.transaction()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;

		let statement = indoc!(
			"
				select id
				from processes
				where sandbox = ?1 and status = 'created'
				order by created_at, id
				limit 1;
			"
		);
		let params = sqlite::params![sandbox];
		let process = transaction
			.query_row(statement, params, |row| row.get::<_, String>(0))
			.optional()
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			.map(|id| id.parse::<tg::process::Id>())
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;
		let Some(process) = process else {
			return Ok(None);
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
		let params = sqlite::params![now, process.to_string(), sandbox];
		let n = transaction
			.execute(statement, params)
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if n == 0 {
			return Ok(None);
		}

		let token = Server::create_process_token_string();
		let statement = indoc!(
			"
				insert into process_tokens (process, token)
				values (?1, ?2);
			"
		);
		transaction
			.execute(statement, sqlite::params![process.to_string(), &token])
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		let output = tg::sandbox::process::queue::Output { process, token };

		Ok(Some(output))
	}
}
