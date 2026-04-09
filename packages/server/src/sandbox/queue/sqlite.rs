use {
	crate::Server,
	indoc::indoc,
	rusqlite::{self as sqlite, OptionalExtension as _},
	tangram_client::prelude::*,
	tangram_database::{self as db, Database as _},
};

impl Server {
	pub(crate) async fn try_dequeue_sandbox_sqlite(
		&self,
		register: &db::sqlite::Database,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		let connection = register
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a register connection"))?;
		connection
			.with(move |connection, _cache| Self::try_dequeue_sandbox_sqlite_sync(connection))
			.await
	}

	fn try_dequeue_sandbox_sqlite_sync(
		connection: &mut sqlite::Connection,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		let transaction = connection
			.transaction()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
		let statement = indoc!(
			"
				select id
				from sandboxes
				where status = 'created'
				order by created_at, id
				limit 1;
			"
		);
		let sandbox = transaction
			.query_row(statement, [], |row| row.get::<_, String>(0))
			.optional()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|id| id.parse::<tg::sandbox::Id>())
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the sandbox id"))?;
		let Some(sandbox) = sandbox else {
			return Ok(None);
		};
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let statement = indoc!(
			"
				update sandboxes
				set
					heartbeat_at = ?1,
					started_at = coalesce(started_at, ?1),
					status = 'started'
				where id = ?2 and status = 'created';
			"
		);
		let n = transaction
			.execute(statement, sqlite::params![now, sandbox.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n == 0 {
			return Ok(None);
		}

		let statement = indoc!(
			"
				select id
				from processes
				where sandbox = ?1 and status = 'created'
				order by created_at, id
				limit 1;
			"
		);
		let process = transaction
			.query_row(statement, sqlite::params![sandbox.to_string()], |row| {
				row.get::<_, String>(0)
			})
			.optional()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let process = if let Some(process) = process {
			let statement = indoc!(
				"
					update processes
					set
						started_at = ?1,
						status = 'started'
					where id = ?2 and sandbox = ?3 and status = 'created';
				"
			);
			let n = transaction
				.execute(
					statement,
					sqlite::params![now, process.clone(), sandbox.to_string()],
				)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			if n == 0 {
				return Ok(None);
			}
			Some(
				process
					.parse()
					.map_err(|source| tg::error!(!source, "failed to parse the process id"))?,
			)
		} else {
			None
		};

		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		let output = tg::sandbox::queue::Output { sandbox, process };

		Ok(Some(output))
	}
}
