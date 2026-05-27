use {
	crate::{Server, Session, sandbox::queue::LocalOutput},
	indoc::indoc,
	rusqlite::{self as sqlite, OptionalExtension as _},
	tangram_client::prelude::*,
	tangram_database::{self as db, Database as _},
};

impl Session {
	pub(super) async fn try_dequeue_sandbox_sqlite(
		&self,
		process_store: &db::sqlite::Database,
		token: bool,
	) -> tg::Result<Option<LocalOutput>> {
		let connection = process_store
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		connection
			.with(move |connection, _cache| {
				Self::try_dequeue_sandbox_sqlite_sync(connection, token)
			})
			.await
	}

	fn try_dequeue_sandbox_sqlite_sync(
		connection: &mut sqlite::Connection,
		token: bool,
	) -> tg::Result<Option<LocalOutput>> {
		let transaction = connection
			.transaction()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let statement = indoc!(
			"
				select id, created_by
				from sandboxes
				where status = 'created'
				order by created_at, id
				limit 1;
			"
		);
		let row = transaction
			.query_row(statement, [], |row| {
				Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
			})
			.optional()
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some((sandbox, created_by)) = row else {
			return Ok(None);
		};
		let sandbox = sandbox
			.parse::<tg::sandbox::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the sandbox id"))?;
		let created_by = created_by
			.map(|user| user.parse::<tg::user::Id>())
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the user id"))?;
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
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
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
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
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
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if n == 0 {
				return Ok(None);
			}
			Some(
				process
					.parse::<tg::process::Id>()
					.map_err(|error| tg::error!(!error, "failed to parse the process id"))?,
			)
		} else {
			None
		};

		let process_token = if let Some(process) = &process {
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
			Some(token)
		} else {
			None
		};

		let token = if token {
			let token = Self::create_sandbox_token_string();
			let statement = indoc!(
				"
					insert into sandbox_tokens (sandbox, token)
					values (?1, ?2);
				"
			);
			transaction
				.execute(statement, sqlite::params![sandbox.to_string(), &token])
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			Some(token)
		} else {
			None
		};

		transaction
			.commit()
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		let output = LocalOutput {
			created_by,
			process,
			process_token,
			sandbox,
			token,
		};

		Ok(Some(output))
	}
}
