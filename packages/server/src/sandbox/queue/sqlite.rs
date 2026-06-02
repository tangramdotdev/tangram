use {
	crate::{Server, Session, sandbox::queue::LocalOutput},
	indoc::indoc,
	rusqlite::{self as sqlite, OptionalExtension as _},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(super) async fn try_dequeue_sandbox_sqlite(
		&self,
		process_store: &db::sqlite::Database,
		token: bool,
	) -> tg::Result<Option<LocalOutput>> {
		process_store
			.run(move |transaction, _cache| {
				Self::try_dequeue_sandbox_sqlite_sync(transaction, token)
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the sandbox"))
	}

	fn try_dequeue_sandbox_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		token: bool,
	) -> tg::Result<ControlFlow<Option<LocalOutput>, db::sqlite::Error>> {
		let statement = indoc!(
			"
				select id, created_by
				from sandboxes
				where status = 'created'
				order by created_at, id
				limit 1;
			"
		);
		let result = transaction
			.query_row(statement, [], |row| {
				Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
			})
			.optional()
			.map_err(db::sqlite::Error::from);
		let row = crate::database::retry!(result, "failed to execute the statement");
		let Some((sandbox, created_by)) = row else {
			return Ok(ControlFlow::Break(None));
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
		let result = transaction
			.execute(statement, sqlite::params![now, sandbox.to_string()])
			.map_err(db::sqlite::Error::from);
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n == 0 {
			return Ok(ControlFlow::Break(None));
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
		let result = transaction
			.query_row(statement, sqlite::params![sandbox.to_string()], |row| {
				row.get::<_, String>(0)
			})
			.optional()
			.map_err(db::sqlite::Error::from);
		let process = crate::database::retry!(result, "failed to execute the statement");
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
			let result = transaction
				.execute(
					statement,
					sqlite::params![now, process.clone(), sandbox.to_string()],
				)
				.map_err(db::sqlite::Error::from);
			let n = crate::database::retry!(result, "failed to execute the statement");
			if n == 0 {
				return Ok(ControlFlow::Break(None));
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
			let result = transaction
				.execute(statement, sqlite::params![process.to_string(), &token])
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
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
			let result = transaction
				.execute(statement, sqlite::params![sandbox.to_string(), &token])
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
			Some(token)
		} else {
			None
		};

		Ok(ControlFlow::Break(Some(LocalOutput {
			created_by,
			process,
			process_token,
			sandbox,
			token,
		})))
	}
}
