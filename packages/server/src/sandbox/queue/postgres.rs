use {
	crate::{Server, Session, sandbox::queue::LocalOutput},
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(super) async fn try_dequeue_sandbox_postgres(
		&self,
		process_store: &db::postgres::Database,
		token: bool,
	) -> tg::Result<Option<LocalOutput>> {
		let mut connection = process_store
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let transaction = connection
			.inner_mut()
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let now = time::OffsetDateTime::now_utc().unix_timestamp();

		let statement = indoc!(
			"
				with candidate as (
					select id
					from sandboxes
					where status = 'created'
					order by created_at, id
					limit 1
					for update skip locked
				)
				update sandboxes
				set
					heartbeat_at = $1,
					started_at = coalesce(started_at, $1),
					status = 'started'
				where id in (select id from candidate)
				returning id;
			"
		);
		let rows = transaction
			.query(statement, &[&now])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = rows.first() else {
			return Ok(None);
		};
		let sandbox = row
			.get::<_, String>(0)
			.parse::<tg::sandbox::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the sandbox id"))?;

		let statement = indoc!(
			"
				with candidate as (
					select id
					from processes
					where sandbox = $1 and status = 'created'
					order by created_at, id
					limit 1
					for update skip locked
				)
				update processes
				set
					started_at = $2,
					status = 'started'
				where id in (select id from candidate)
				returning id;
			"
		);
		let sandbox_string = sandbox.to_string();
		let rows = transaction
			.query(statement, &[&sandbox_string, &now])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let process: Option<tg::process::Id> = rows
			.first()
			.map(|row| {
				row.get::<_, String>(0)
					.parse::<tg::process::Id>()
					.map_err(|error| tg::error!(!error, "failed to parse the process id"))
			})
			.transpose()?;

		let process_token = if let Some(process) = &process {
			let token = Server::create_process_token_string();
			let statement = indoc!(
				"
					insert into process_tokens (process, token)
					values ($1, $2);
				"
			);
			let process_string = process.to_string();
			transaction
				.execute(statement, &[&process_string, &token])
				.await
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
					values ($1, $2);
				"
			);
			let sandbox_string = sandbox.to_string();
			transaction
				.execute(statement, &[&sandbox_string, &token])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			Some(token)
		} else {
			None
		};

		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		let output = LocalOutput {
			process,
			process_token,
			sandbox,
			token,
		};

		Ok(Some(output))
	}
}
