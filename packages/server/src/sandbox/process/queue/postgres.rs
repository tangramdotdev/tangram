use {
	crate::{Server, Session},
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_dequeue_sandbox_process_postgres(
		&self,
		process_store: &db::postgres::Database,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
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
		let sandbox = sandbox.to_string();
		let rows = transaction
			.query(statement, &[&sandbox, &now])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = rows.first() else {
			return Ok(None);
		};
		let process = row
			.get::<_, String>(0)
			.parse::<tg::process::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

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

		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		let output = tg::sandbox::process::queue::Output { process, token };

		Ok(Some(output))
	}
}
