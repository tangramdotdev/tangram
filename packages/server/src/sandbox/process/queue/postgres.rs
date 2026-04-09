use {
	crate::Server,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_dequeue_sandbox_process_postgres(
		&self,
		register: &db::postgres::Database,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		let mut connection = register
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a register connection"))?;

		let transaction = connection
			.inner_mut()
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

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
		let rows = transaction
			.query(statement, &[&sandbox.to_string(), &now])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows.first() else {
			return Ok(None);
		};
		let process = row
			.get::<_, String>(0)
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		let output = tg::sandbox::process::queue::Output { process };

		Ok(Some(output))
	}
}
