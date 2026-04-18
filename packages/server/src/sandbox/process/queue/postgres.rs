use {
	crate::Server,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_dequeue_sandbox_process_postgres(
		&self,
		process_store: &db::postgres::Database,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		let connection = process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;
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
		let rows = connection
			.inner()
			.query(statement, &[&sandbox, &now])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows.first() else {
			return Ok(None);
		};
		let process = row
			.get::<_, String>(0)
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		let output = tg::sandbox::process::queue::Output { process };

		Ok(Some(output))
	}
}
