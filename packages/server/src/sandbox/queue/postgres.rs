use {
	crate::Server,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_dequeue_sandbox_postgres(
		&self,
		sandbox_store: &db::postgres::Database,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		let mut connection = sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
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
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows.first() else {
			return Ok(None);
		};
		let sandbox = row
			.get::<_, String>(0)
			.parse::<tg::sandbox::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the sandbox id"))?;

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
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let process = rows
			.first()
			.map(|row| {
				row.get::<_, String>(0)
					.parse()
					.map_err(|source| tg::error!(!source, "failed to parse the process id"))
			})
			.transpose()?;

		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		let output = tg::sandbox::queue::Output { sandbox, process };

		Ok(Some(output))
	}
}
