use {
	crate::{Server, sandbox::finalize::Entry},
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_dequeue_sandbox_finalize_batch_postgres(
		&self,
		process_store: &db::postgres::Database,
		batch_size: usize,
	) -> tg::Result<Option<Vec<Entry>>> {
		let connection = process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			position: i64,
			#[tangram_database(as = "db::postgres::value::FromStr")]
			sandbox: tg::sandbox::Id,
		}
		let statement = indoc!(
			"
				with candidate as (
					select position, sandbox
					from sandbox_finalize_queue
					where status = 'created'
					order by position
					limit $1
					for update skip locked
				),
				started as (
					update sandbox_finalize_queue
					set
						started_at = coalesce(started_at, $2),
						status = 'started'
					where position in (select position from candidate)
					returning position, sandbox
				)
				select position, sandbox
				from started
				order by position;
			"
		);
		let batch_size = i64::try_from(batch_size).unwrap();
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let rows = connection
			.inner()
			.query(statement, &[&batch_size, &now])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let entries = rows
			.iter()
			.map(|row| {
				<Row as db::postgres::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.map(|row| {
				let row = row?;
				Ok(Entry {
					position: row.position,
					sandbox: row.sandbox,
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;
		if entries.is_empty() {
			return Ok(None);
		}
		Ok(Some(entries))
	}
}
