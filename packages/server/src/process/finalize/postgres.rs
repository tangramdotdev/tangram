use {
	crate::{Server, process::finalize::Entry},
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn finalizer_try_dequeue_batch_postgres(
		&self,
		sandbox_store: &db::postgres::Database,
		batch_size: usize,
	) -> tg::Result<Option<Vec<Entry>>> {
		let connection = sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			position: i64,
			#[tangram_database(as = "db::postgres::value::FromStr")]
			process: tg::process::Id,
		}
		let statement = indoc!(
			"
				with candidate as (
					select position, process
					from process_finalize_queue
					order by position
					limit $1
					for update skip locked
				),
				deleted as (
					delete from process_finalize_queue
					where position in (select position from candidate)
					returning position, process
				)
				select position, process
				from deleted
				order by position;
			"
		);
		let batch_size = i64::try_from(batch_size).unwrap();
		let rows = connection
			.inner()
			.query(statement, &[&batch_size])
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
					process: row.process,
				})
			})
			.collect::<tg::Result<Vec<_>>>()?;
		if entries.is_empty() {
			return Ok(None);
		}
		Ok(Some(entries))
	}
}
