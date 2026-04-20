use {
	crate::Server,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_dequeue_process_signal_postgres(
		&self,
		process_store: &db::postgres::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Signal>> {
		let connection = process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			position: i64,
			#[tangram_database(as = "db::postgres::value::FromStr")]
			signal: tg::process::Signal,
		}
		let statement = indoc!(
			"
				with candidate as (
					select position, signal
					from process_signals
					where process = $1
					order by position
					limit 1
					for update skip locked
				),
				deleted as (
					delete from process_signals
					where position in (select position from candidate)
					returning position, signal
				)
				select position, signal
				from deleted;
			"
		);
		let id = id.to_string();
		let row = connection
			.inner()
			.query_opt(statement, &[&id])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| {
				<Row as db::postgres::row::Deserialize>::deserialize(&row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.transpose()?;
		let Some(row) = row else {
			return Ok(None);
		};
		let _ = row.position;
		Ok(Some(row.signal))
	}
}
