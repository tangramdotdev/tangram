use {
	crate::Session,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_dequeue_process_signal_postgres(
		&self,
		process_store: &db::postgres::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Signal>> {
		let id = id.to_string();
		db::postgres::run!(process_store, |transaction| {
			Self::try_dequeue_process_signal_postgres_with_transaction(transaction, &id).await
		})
		.map_err(|error| tg::error!(!error, "failed to dequeue the process signal"))
	}

	async fn try_dequeue_process_signal_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		id: &str,
	) -> tg::Result<ControlFlow<Option<tg::process::Signal>, db::postgres::Error>> {
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
		let result = transaction
			.inner()
			.query_opt(statement, &[&id])
			.await
			.map_err(db::postgres::Error::from);
		let row = crate::database::retry!(result, "failed to execute the statement")
			.map(|row| {
				<Row as db::postgres::row::Deserialize>::deserialize(&row)
					.map_err(|error| tg::error!(!error, "failed to deserialize the row"))
			})
			.transpose()?;
		let Some(row) = row else {
			return Ok(ControlFlow::Break(None));
		};
		let _ = row.position;
		Ok(ControlFlow::Break(Some(row.signal)))
	}
}
