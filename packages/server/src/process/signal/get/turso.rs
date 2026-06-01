use {
	crate::Session,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_dequeue_process_signal_turso(
		&self,
		process_store: &db::turso::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Signal>> {
		let id = id.to_string();
		db::turso::run!(process_store, |transaction| {
			Self::try_dequeue_process_signal_turso_with_transaction(transaction, &id).await
		})
		.map_err(|error| tg::error!(!error, "failed to dequeue the process signal"))
	}

	async fn try_dequeue_process_signal_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		id: &str,
	) -> tg::Result<ControlFlow<Option<tg::process::Signal>, db::turso::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			position: i64,
			#[tangram_database(as = "db::value::FromStr")]
			signal: tg::process::Signal,
		}
		let statement = indoc!(
			"
				select position, signal
				from process_signals
				where process = ?1
				order by position
				limit 1;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_owned()])
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		let Some(row) = row else {
			return Ok(ControlFlow::Break(None));
		};

		let statement = indoc!(
			"
				delete from process_signals
				where position = ?1;
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![row.position])
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(Some(row.signal)))
	}
}
