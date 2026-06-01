use {
	crate::{Server, process::finalize::Entry},
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_finalizer_dequeue_batch_postgres(
		&self,
		process_store: &db::postgres::Database,
		batch_size: usize,
	) -> tg::Result<Option<Vec<Entry>>> {
		db::postgres::run!(process_store, |transaction| {
			Self::try_finalizer_dequeue_batch_postgres_with_transaction(transaction, batch_size)
				.await
		})
		.map_err(|error| tg::error!(!error, "failed to dequeue finalize entries"))
	}

	async fn try_finalizer_dequeue_batch_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		batch_size: usize,
	) -> tg::Result<ControlFlow<Option<Vec<Entry>>, db::postgres::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			position: i64,
			process: String,
		}
		let statement = indoc!(
			"
				select position, process
				from process_finalize_queue
				where status = 'created'
				order by position
				limit $1;
			"
		);
		let result = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![i64::try_from(batch_size).unwrap()],
			)
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let mut entries = Vec::new();
		for row in rows {
			let process = row
				.process
				.parse()
				.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;
			entries.push(Entry {
				position: row.position,
				process,
			});
		}
		if entries.is_empty() {
			return Ok(ControlFlow::Break(None));
		}

		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let statement = indoc!(
			"
				update process_finalize_queue
				set
					started_at = coalesce(started_at, $1),
					status = 'started'
				where position = $2;
			"
		);
		for entry in &entries {
			let result = transaction
				.execute(statement.into(), db::params![now, entry.position])
				.await;
			let n = crate::database::retry!(result, "failed to execute the statement");
			if n != 1 {
				return Err(tg::error!(
					"failed to claim the process finalize queue entry"
				));
			}
		}

		Ok(ControlFlow::Break(Some(entries)))
	}
}
