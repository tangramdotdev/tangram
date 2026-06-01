use {
	crate::{Server, sandbox::finalize::Entry},
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_dequeue_sandbox_finalize_batch_turso(
		&self,
		process_store: &db::turso::Database,
		batch_size: usize,
	) -> tg::Result<Option<Vec<Entry>>> {
		db::turso::run!(process_store, |transaction| {
			Self::try_dequeue_sandbox_finalize_batch_turso_with_transaction(transaction, batch_size)
				.await
		})
		.map_err(|error| tg::error!(!error, "failed to dequeue sandbox finalize entries"))
	}

	async fn try_dequeue_sandbox_finalize_batch_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		batch_size: usize,
	) -> tg::Result<ControlFlow<Option<Vec<Entry>>, db::turso::Error>> {
		let statement = indoc!(
			"
				select position, sandbox
				from sandbox_finalize_queue
				where status = 'created'
				order by position
				limit ?1;
			"
		);
		#[derive(db::row::Deserialize)]
		struct Row {
			position: i64,
			#[tangram_database(as = "db::value::FromStr")]
			sandbox: tg::sandbox::Id,
		}
		let result = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![i64::try_from(batch_size).unwrap()],
			)
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let entries = rows
			.into_iter()
			.map(|row| Entry {
				position: row.position,
				sandbox: row.sandbox,
			})
			.collect::<Vec<_>>();
		if entries.is_empty() {
			return Ok(ControlFlow::Break(None));
		}
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let statement = indoc!(
			"
				update sandbox_finalize_queue
				set
					started_at = coalesce(started_at, ?1),
					status = 'started'
				where position = ?2;
			"
		);
		for entry in &entries {
			let result = transaction
				.execute(statement.into(), db::params![now, entry.position])
				.await;
			let n = crate::database::retry!(result, "failed to execute the statement");
			if n != 1 {
				return Err(tg::error!(
					"failed to claim the sandbox finalize queue entry"
				));
			}
		}
		Ok(ControlFlow::Break(Some(entries)))
	}
}
