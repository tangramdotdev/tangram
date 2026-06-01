use {
	crate::Server,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn clean_processes_turso(
		&self,
		process_store: &db::turso::Database,
		processes: &[tg::process::Id],
		max_stored_at: i64,
	) -> tg::Result<()> {
		if processes.is_empty() {
			return Ok(());
		}

		for process in processes {
			let process = process.to_string();
			db::turso::run!(process_store, |transaction| {
				Self::clean_processes_turso_with_transaction(transaction, &process, max_stored_at)
					.await
			})
			.map_err(|error| tg::error!(!error, "failed to clean the process"))?;
		}

		Ok(())
	}

	async fn clean_processes_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		process: &str,
		max_stored_at: i64,
	) -> tg::Result<ControlFlow<(), db::turso::Error>> {
		let statement = indoc!(
			"
				delete from processes
				where id = ?1 and stored_at <= ?2;
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![process.to_owned(), max_stored_at],
			)
			.await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n == 0 {
			return Ok(ControlFlow::Break(()));
		}

		let statement = indoc!(
			"
				delete from process_grants
				where process = ?1;
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![process.to_owned()])
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		let statement = indoc!(
			"
				delete from process_tokens
				where process = ?1;
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![process.to_owned()])
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		let statement = indoc!(
			"
				delete from process_children
				where process = ?1;
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![process.to_owned()])
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		let statement = indoc!(
			"
				delete from process_leases
				where process = ?1;
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![process.to_owned()])
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		let statement = indoc!(
			"
				delete from process_finalize_queue
				where process = ?1;
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![process.to_owned()])
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		let statement = indoc!(
			"
				delete from process_signals
				where process = ?1;
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![process.to_owned()])
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		let statement = indoc!(
			"
				delete from process_stdio
				where process = ?1;
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![process.to_owned()])
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn clean_expired_process_grants_turso(
		&self,
		process_store: &db::turso::Database,
		now: i64,
	) -> tg::Result<()> {
		db::turso::run!(process_store, |transaction| {
			Self::clean_expired_process_grants_turso_with_transaction(transaction, now).await
		})
		.map_err(|error| tg::error!(!error, "failed to delete process grants"))
	}

	async fn clean_expired_process_grants_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		now: i64,
	) -> tg::Result<ControlFlow<(), db::turso::Error>> {
		let statement = indoc!(
			"
				delete from process_grants
				where expires_at <= ?1;
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![now])
			.await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}
}
