use {
	crate::Server,
	futures::FutureExt as _,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Server {
	pub(crate) async fn clean_sandboxes_postgres(
		&self,
		process_store: &db::postgres::Database,
		sandboxes: &[tg::sandbox::Id],
		max_finished_at: i64,
	) -> tg::Result<()> {
		if sandboxes.is_empty() {
			return Ok(());
		}

		let sandboxes = sandboxes
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<_>>();
		process_store
			.run(|transaction| {
				let sandboxes = sandboxes.clone();
				async move {
					Self::clean_sandboxes_postgres_with_transaction(
						transaction,
						&sandboxes,
						max_finished_at,
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to clean the sandbox"))
	}

	async fn clean_sandboxes_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		sandboxes: &[String],
		max_finished_at: i64,
	) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
		let statement = indoc!(
			"
				delete from sandboxes
				where id = any($1::text[])
					and status = 'destroyed'
					and finished_at <= $2
				returning id;
			"
		);
		let result = transaction
			.inner()
			.query(statement, &[&sandboxes, &max_finished_at])
			.await
			.map_err(db::postgres::Error::from);
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn clean_processes_postgres(
		&self,
		process_store: &db::postgres::Database,
		processes: &[tg::process::Id],
		max_stored_at: i64,
	) -> tg::Result<()> {
		if processes.is_empty() {
			return Ok(());
		}

		let processes = processes
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<_>>();
		process_store
			.run(|transaction| {
				let processes = processes.clone();
				async move {
					Self::clean_processes_postgres_with_transaction(
						transaction,
						&processes,
						max_stored_at,
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to clean the process"))
	}

	async fn clean_processes_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		processes: &[String],
		max_stored_at: i64,
	) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
		let statement = indoc!(
			"
				with deleted_processes as (
					delete from processes
					where id = any($1::text[]) and stored_at <= $2
					returning id
				),
				deleted_process_children as (
					delete from process_children
					where process in (select id from deleted_processes)
					returning 1
				)
				select id
				from deleted_processes;
			"
		);
		let result = transaction
			.inner()
			.query(statement, &[&processes, &max_stored_at])
			.await
			.map_err(db::postgres::Error::from);
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}
}
