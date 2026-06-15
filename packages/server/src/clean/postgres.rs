use {
	crate::Server,
	futures::FutureExt as _,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Server {
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
				),
				deleted_process_tokens as (
					delete from process_tokens
					where process in (select id from deleted_processes)
					returning 1
				),
				deleted_process_leases as (
					delete from process_leases
					where process in (select id from deleted_processes)
					returning 1
				),
				deleted_process_finalize_queue as (
					delete from process_finalize_queue
					where process in (select id from deleted_processes)
					returning 1
				),
				deleted_process_signals as (
					delete from process_signals
					where process in (select id from deleted_processes)
					returning 1
				),
				deleted_process_stdio as (
					delete from process_stdio
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
