use {
	crate::Server,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn clean_processes_postgres(
		&self,
		process_store: &db::postgres::Database,
		processes: &[tg::process::Id],
		max_touched_at: i64,
	) -> tg::Result<()> {
		if processes.is_empty() {
			return Ok(());
		}

		let processes = processes
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<_>>();
		let connection = process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;
		let statement = indoc!(
			"
				with deleted_processes as (
					delete from processes
					where id = any($1::text[]) and touched_at <= $2
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
		connection
			.inner()
			.query(statement, &[&processes, &max_touched_at])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}
}
