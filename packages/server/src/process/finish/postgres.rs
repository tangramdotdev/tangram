use {
	crate::{Server, process::finish::InnerArg},
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(super) async fn try_finish_process_inner_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		id: &tg::process::Id,
		arg: InnerArg,
	) -> tg::Result<bool> {
		let statement = indoc!(
			"
				with updated as (
					update processes
					set
						actual_checksum = $1,
						depth = null,
						error = $2,
						error_code = $3,
						finished_at = $4,
						output = $5,
						exit = $6,
						status = $7,
						stderr_open = case when stderr_open is null then null else false end,
						stdin_open = case when stdin_open is null then null else false end,
						stdout_open = case when stdout_open is null then null else false end,
						token_count = 0,
						touched_at = $8
					where
						id = $9 and
						status != 'finished'
					returning id
				),
				deleted_tokens as (
					delete from process_tokens
					where process in (select id from updated)
					returning process
				),
				enqueued as (
					insert into process_finalize_queue (created_at, process, status)
					select $4, id, $10
					from updated
					returning process
				)
				select exists(select 1 from updated);
			"
		);
		let finished = transaction
			.query_one_value_into::<bool>(
				statement.into(),
				db::params![
					arg.checksum,
					arg.error,
					arg.error_code,
					arg.now,
					arg.output,
					arg.exit,
					tg::process::Status::Finished.to_string(),
					arg.now,
					id.to_string(),
					"created",
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(finished)
	}
}
