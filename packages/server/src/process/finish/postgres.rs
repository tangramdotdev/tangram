use {
	crate::{
		Server,
		process::finish::{Condition, InnerArg},
	},
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
		let error_code = arg.error_code.map(|code| code.to_string());
		let error = arg.error.as_ref().map(|error| match error {
			tg::Either::Left(data) => serde_json::to_string(data).unwrap(),
			tg::Either::Right(id) => id.to_string(),
		});
		let output = arg
			.output
			.as_ref()
			.map(serde_json::to_string)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
		let (condition, max_depth) = match arg.condition {
			Some(Condition::DepthExceeded { max_depth }) => {
				(Some("depth_exceeded"), Some(max_depth))
			},
			Some(Condition::TokenCountZero) => (Some("token_count_zero"), None),
			None => (None, None),
		};
		let statement = indoc!(
			"
				with updated as (
					update processes
					set
						actual_checksum = $1,
						depth = null,
						error = $2,
						error_code = $3,
						exit = $4,
						finished_at = $5,
						output = $6,
						status = $7,
						stderr_open = case when stderr_open is null then null else false end,
						stdin_open = case when stdin_open is null then null else false end,
						stdout_open = case when stdout_open is null then null else false end,
						stored_at = $5,
						token_count = 0
					where
						id = $8 and
						status != 'finished' and
						(
							$10::text is null or
							($10 = 'depth_exceeded' and depth > $11) or
							($10 = 'token_count_zero' and token_count = 0)
						)
					returning id
				),
				deleted_tokens as (
					delete from process_tokens
					where process in (select id from updated)
					returning process
				),
				enqueued as (
					insert into process_finalize_queue (created_at, process, status)
					select $5, id, $9
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
					arg.checksum.as_ref().map(ToString::to_string),
					error,
					error_code,
					i64::from(arg.exit),
					arg.now,
					output,
					tg::process::Status::Finished.to_string(),
					id.to_string(),
					"created",
					condition,
					max_depth,
				],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(finished)
	}
}
