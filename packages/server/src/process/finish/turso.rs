use {
	crate::{
		Session,
		process::finish::{Condition, InnerArg},
	},
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(super) async fn try_finish_process_inner_turso(
		&self,
		transaction: &db::turso::Transaction<'_>,
		id: &tg::process::Id,
		arg: InnerArg,
	) -> tg::Result<ControlFlow<bool, db::turso::Error>> {
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
			.map_err(|error| tg::error!(!error, "failed to serialize the output"))?;
		let (condition, max_depth) = match arg.condition {
			Some(Condition::DepthExceeded { max_depth }) => {
				(Some("depth_exceeded"), Some(max_depth))
			},
			Some(Condition::LeaseCountZero) => (Some("lease_count_zero"), None),
			None => (None, None),
		};
		let checksum = arg.checksum.as_ref().map(ToString::to_string);
		let statement = indoc!(
			"
				update processes
				set
					actual_checksum = ?1,
					depth = null,
					error = ?2,
					error_code = ?3,
					exit = ?4,
					finished_at = ?5,
					lease_count = 0,
					output = ?6,
					status = ?7,
					stderr_open = case when stderr_open is null then null else false end,
					stdin_open = case when stdin_open is null then null else false end,
					stdout_open = case when stdout_open is null then null else false end,
					stored_at = ?5
				where
					id = ?8 and
					status != 'finished' and
					(
						?9 is null or
						(?9 = 'depth_exceeded' and depth > ?10) or
						(?9 = 'lease_count_zero' and lease_count = 0)
					);
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![
					checksum.as_deref(),
					error.as_deref(),
					error_code.as_deref(),
					i64::from(arg.exit),
					arg.now,
					output.as_deref(),
					tg::process::Status::Finished.to_string(),
					id.to_string(),
					condition,
					max_depth,
				],
			)
			.await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n != 1 {
			return Ok(ControlFlow::Break(false));
		}

		let statement = indoc!(
			"
				delete from process_leases
				where process = ?1;
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![id.to_string()])
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		let statement = indoc!(
			"
				insert into process_finalize_queue (created_at, process, status)
				values (?1, ?2, ?3);
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![arg.now, id.to_string(), "created"],
			)
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(true))
	}
}
