use {
	crate::{
		Session,
		sandbox::destroy::{Condition, InnerArg, InnerOutput},
	},
	indoc::indoc,
	rusqlite as sqlite,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(super) async fn try_destroy_sandbox_inner_sqlite(
		&self,
		transaction: &db::sqlite::Transaction<'_>,
		id: &tg::sandbox::Id,
		arg: InnerArg,
	) -> tg::Result<ControlFlow<InnerOutput, db::sqlite::Error>> {
		let id = id.clone();
		transaction
			.with(move |transaction, _cache| {
				Self::try_destroy_sandbox_inner_sqlite_sync(transaction, &id, &arg)
			})
			.await
	}

	fn try_destroy_sandbox_inner_sqlite_sync(
		transaction: &mut sqlite::Transaction<'_>,
		id: &tg::sandbox::Id,
		arg: &InnerArg,
	) -> tg::Result<ControlFlow<InnerOutput, db::sqlite::Error>> {
		let (condition, max_heartbeat_at) = match arg.condition {
			Some(Condition::HeartbeatExpired { max_heartbeat_at }) => {
				(Some("heartbeat_expired"), Some(max_heartbeat_at))
			},
			None => (None, None),
		};
		let statement = indoc!(
			"
				update sandboxes
				set
					finished_at = ?1,
					heartbeat_at = null,
					status = ?2
				where
					id = ?3 and
					status != 'destroyed' and
					(
						?4 is null or
						(?4 = 'heartbeat_expired' and status = 'started' and heartbeat_at < ?5)
					);
			"
		);
		let result = transaction
			.execute(
				statement,
				sqlite::params![
					arg.now,
					tg::sandbox::Status::Destroyed.to_string(),
					id.to_string(),
					condition,
					max_heartbeat_at,
				],
			)
			.map_err(db::sqlite::Error::from);
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n != 1 {
			let output = InnerOutput {
				destroyed: false,
				unfinished_processes: Vec::new(),
			};
			return Ok(ControlFlow::Break(output));
		}

		let statement = indoc!(
			"
				insert into sandbox_finalize_queue (created_at, sandbox, status)
				values (?1, ?2, ?3);
			"
		);
		let result = transaction
			.execute(
				statement,
				sqlite::params![arg.now, id.to_string(), "created"],
			)
			.map_err(db::sqlite::Error::from);
		crate::database::retry!(result, "failed to execute the statement");

		let statement = indoc!(
			"
				select id
				from processes
				where
					sandbox = ?1 and
					status != 'finished'
				order by created_at, id;
			"
		);
		let mut statement = transaction
			.prepare(statement)
			.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params![id.to_string()])
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut unfinished_processes = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		{
			let process = row
				.get::<_, String>(0)
				.map_err(|error| tg::error!(!error, "failed to get the process"))?
				.parse()
				.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;
			unfinished_processes.push(process);
		}

		let output = InnerOutput {
			destroyed: true,
			unfinished_processes,
		};

		Ok(ControlFlow::Break(output))
	}
}
