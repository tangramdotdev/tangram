use {
	crate::{
		Session,
		sandbox::destroy::{Condition, InnerArg, InnerOutput},
	},
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(super) async fn try_destroy_sandbox_inner_turso(
		&self,
		transaction: &db::turso::Transaction<'_>,
		id: &tg::sandbox::Id,
		arg: InnerArg,
	) -> tg::Result<ControlFlow<InnerOutput, db::turso::Error>> {
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
				statement.into(),
				db::params![
					arg.now,
					tg::sandbox::Status::Destroyed.to_string(),
					id.to_string(),
					condition,
					max_heartbeat_at,
				],
			)
			.await;
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
				statement.into(),
				db::params![arg.now, id.to_string(), "created"],
			)
			.await;
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
		#[derive(db::row::Deserialize)]
		struct ProcessRow {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::process::Id,
		}
		let result = transaction
			.query_all_into::<ProcessRow>(statement.into(), db::params![id.to_string()])
			.await;
		let unfinished_processes =
			crate::database::retry!(result, "failed to execute the statement")
				.into_iter()
				.map(|row| row.id)
				.collect();

		let output = InnerOutput {
			destroyed: true,
			unfinished_processes,
		};

		Ok(ControlFlow::Break(output))
	}
}
