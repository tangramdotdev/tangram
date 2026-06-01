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
	pub(super) async fn try_destroy_sandbox_inner_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		id: &tg::sandbox::Id,
		arg: InnerArg,
	) -> tg::Result<ControlFlow<InnerOutput, db::postgres::Error>> {
		let (condition, max_heartbeat_at) = match arg.condition {
			Some(Condition::HeartbeatExpired { max_heartbeat_at }) => {
				(Some("heartbeat_expired"), Some(max_heartbeat_at))
			},
			None => (None, None),
		};
		#[derive(db::row::Deserialize)]
		struct Row {
			destroyed: bool,
			#[tangram_database(as = "db::value::Json<Vec<tg::process::Id>>")]
			unfinished_processes: Vec<tg::process::Id>,
		}
		let statement = indoc!(
			"
				with updated as (
					update sandboxes
					set
						finished_at = $1,
						heartbeat_at = null,
						status = $2
					where
						id = $3 and
						status != 'destroyed' and
						(
							$5::text is null or
							($5 = 'heartbeat_expired' and status = 'started' and heartbeat_at < $6)
						)
					returning id
				),
				enqueued as (
					insert into sandbox_finalize_queue (created_at, sandbox, status)
					select $1, id, $4
					from updated
					returning sandbox
				),
				unfinished_processes as (
					select processes.id, processes.created_at
					from processes
					where
						processes.sandbox in (select id from updated) and
						processes.status != 'finished'
				)
				select
					exists(select 1 from updated) as destroyed,
					(
						select coalesce(
							json_agg(id order by created_at, id),
							'[]'::json
						)
						from unfinished_processes
					) as unfinished_processes;
			"
		);
		let params = db::params![
			arg.now,
			tg::sandbox::Status::Destroyed.to_string(),
			id.to_string(),
			"created",
			condition,
			max_heartbeat_at,
		];
		let result = transaction
			.query_one_into::<Row>(statement.into(), params)
			.await;
		let Row {
			destroyed,
			unfinished_processes,
		} = crate::database::retry!(result, "failed to execute the statement");
		let output = InnerOutput {
			destroyed,
			unfinished_processes,
		};
		Ok(ControlFlow::Break(output))
	}
}
