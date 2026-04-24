use {
	crate::{
		Server,
		sandbox::finish::{Condition, InnerArg, InnerOutput},
	},
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(super) async fn try_finish_sandbox_inner_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		id: &tg::sandbox::Id,
		arg: InnerArg,
	) -> tg::Result<InnerOutput> {
		let (condition, max_heartbeat_at) = match arg.condition {
			Some(Condition::HeartbeatExpired { max_heartbeat_at }) => {
				(Some("heartbeat_expired"), Some(max_heartbeat_at))
			},
			None => (None, None),
		};
		#[derive(db::row::Deserialize)]
		struct Row {
			finished: bool,
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
						status != 'finished' and
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
					exists(select 1 from updated) as finished,
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
			tg::sandbox::Status::Finished.to_string(),
			id.to_string(),
			"created",
			condition,
			max_heartbeat_at,
		];
		let Row {
			finished,
			unfinished_processes,
		} = transaction
			.query_one_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let output = InnerOutput {
			finished,
			unfinished_processes,
		};
		Ok(output)
	}
}
