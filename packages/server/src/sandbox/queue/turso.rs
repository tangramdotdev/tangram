use {
	crate::{Server, Session, sandbox::queue::LocalOutput},
	futures::FutureExt as _,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(super) async fn try_dequeue_sandbox_turso(
		&self,
		process_store: &db::turso::Database,
		token: bool,
	) -> tg::Result<Option<LocalOutput>> {
		process_store
			.run(|transaction| {
				async move {
					Self::try_dequeue_sandbox_turso_with_transaction(transaction, token).await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the sandbox"))
	}

	async fn try_dequeue_sandbox_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		token: bool,
	) -> tg::Result<ControlFlow<Option<LocalOutput>, db::turso::Error>> {
		let statement = indoc!(
			"
				select id, created_by
				from sandboxes
				where status = 'created'
				order by created_at, id
				limit 1;
			"
		);
		#[derive(db::row::Deserialize)]
		struct SandboxRow {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::sandbox::Id,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}
		let result = transaction
			.query_optional_into::<SandboxRow>(statement.into(), db::params![])
			.await;
		let Some(SandboxRow {
			id: sandbox,
			created_by,
		}) = crate::database::retry!(result, "failed to execute the statement")
		else {
			return Ok(ControlFlow::Break(None));
		};
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let statement = indoc!(
			"
				update sandboxes
				set
					heartbeat_at = ?1,
					started_at = coalesce(started_at, ?1),
					status = 'started'
				where id = ?2 and status = 'created';
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![now, sandbox.to_string()])
			.await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n == 0 {
			return Ok(ControlFlow::Break(None));
		}

		let statement = indoc!(
			"
				select id
				from processes
				where sandbox = ?1 and status = 'created'
				order by created_at, id
				limit 1;
			"
		);
		#[derive(db::row::Deserialize)]
		struct ProcessRow {
			id: String,
		}
		let result = transaction
			.query_optional_into::<ProcessRow>(statement.into(), db::params![sandbox.to_string()])
			.await;
		let process = crate::database::retry!(result, "failed to execute the statement")
			.map(|row| row.id.parse::<tg::process::Id>())
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;
		let process = if let Some(process) = process {
			let statement = indoc!(
				"
					update processes
					set
						started_at = ?1,
						status = 'started'
					where id = ?2 and sandbox = ?3 and status = 'created';
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![now, process.to_string(), sandbox.to_string()],
				)
				.await;
			let n = crate::database::retry!(result, "failed to execute the statement");
			if n == 0 {
				return Ok(ControlFlow::Break(None));
			}
			Some(process)
		} else {
			None
		};

		let process_token = if let Some(process) = &process {
			let token = Server::create_process_token_string();
			let statement = indoc!(
				"
					insert into process_tokens (process, token)
					values (?1, ?2);
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![process.to_string(), token.clone()],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
			Some(token)
		} else {
			None
		};

		let token = if token {
			let token = Self::create_sandbox_token_string();
			let statement = indoc!(
				"
					insert into sandbox_tokens (sandbox, token)
					values (?1, ?2);
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![sandbox.to_string(), token.clone()],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
			Some(token)
		} else {
			None
		};

		Ok(ControlFlow::Break(Some(LocalOutput {
			created_by,
			process,
			process_token,
			sandbox,
			token,
		})))
	}
}
