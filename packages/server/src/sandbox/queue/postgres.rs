use {
	crate::{Server, Session, sandbox::queue::LocalOutput},
	futures::FutureExt as _,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(super) async fn try_dequeue_sandbox_postgres(
		&self,
		process_store: &db::postgres::Database,
		token: bool,
	) -> tg::Result<Option<LocalOutput>> {
		process_store
			.run(|transaction| {
				async move {
					Self::try_dequeue_sandbox_postgres_with_transaction(transaction, token).await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the sandbox"))
	}

	async fn try_dequeue_sandbox_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		token: bool,
	) -> tg::Result<ControlFlow<Option<LocalOutput>, db::postgres::Error>> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();

		let statement = indoc!(
			"
				with candidate as (
					select id, created_by
					from sandboxes
					where status = 'created'
					order by created_at, id
					limit 1
					for update skip locked
				)
				update sandboxes
				set
					heartbeat_at = $1,
					started_at = coalesce(started_at, $1),
					status = 'started'
				where id in (select id from candidate)
				returning id, created_by;
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
			.query_optional_into::<SandboxRow>(statement.into(), db::params![now])
			.await;
		let Some(SandboxRow {
			id: sandbox,
			created_by,
		}) = crate::database::retry!(result, "failed to execute the statement")
		else {
			return Ok(ControlFlow::Break(None));
		};

		let statement = indoc!(
			"
				with candidate as (
					select id
					from processes
					where sandbox = $1 and status = 'created'
					order by created_at, id
					limit 1
					for update skip locked
				)
				update processes
				set
					started_at = $2,
					status = 'started'
				where id in (select id from candidate)
				returning id;
			"
		);
		#[derive(db::row::Deserialize)]
		struct ProcessRow {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::process::Id,
		}
		let result = transaction
			.query_optional_into::<ProcessRow>(
				statement.into(),
				db::params![sandbox.to_string(), now],
			)
			.await;
		let process =
			crate::database::retry!(result, "failed to execute the statement").map(|row| row.id);

		let process_token = if let Some(process) = &process {
			let token = Server::create_process_token_string();
			let statement = indoc!(
				"
					insert into process_tokens (process, token)
					values ($1, $2);
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
					values ($1, $2);
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
