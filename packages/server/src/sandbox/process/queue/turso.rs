use {
	crate::{Server, Session},
	futures::FutureExt as _,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_dequeue_sandbox_process_turso(
		&self,
		process_store: &db::turso::Database,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		let sandbox = sandbox.to_string();
		process_store
			.run(|transaction| {
				let sandbox = sandbox.clone();
				async move {
					Self::try_dequeue_sandbox_process_turso_with_transaction(transaction, &sandbox)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the sandbox process"))
	}

	async fn try_dequeue_sandbox_process_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		sandbox: &str,
	) -> tg::Result<ControlFlow<Option<tg::sandbox::process::queue::Output>, db::turso::Error>> {
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
			.query_optional_into::<ProcessRow>(statement.into(), db::params![sandbox.to_owned()])
			.await;
		let process = crate::database::retry!(result, "failed to execute the statement")
			.map(|row| row.id.parse::<tg::process::Id>())
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;
		let Some(process) = process else {
			return Ok(ControlFlow::Break(None));
		};

		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let statement = indoc!(
			"
				update processes
				set
					started_at = ?1,
					status = 'dequeued'
				where id = ?2 and sandbox = ?3 and status = 'created';
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![now, process.to_string(), sandbox.to_owned()],
			)
			.await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n == 0 {
			return Ok(ControlFlow::Break(None));
		}

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

		Ok(ControlFlow::Break(Some(
			tg::sandbox::process::queue::Output { process, token },
		)))
	}
}
