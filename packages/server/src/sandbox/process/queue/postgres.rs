use {
	crate::{Server, Session},
	futures::FutureExt as _,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_dequeue_sandbox_process_postgres(
		&self,
		process_store: &db::postgres::Database,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		let sandbox = sandbox.to_string();
		process_store
			.run(|transaction| {
				let sandbox = sandbox.clone();
				async move {
					Self::try_dequeue_sandbox_process_postgres_with_transaction(
						transaction,
						&sandbox,
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the sandbox process"))
	}

	async fn try_dequeue_sandbox_process_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		sandbox: &str,
	) -> tg::Result<ControlFlow<Option<tg::sandbox::process::queue::Output>, db::postgres::Error>>
	{
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
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
		struct Row {
			id: String,
		}
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![sandbox.to_owned(), now])
			.await;
		let Some(row) = crate::database::retry!(result, "failed to execute the statement") else {
			return Ok(ControlFlow::Break(None));
		};
		let process: tg::process::Id = row
			.id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

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

		Ok(ControlFlow::Break(Some(
			tg::sandbox::process::queue::Output { process, token },
		)))
	}
}
