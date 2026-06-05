use {
	crate::{Server, Session, authorization, database},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_messenger::prelude::*,
};

pub mod cancel;
pub mod children;
pub mod control;
pub mod finalize;
pub mod finish;
pub mod get;
pub mod grants;
pub mod list;
pub mod metadata;
pub mod put;
pub mod signal;
pub mod spawn;
pub mod status;
pub mod stdio;
pub mod store;
pub mod touch;
pub mod tty;
pub mod wait;

impl Server {
	pub(crate) async fn create_process_token(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<String> {
		let token = Self::create_process_token_string();
		let process = process.to_string();
		self.process_store
			.run(|transaction| {
				let process = process.clone();
				let token = token.clone();
				async move {
					Self::create_process_token_with_transaction(transaction, &process, &token).await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to create the process token"))?;
		Ok(token)
	}

	async fn create_process_token_with_transaction(
		transaction: &database::Transaction<'_>,
		process: &str,
		token: &str,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into process_tokens (process, token)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![process, token];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}

	pub(crate) fn create_process_token_string() -> String {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
	}

	pub(crate) async fn delete_sandbox_process_tokens_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from process_tokens
				where process in (
					select id
					from processes
					where sandbox = {p}1
				);
			"
		);
		let params = db::params![sandbox.to_string()];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn try_start_process_local(&self, id: &tg::process::Id) -> tg::Result<bool> {
		let id = id.to_string();
		let n = self
			.process_store
			.run(|transaction| {
				let id = id.clone();
				async move { Self::try_start_process_with_transaction(transaction, &id).await }
					.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to start the process"))?;

		if n == 0 {
			return Ok(false);
		}

		let subject = format!("processes.{id}.status");
		self.messenger.publish(subject, ()).await.ok();

		Ok(true)
	}

	async fn try_start_process_with_transaction(
		transaction: &database::Transaction<'_>,
		id: &str,
	) -> tg::Result<ControlFlow<u64, database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				update processes
				set
					started_at = {p}1,
					status = 'started'
				where id = {p}2 and status = 'created';
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![now, id];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(n))
	}
}

impl Session {
	pub(crate) async fn get_process_exists_local(&self, id: &tg::process::Id) -> tg::Result<bool> {
		// Get a database connection.
		let mut connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;

		// Check if the process exists.
		let p = transaction.p();
		let sandbox = match &self.context.principal {
			Some(tg::Principal::Sandbox(sandbox)) => Some(sandbox.clone()),
			_ => None,
		};
		let principal = if sandbox.is_some() {
			Some(tg::Principal::Root)
		} else {
			self.context.principal.clone()
		};
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let sandbox_condition =
			if matches!(principal, Some(tg::Principal::Root)) && sandbox.is_some() {
				format!("and processes.sandbox = {p}2")
			} else {
				String::new()
			};
		let statement = formatdoc!(
			"
				select count(*) != 0
				from processes
				where
					processes.id = {p}1
					{sandbox_condition};
			"
		);
		let params = if let Some(sandbox) = sandbox {
			db::params![id.to_string(), sandbox.to_string()]
		} else {
			db::params![id.to_string()]
		};
		let exists_future = async {
			transaction
				.query_one_value_into(statement.into(), params)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))
		};
		let grants_future = Self::try_get_process_grants_for_authorization(
			&transaction,
			id,
			principal.as_ref(),
			now,
		);
		let (exists, grants) = futures::try_join!(exists_future, grants_future)?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;

		// Drop the database connection.
		drop(connection);

		Ok(exists && self.authorize_process(id, &grants))
	}

	async fn try_get_process_grants_for_authorization(
		connection: &impl db::Query<Error = database::Error>,
		id: &tg::process::Id,
		principal: Option<&tg::Principal>,
		now: i64,
	) -> tg::Result<Vec<authorization::ProcessGrant>> {
		let Some(principal) = principal else {
			return Ok(Vec::new());
		};
		if matches!(principal, tg::Principal::Root) {
			return Ok(Vec::new());
		}
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			expires_at: i64,
			node: bool,
			node_command: bool,
			node_error: bool,
			node_log: bool,
			node_output: bool,
			principal: String,
			process: String,
			subtree: bool,
			subtree_command: bool,
			subtree_error: bool,
			subtree_log: bool,
			subtree_output: bool,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					created_at,
					expires_at,
					node,
					node_command,
					node_error,
					node_log,
					node_output,
					principal,
					process,
					subtree,
					subtree_command,
					subtree_error,
					subtree_log,
					subtree_output
				from process_grants
				where process = {p}1
					and principal = {p}2
					and expires_at > {p}3;
			"
		);
		let rows = connection
			.query_all_into::<Row>(
				statement.into(),
				db::params![id.to_string(), principal.to_string(), now],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let grants = rows
			.into_iter()
			.map(|row| {
				Ok(authorization::ProcessGrant {
					created_at: row.created_at,
					expires_at: row.expires_at,
					node: row.node,
					node_command: row.node_command,
					node_error: row.node_error,
					node_log: row.node_log,
					node_output: row.node_output,
					principal: row
						.principal
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse the principal"))?,
					process: row
						.process
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse the process"))?,
					subtree: row.subtree,
					subtree_command: row.subtree_command,
					subtree_error: row.subtree_error,
					subtree_log: row.subtree_log,
					subtree_output: row.subtree_output,
				})
			})
			.collect::<tg::Result<_>>()?;
		Ok(grants)
	}
}

impl Server {
	fn spawn_publish_process_status_task(&self, id: &tg::process::Id) {
		let subject = format!("processes.{id}.status");
		tokio::spawn({
			let server = self.clone();
			async move {
				let result = server.messenger.publish(subject, ()).await;
				if let Err(error) = result {
					tracing::error!(%error, "failed to publish the process status message");
				}
			}
		});
	}

	pub(crate) async fn try_lock_process_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		id: &tg::process::Id,
	) -> tg::Result<ControlFlow<Option<tg::process::Status>, database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				update processes
				set stored_at = stored_at
				where id = {p}1
				returning status;
			"
		);
		let params = db::params![id.to_string()];
		let result = transaction
			.query_optional_value_into::<db::value::Serde<tg::process::Status>>(
				statement.into(),
				params,
			)
			.await;
		let status = crate::database::retry!(result, "failed to lock the process");
		Ok(ControlFlow::Break(status.map(|status| status.0)))
	}

	pub(crate) async fn update_process_lease_count_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		id: &tg::process::Id,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				update processes
				set lease_count = (
					select count(*)
					from process_leases
					where process = {p}1
				)
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to update the lease count");
		Ok(ControlFlow::Break(()))
	}
}
