use {
	crate::{Server, Session, database},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_messenger::prelude::*,
};

pub(crate) mod cancel;
pub(crate) mod children;
pub(crate) mod finalize;
pub(crate) mod finish;
pub(crate) mod get;
pub(crate) mod grants;
pub(crate) mod list;
pub(crate) mod metadata;
pub(crate) mod put;
pub(crate) mod signal;
pub(crate) mod spawn;
pub(crate) mod status;
pub(crate) mod stdio;
pub(crate) mod store;
pub(crate) mod touch;
pub(crate) mod tty;
pub(crate) mod wait;

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
		let connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		// Check if the process exists.
		let p = connection.p();
		let sandbox = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_sandbox_ref().ok())
			.map(|sandbox| sandbox.id.clone());
		let principal = if sandbox.is_some() {
			tg::Principal::Root
		} else {
			self.read_principal()
		};
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let join = if principal.is_root() {
			String::new()
		} else {
			formatdoc!(
				"
					join process_grants on
						process_grants.process = processes.id and
						process_grants.principal = {p}2 and
						process_grants.expires_at > {p}3
				"
			)
		};
		let sandbox_condition = if principal.is_root() && sandbox.is_some() {
			format!("and processes.sandbox = {p}2")
		} else {
			String::new()
		};
		let statement = formatdoc!(
			"
				select count(*) != 0
				from processes
				{join}
				where processes.id = {p}1
					{sandbox_condition};
			"
		);
		let params = if let Some(sandbox) = sandbox {
			db::params![id.to_string(), sandbox.to_string()]
		} else if principal.is_root() {
			db::params![id.to_string()]
		} else {
			db::params![id.to_string(), principal.to_string(), now]
		};
		let exists = connection
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(exists)
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
