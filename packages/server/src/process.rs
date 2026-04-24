use {
	crate::{Server, database},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_messenger::prelude::*,
};

pub(crate) mod cancel;
pub(crate) mod children;
pub(crate) mod finalize;
pub(crate) mod finish;
pub(crate) mod get;
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
	async fn get_process_exists_local(&self, id: &tg::process::Id) -> tg::Result<bool> {
		// Get a database connection.
		let connection = self
			.process_store
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Check if the process exists.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count(*) != 0
				from processes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let exists = connection
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(exists)
	}

	pub(crate) async fn try_start_process_local(&self, id: &tg::process::Id) -> tg::Result<bool> {
		let connection = self
			.process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
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
		let params = db::params![now, id.to_string()];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);

		if n == 0 {
			return Ok(false);
		}

		let subject = format!("processes.{id}.status");
		self.messenger.publish(subject, ()).await.ok();

		Ok(true)
	}

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

	pub(crate) async fn try_lock_process_for_token_mutation_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Status>> {
		let p = transaction.p();

		// Touch the row to serialize token mutations for this process.
		let statement = formatdoc!(
			"
				update processes
				set touched_at = touched_at
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let n = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n == 0 {
			return Ok(None);
		}

		let statement = formatdoc!(
			"
				select status
				from processes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let status = transaction
			.query_one_value_into::<db::value::Serde<tg::process::Status>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.0;
		Ok(Some(status))
	}

	pub(crate) async fn update_process_token_count_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				update processes
				set token_count = (
					select count(*)
					from process_tokens
					where process = {p}1
				)
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}
}
