use {
	crate::{Server, Session, database},
	dashmap::DashMap,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_messenger::prelude::*,
};

pub mod create;
pub mod destroy;
pub mod finalize;
pub mod get;
pub mod heartbeat;
pub mod isolation;
pub mod list;
pub mod process;
pub mod queue;
pub mod status;

pub type Map = DashMap<tg::sandbox::Id, tangram_sandbox::Sandbox, tg::id::BuildHasher>;

pub type Permits =
	DashMap<tg::sandbox::Id, Arc<tokio::sync::Mutex<Option<Permit>>>, tg::id::BuildHasher>;

pub struct Permit(
	#[expect(dead_code)]
	pub  tg::Either<tokio::sync::OwnedSemaphorePermit, tokio::sync::OwnedMutexGuard<Option<Self>>>,
);

pub type Tasks = tangram_futures::task::Map<tg::sandbox::Id, (), (), tg::id::BuildHasher>;

impl Session {
	pub(super) fn create_sandbox_token_string() -> String {
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
	}
}

impl Server {
	pub(crate) async fn delete_sandbox_tokens_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from sandbox_tokens
				where sandbox = {p}1;
			"
		);
		let params = db::params![sandbox.to_string()];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn get_sandbox_exists_local(&self, id: &tg::sandbox::Id) -> tg::Result<bool> {
		let connection = self
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count(*) != 0
				from sandboxes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let exists = connection
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		drop(connection);
		Ok(exists)
	}

	pub(crate) async fn try_start_sandbox_local(&self, id: &tg::sandbox::Id) -> tg::Result<bool> {
		let sandbox = id.to_string();
		let n = self
			.process_store
			.run(|transaction| {
				let sandbox = sandbox.clone();
				async move { Self::try_start_sandbox_with_transaction(transaction, &sandbox).await }
					.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to start the sandbox"))?;
		if n == 0 {
			return Ok(false);
		}
		self.spawn_publish_sandbox_status_task(id);
		Ok(true)
	}

	async fn try_start_sandbox_with_transaction(
		transaction: &database::Transaction<'_>,
		sandbox: &str,
	) -> tg::Result<ControlFlow<u64, database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				update sandboxes
				set
					heartbeat_at = {p}1,
					started_at = case when started_at is null then {p}1 else started_at end,
					status = 'started'
				where
					id = {p}2 and
					status = 'created';
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![now, sandbox];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(n))
	}

	pub(crate) fn spawn_publish_sandbox_status_task(&self, id: &tg::sandbox::Id) {
		let subject = format!("sandboxes.{id}.status");
		tokio::spawn({
			let server = self.clone();
			async move {
				let result = server.messenger.publish(subject, ()).await;
				if let Err(error) = result {
					tracing::error!(%error, "failed to publish the sandbox status message");
				}
			}
		});
	}

	pub(crate) fn validate_sandbox_resources(
		isolation: &tangram_sandbox::Isolation,
		cpu: Option<u64>,
		memory: Option<u64>,
		hostname: Option<&str>,
	) -> tg::Result<()> {
		if cpu == Some(0) {
			return Err(tg::error!("sandbox cpu must be greater than zero"));
		}
		if memory == Some(0) {
			return Err(tg::error!("sandbox memory must be greater than zero"));
		}
		if matches!(isolation, tangram_sandbox::Isolation::Seatbelt(_))
			&& (cpu.is_some() || memory.is_some())
		{
			return Err(tg::error!(
				"sandbox cpu and memory are not supported with seatbelt isolation"
			));
		}
		if matches!(isolation, tangram_sandbox::Isolation::Seatbelt(_)) && hostname.is_some() {
			return Err(tg::error!(
				"setting a hostname is not supported with seatbelt isolation"
			));
		}
		Ok(())
	}
}
