use {
	crate::Server,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_messenger::prelude::*,
};

pub mod create;
pub mod delete;
pub mod finish;
pub mod get;
pub mod heartbeat;
pub mod list;
pub mod queue;
pub mod status;

impl Server {
	pub(crate) async fn try_get_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::get::Output>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			hostname: Option<String>,
			#[tangram_database(as = "Option<db::value::Json<Vec<tg::sandbox::Mount>>>")]
			mounts: Option<Vec<tg::sandbox::Mount>>,
			network: bool,
			#[tangram_database(as = "db::value::FromStr")]
			status: tg::sandbox::Status,
			ttl: i64,
			user: Option<String>,
		}
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select hostname, mounts, network, status, ttl, \"user\" as user
				from sandboxes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let row = row.map(|row| tg::sandbox::get::Output {
			id: id.clone(),
			hostname: row.hostname,
			mounts: row.mounts.unwrap_or_default(),
			network: row.network,
			status: row.status,
			ttl: u64::try_from(row.ttl).unwrap(),
			user: row.user,
		});
		Ok(row)
	}

	pub(crate) async fn get_sandbox_exists_local(&self, id: &tg::sandbox::Id) -> tg::Result<bool> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
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
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);
		Ok(exists)
	}

	pub(crate) async fn try_start_sandbox_local(&self, id: &tg::sandbox::Id) -> tg::Result<bool> {
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update sandboxes
				set
					heartbeat_at = {p}1,
					started_at = case when started_at is null then {p}1 else started_at end,
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
		self.publish_sandbox_status(id);
		Ok(true)
	}

	pub(crate) async fn try_finish_sandbox_local(&self, id: &tg::sandbox::Id) -> tg::Result<bool> {
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update sandboxes
				set
					finished_at = {p}1,
					heartbeat_at = null,
					status = 'finished'
				where id = {p}2 and status != 'finished';
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
		self.publish_sandbox_status(id);
		Ok(true)
	}

	pub(crate) fn publish_sandbox_status(&self, id: &tg::sandbox::Id) {
		let subject = format!("sandboxes.{id}.status");
		tokio::spawn({
			let server = self.clone();
			async move {
				server.messenger.publish(subject, ()).await.ok();
			}
		});
	}

	pub(crate) async fn finish_unfinished_processes_in_sandbox(
		&self,
		id: &tg::sandbox::Id,
		remote: Option<&str>,
		error: tg::error::Data,
	) -> tg::Result<()> {
		if let Some(remote) = remote {
			self.finish_unfinished_processes_in_sandbox_remote(id, remote, error)
				.await
		} else {
			self.finish_unfinished_processes_in_sandbox_local(id, error)
				.await
		}
	}

	pub(crate) async fn finish_unfinished_processes_in_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
		error: tg::error::Data,
	) -> tg::Result<()> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::process::Id,
		}
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select id
				from processes
				where sandbox = {p}1 and status != 'finished';
			"
		);
		let params = db::params![id.to_string()];
		let rows = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);

		rows.into_iter()
			.map(|row| {
				let server = self.clone();
				let error = error.clone();
				async move {
					let arg = tg::process::finish::Arg {
						checksum: None,
						error: Some(tg::Either::Left(error)),
						exit: 1,
						local: Some(true),
						output: None,
						remotes: None,
					};
					server.finish_process(&row.id, arg).await.ok();
				}
				.boxed()
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(())
	}

	async fn finish_unfinished_processes_in_sandbox_remote(
		&self,
		id: &tg::sandbox::Id,
		remote: &str,
		error: tg::error::Data,
	) -> tg::Result<()> {
		let client = self.get_remote_client(remote.to_owned()).await.map_err(
			|source| tg::error!(!source, %id, %remote, "failed to get the remote client"),
		)?;
		let output = client
			.list_processes(tg::process::list::Arg::default())
			.await
			.map_err(
				|source| tg::error!(!source, %id, %remote, "failed to list the remote processes"),
			)?;

		output
			.data
			.into_iter()
			.filter(|output| {
				output.data.sandbox.as_ref() == Some(id) && !output.data.status.is_finished()
			})
			.map(|output| {
				let server = self.clone();
				let error = error.clone();
				let remote = remote.to_owned();
				async move {
					let arg = tg::process::finish::Arg {
						checksum: None,
						error: Some(tg::Either::Left(error)),
						exit: 1,
						local: None,
						output: None,
						remotes: Some(vec![remote]),
					};
					server.finish_process(&output.id, arg).await.ok();
				}
				.boxed()
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(())
	}
}
