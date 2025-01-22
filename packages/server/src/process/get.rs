use crate::Server;
use futures::{future, stream, FutureExt as _, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use serde_with::serde_as;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn try_get_process(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::get::Output>> {
		if let Some(output) = self.try_get_process_local(id).await? {
			Ok(Some(output))
		} else if let Some(output) = self.try_get_process_remote(id).await? {
			Ok(Some(output))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn try_get_process_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::get::Output>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the process.
		#[serde_as]
		#[derive(serde::Deserialize)]
		pub struct Row {
			pub id: tg::process::Id,
			#[serde(default)]
			pub count: Option<u64>,
			pub depth: u64,
			pub error: Option<db::value::Json<tg::Error>>,
			pub exit: Option<db::value::Json<tg::process::Exit>>,
			pub host: String,
			#[serde(default)]
			pub log: Option<tg::blob::Id>,
			#[serde(default)]
			pub logs_count: Option<u64>,
			#[serde(default)]
			pub logs_depth: Option<u64>,
			#[serde(default)]
			pub logs_weight: Option<u64>,
			#[serde(default)]
			pub output: Option<db::value::Json<tg::value::Data>>,
			#[serde(default)]
			pub outputs_count: Option<u64>,
			#[serde(default)]
			pub outputs_depth: Option<u64>,
			#[serde(default)]
			pub outputs_weight: Option<u64>,
			pub retry: bool,
			pub status: tg::process::Status,
			pub command: tg::command::Id,
			#[serde(default)]
			pub commands_count: Option<u64>,
			#[serde(default)]
			pub commands_depth: Option<u64>,
			#[serde(default)]
			pub commands_weight: Option<u64>,
			#[serde_as(as = "Rfc3339")]
			pub created_at: time::OffsetDateTime,
			#[serde(default)]
			#[serde_as(as = "Option<Rfc3339>")]
			pub enqueued_at: Option<time::OffsetDateTime>,
			#[serde(default)]
			#[serde_as(as = "Option<Rfc3339>")]
			pub dequeued_at: Option<time::OffsetDateTime>,
			#[serde(default)]
			#[serde_as(as = "Option<Rfc3339>")]
			pub started_at: Option<time::OffsetDateTime>,
			#[serde(default)]
			#[serde_as(as = "Option<Rfc3339>")]
			pub finished_at: Option<time::OffsetDateTime>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					id,
					count,
					depth,
					error,
					exit,
					host,
					log,
					logs_complete,
					logs_count,
					logs_weight,
					output,
					outputs_complete,
					outputs_count,
					outputs_weight,
					retry,
					status,
					command,
					commands_complete,
					commands_count,
					commands_weight,
					created_at,
					enqueued_at,
					dequeued_at,
					started_at,
					finished_at
				from processes
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let output = row.map(|row| tg::process::get::Output {
			id: row.id,
			count: row.count,
			depth: row.depth,
			error: row.error.map(|error| error.0),
			exit: row.exit.map(|exit| exit.0),
			host: row.host,
			log: row.log,
			logs_count: row.logs_count,
			logs_depth: row.logs_depth,
			logs_weight: row.logs_weight,
			output: row.output.map(|output| output.0),
			outputs_count: row.outputs_count,
			outputs_depth: row.outputs_depth,
			outputs_weight: row.outputs_weight,
			retry: row.retry,
			status: row.status,
			command: row.command,
			commands_count: row.commands_count,
			commands_depth: row.commands_depth,
			commands_weight: row.commands_weight,
			created_at: row.created_at,
			enqueued_at: row.enqueued_at,
			dequeued_at: row.dequeued_at,
			started_at: row.started_at,
			finished_at: row.finished_at,
		});

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	async fn try_get_process_remote(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::get::Output>> {
		// Attempt to get the process from the remotes.
		let futures = self
			.get_remote_clients()
			.await?
			.into_values()
			.map(|client| async move { client.get_process(id).await }.boxed())
			.collect_vec();
		if futures.is_empty() {
			return Ok(None);
		}
		let Ok((output, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};

		// Spawn a task to put the process if it is finished.
		if output.status.is_finished() {
			tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				let output = output.clone();
				async move {
					let arg = tg::process::children::get::Arg::default();
					let children = server
						.try_get_process_children(&id, arg)
						.await?
						.ok_or_else(|| tg::error!("expected the process to exist"))?
						.map_ok(|chunk| stream::iter(chunk.data).map(Ok::<_, tg::Error>))
						.try_flatten()
						.try_collect()
						.await?;
					let arg = tg::process::put::Arg {
						id: output.id.clone(),
						children,
						depth: output.depth,
						error: output.error,
						host: output.host.clone(),
						log: output.log.clone(),
						output: output.output,
						retry: output.retry,
						status: output.status,
						command: output.command.clone(),
						created_at: output.created_at,
						enqueued_at: output.enqueued_at,
						dequeued_at: output.dequeued_at,
						started_at: output.started_at,
						finished_at: output.finished_at,
					};
					server.put_process(&id, arg).await?;
					Ok::<_, tg::Error>(())
				}
			});
		}

		Ok(Some(output))
	}
}

impl Server {
	pub(crate) async fn handle_get_process_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(output) = handle.try_get_process(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
