use crate::Server;
use futures::{future, stream, FutureExt as _, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use serde_with::serde_as;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_http::{response::builder::Ext as _, Body};
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
		struct Row {
			cwd: Option<PathBuf>,
			cacheable: bool,
			checksum: Option<tg::Checksum>,
			command: tg::command::Id,
			commands_complete: bool,
			#[serde(default)]
			commands_count: Option<u64>,
			#[serde(default)]
			commands_depth: Option<u64>,
			#[serde(default)]
			commands_weight: Option<u64>,
			complete: bool,
			#[serde(default)]
			count: Option<u64>,
			#[serde_as(as = "Rfc3339")]
			created_at: time::OffsetDateTime,
			#[serde(default)]
			#[serde_as(as = "Option<Rfc3339>")]
			dequeued_at: Option<time::OffsetDateTime>,
			#[serde(default)]
			#[serde_as(as = "Option<Rfc3339>")]
			enqueued_at: Option<time::OffsetDateTime>,
			env: Option<db::value::Json<BTreeMap<String, String>>>,
			error: Option<db::value::Json<tg::Error>>,
			exit: Option<db::value::Json<tg::process::Exit>>,
			#[serde(default)]
			#[serde_as(as = "Option<Rfc3339>")]
			finished_at: Option<time::OffsetDateTime>,
			#[serde(default)]
			#[serde_as(as = "Option<Rfc3339>")]
			heartbeat_at: Option<time::OffsetDateTime>,
			host: String,
			id: tg::process::Id,
			#[serde(default)]
			log: Option<tg::blob::Id>,
			logs_complete: bool,
			#[serde(default)]
			logs_count: Option<u64>,
			#[serde(default)]
			logs_depth: Option<u64>,
			#[serde(default)]
			logs_weight: Option<u64>,
			#[serde(default)]
			output: Option<db::value::Json<tg::value::Data>>,
			outputs_complete: bool,
			#[serde(default)]
			outputs_count: Option<u64>,
			#[serde(default)]
			outputs_depth: Option<u64>,
			#[serde(default)]
			outputs_weight: Option<u64>,
			retry: bool,
			#[serde(default)]
			network: bool,
			#[serde(default)]
			#[serde_as(as = "Option<Rfc3339>")]
			started_at: Option<time::OffsetDateTime>,
			status: tg::process::Status,
			#[serde(default)]
			#[serde_as(as = "Option<Rfc3339>")]
			touched_at: Option<time::OffsetDateTime>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					cacheable,
					checksum,
					command,
					commands_complete,
					commands_count,
					commands_weight,
					complete,
					count,
					created_at,
					cwd,
					dequeued_at,
					enqueued_at,
					env,
					error,
					exit,
					finished_at,
					heartbeat_at,
					host,
					id,
					log,
					logs_complete,
					logs_count,
					logs_weight,
					output,
					outputs_complete,
					outputs_count,
					outputs_weight,
					retry,
					network,
					started_at,
					status,
					touched_at
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
			cacheable: row.cacheable,
			checksum: row.checksum,
			command: row.command,
			commands_complete: row.commands_complete,
			commands_count: row.commands_count,
			commands_depth: row.commands_depth,
			commands_weight: row.commands_weight,
			complete: row.complete,
			count: row.count,
			created_at: row.created_at,
			cwd: row.cwd,
			dequeued_at: row.dequeued_at,
			enqueued_at: row.enqueued_at,
			env: row.env.map(|env| env.0),
			error: row.error.map(|error| error.0),
			exit: row.exit.map(|exit| exit.0),
			finished_at: row.finished_at,
			heartbeat_at: row.heartbeat_at,
			host: row.host,
			id: row.id,
			log: row.log,
			logs_complete: row.logs_complete,
			logs_count: row.logs_count,
			logs_depth: row.logs_depth,
			logs_weight: row.logs_weight,
			output: row.output.map(|output| output.0),
			outputs_complete: row.outputs_complete,
			outputs_count: row.outputs_count,
			outputs_depth: row.outputs_depth,
			outputs_weight: row.outputs_weight,
			retry: row.retry,
			network: row.network,
			started_at: row.started_at,
			status: row.status,
			touched_at: row.touched_at,
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
						checksum: output.checksum,
						children,
						command: output.command.clone(),
						created_at: output.created_at,
						cwd: output.cwd,
						dequeued_at: output.dequeued_at,
						enqueued_at: output.enqueued_at,
						env: output.env,
						error: output.error,
						finished_at: output.finished_at,
						host: output.host.clone(),
						id: output.id.clone(),
						log: output.log.clone(),
						network: output.network,
						output: output.output,
						retry: output.retry,
						started_at: output.started_at,
						status: output.status,
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
		_request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
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
