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
	pub async fn try_get_build(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<tg::build::get::Output>> {
		if let Some(output) = self.try_get_build_local(id).await? {
			Ok(Some(output))
		} else if let Some(output) = self.try_get_build_remote(id).await? {
			Ok(Some(output))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn try_get_build_local(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<tg::build::get::Output>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the build.
		#[serde_as]
		#[derive(serde::Deserialize)]
		pub struct Row {
			pub id: tg::build::Id,
			#[serde(default)]
			pub count: Option<u64>,
			pub depth: u64,
			pub error: Option<db::value::Json<tg::Error>>,
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
			pub retry: tg::build::Retry,
			pub status: tg::build::Status,
			pub target: tg::target::Id,
			#[serde(default)]
			pub targets_count: Option<u64>,
			#[serde(default)]
			pub targets_depth: Option<u64>,
			#[serde(default)]
			pub targets_weight: Option<u64>,
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
					target,
					targets_complete,
					targets_count,
					targets_weight,
					created_at,
					enqueued_at,
					dequeued_at,
					started_at,
					finished_at
				from builds
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let row = connection
			.query_optional_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let output = row.map(|row| tg::build::get::Output {
			id: row.id,
			count: row.count,
			depth: row.depth,
			error: row.error.map(|error| error.0),
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
			target: row.target,
			targets_count: row.targets_count,
			targets_depth: row.targets_depth,
			targets_weight: row.targets_weight,
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

	async fn try_get_build_remote(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<tg::build::get::Output>> {
		// Attempt to get the build from the remotes.
		let futures = self
			.get_remote_clients()
			.await?
			.into_values()
			.map(|client| async move { client.get_build(id).await }.boxed())
			.collect_vec();
		if futures.is_empty() {
			return Ok(None);
		}
		let Ok((output, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};

		// Spawn a task to put the build if it is finished.
		if output.status.is_finished() {
			tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				let output = output.clone();
				async move {
					let arg = tg::build::children::get::Arg::default();
					let children = server
						.try_get_build_children(&id, arg)
						.await?
						.ok_or_else(|| tg::error!("expected the build to exist"))?
						.map_ok(|chunk| stream::iter(chunk.data).map(Ok::<_, tg::Error>))
						.try_flatten()
						.try_collect()
						.await?;
					let arg = tg::build::put::Arg {
						id: output.id.clone(),
						children,
						depth: output.depth,
						error: output.error,
						host: output.host.clone(),
						log: output.log.clone(),
						output: output.output,
						retry: output.retry,
						status: output.status,
						target: output.target.clone(),
						created_at: output.created_at,
						enqueued_at: output.enqueued_at,
						dequeued_at: output.dequeued_at,
						started_at: output.started_at,
						finished_at: output.finished_at,
					};
					server.put_build(&id, arg).await?;
					Ok::<_, tg::Error>(())
				}
			});
		}

		Ok(Some(output))
	}
}

impl Server {
	pub(crate) async fn handle_get_build_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(output) = handle.try_get_build(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
