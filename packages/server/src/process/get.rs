use crate::Server;
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream};
use indoc::formatdoc;
use itertools::Itertools as _;
use serde_with::serde_as;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, response::builder::Ext as _};
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
			host: String,
			id: tg::process::Id,
			#[serde(default)]
			log: Option<tg::blob::Id>,
			#[serde(default)]
			output: Option<db::value::Json<tg::value::Data>>,
			retry: bool,
			#[serde(default)]
			mounts: Option<db::value::Json<Vec<tg::process::data::Mount>>>,
			#[serde(default)]
			network: bool,
			#[serde(default)]
			#[serde_as(as = "Option<Rfc3339>")]
			started_at: Option<time::OffsetDateTime>,
			status: tg::process::Status,
			#[serde(default)]
			stderr: Option<tg::process::Stdio>,
			#[serde(default)]
			stdin: Option<tg::process::Stdio>,
			#[serde(default)]
			stdout: Option<tg::process::Stdio>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					cacheable,
					checksum,
					command,
					created_at,
					cwd,
					dequeued_at,
					enqueued_at,
					env,
					error,
					exit,
					finished_at,
					host,
					id,
					log,
					output,
					retry,
					mounts,
					network,
					started_at,
					status,
					stderr,
					stdin,
					stdout
				from processes
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let output = row.map(|row| {
			let data = tg::process::Data {
				cacheable: row.cacheable,
				checksum: row.checksum,
				children: None,
				command: row.command,
				created_at: row.created_at,
				cwd: row.cwd,
				dequeued_at: row.dequeued_at,
				enqueued_at: row.enqueued_at,
				env: row.env.map(|env| env.0),
				error: row.error.map(|error| error.0),
				exit: row.exit.map(|exit| exit.0),
				finished_at: row.finished_at,
				host: row.host,
				id: row.id,
				log: row.log,
				output: row.output.map(|output| output.0),
				retry: row.retry,
				mounts: row.mounts.map(|output| output.0).unwrap_or_default(),
				network: row.network,
				started_at: row.started_at,
				status: row.status,
				stderr: row.stderr,
				stdin: row.stdin,
				stdout: row.stdout,
			};
			tg::process::get::Output { data }
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
		if output.data.status.is_finished() {
			tokio::spawn({
				let server = self.clone();
				let id = id.clone();
				let mut data = output.data.clone();
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
					data.children = Some(children);
					let arg = tg::process::put::Arg { data };
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
		let response = http::Response::builder().json(output.data).unwrap();
		Ok(response)
	}
}
