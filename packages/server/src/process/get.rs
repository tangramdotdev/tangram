use crate::Server;
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future, stream};
use indoc::{formatdoc, indoc};
use itertools::Itertools as _;
use rusqlite::{self as sqlite, fallible_streaming_iterator::FallibleStreamingIterator as _};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, response::builder::Ext as _};

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
		#[derive(serde::Deserialize)]
		struct Row {
			actual_checksum: Option<tg::Checksum>,
			cacheable: bool,
			command: tg::command::Id,
			created_at: i64,
			dequeued_at: Option<i64>,
			enqueued_at: Option<i64>,
			error: Option<db::value::Json<tg::Error>>,
			exit: Option<u8>,
			expected_checksum: Option<tg::Checksum>,
			finished_at: Option<i64>,
			host: String,
			id: tg::process::Id,
			log: Option<tg::blob::Id>,
			output: Option<db::value::Json<tg::value::Data>>,
			retry: bool,
			mounts: Option<db::value::Json<Vec<tg::process::data::Mount>>>,
			network: bool,
			started_at: Option<i64>,
			status: tg::process::Status,
			stderr: Option<tg::process::Stdio>,
			stdin: Option<tg::process::Stdio>,
			stdout: Option<tg::process::Stdio>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					actual_checksum,
					cacheable,
					command,
					created_at,
					dequeued_at,
					enqueued_at,
					error,
					exit,
					expected_checksum,
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
				actual_checksum: row.actual_checksum,
				cacheable: row.cacheable,
				children: None,
				command: row.command,
				created_at: row.created_at,
				dequeued_at: row.dequeued_at,
				enqueued_at: row.enqueued_at,
				error: row.error.map(|error| error.0),
				exit: row.exit,
				expected_checksum: row.expected_checksum,
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

	pub(crate) fn try_get_process_local_sync(
		database: &sqlite::Connection,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Data>> {
		// Get the process.
		let statement = indoc!(
			"
				select
					actual_checksum,
					cacheable,
					command,
					created_at,
					dequeued_at,
					enqueued_at,
					error,
					exit,
					expected_checksum,
					finished_at,
					host,
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
				where id = ?1;
			"
		);
		let mut statement = database
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
		let mut rows = statement
			.query([id.to_string()])
			.map_err(|source| tg::error!(!source, "query failed"))?;
		rows.advance()
			.map_err(|source| tg::error!(!source, "query failed"))?;
		let Some(row) = rows.get() else {
			return Ok(None);
		};

		// Deserialize the row.
		let actual_checksum = row
			.get::<_, Option<String>>(0)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;
		let cacheable = row
			.get::<_, u64>(1)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			!= 0;
		let command = row
			.get::<_, String>(2)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.parse()?;
		let created_at = row
			.get::<_, i64>(3)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let dequeued_at = row
			.get::<_, Option<i64>>(4)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let enqueued_at = row
			.get::<_, Option<i64>>(5)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let error = row
			.get::<_, Option<String>>(6)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| serde_json::from_str(&s))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize"))?;
		let exit = row
			.get::<_, Option<u8>>(7)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let expected_checksum = row
			.get::<_, Option<String>>(8)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;
		let finished_at = row
			.get::<_, Option<i64>>(9)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let host = row
			.get::<_, String>(10)
			.map_err(|source| tg::error!(!source, "expected a string"))?;
		let log = row
			.get::<_, Option<String>>(11)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;
		let output = row
			.get::<_, Option<String>>(12)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| serde_json::from_str(&s))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize"))?;
		let retry = row
			.get::<_, u64>(13)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			!= 0;
		let mounts = row
			.get::<_, Option<String>>(14)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| serde_json::from_str(&s))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize"))?
			.unwrap_or_default();
		let network = row
			.get::<_, u64>(15)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			!= 0;
		let started_at = row
			.get::<_, Option<i64>>(16)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let status = row
			.get::<_, String>(17)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.parse()?;
		let stderr = row
			.get::<_, Option<String>>(18)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;
		let stdin = row
			.get::<_, Option<String>>(19)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;
		let stdout = row
			.get::<_, Option<String>>(20)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;

		// Get the children.
		let statement = indoc!(
			"
				select child
				from process_children
				where process = ?1;
			"
		);
		let mut statement = database
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query([id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?;
		let mut children = Vec::new();
		rows.advance()
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?;
		while let Some(row) = rows.get() {
			let id = row
				.get::<_, String>(0)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.parse()?;
			children.push(id);
			rows.advance()
				.map_err(|source| tg::error!(!source, "query failed"))?;
		}

		let data = tg::process::Data {
			actual_checksum,
			cacheable,
			children: Some(children),
			command,
			created_at,
			dequeued_at,
			enqueued_at,
			error,
			exit,
			expected_checksum,
			finished_at,
			host,
			id: id.clone(),
			log,
			output,
			retry,
			mounts,
			network,
			started_at,
			status,
			stderr,
			stdin,
			stdout,
		};

		Ok(Some(data))
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
