use super::Server;
use futures::{Stream, StreamExt as _};
use indoc::{formatdoc, indoc};
use num::ToPrimitive as _;
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::prelude::*;
use tokio_util::task::AbortOnDropHandle;

struct InnerOutput {
	cache_entries: Vec<tg::artifact::Id>,
	objects: Vec<tg::object::Id>,
	processes: Vec<tg::process::Id>,
}

#[derive(Debug, serde::Deserialize)]
struct Count {
	cache_entries: u64,
	objects: u64,
	processes: u64,
}

impl Server {
	pub async fn clean(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let progress = crate::progress::Handle::new();
		let task = AbortOnDropHandle::new(tokio::spawn({
			let progress = progress.clone();
			let server = self.clone();
			async move {
				// Clean the temporary directory.
				crate::util::fs::remove(server.temp_path())
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to remove the temporary directory")
					})?;
				tokio::fs::create_dir_all(server.temp_path())
					.await
					.map_err(|error| {
						tg::error!(source = error, "failed to recreate the temporary directory")
					})?;

				let count = server.clean_count_items().await?;

				if count.cache_entries > 0 {
					progress.start(
						"cache".into(),
						"cache entries".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.cache_entries),
					);
				}
				if count.objects > 0 {
					progress.start(
						"objects".into(),
						"objects".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.objects),
					);
				}
				if count.processes > 0 {
					progress.start(
						"processes".into(),
						"processes".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.processes),
					);
				}

				loop {
					let now = time::OffsetDateTime::now_utc().unix_timestamp();
					let ttl = Duration::from_secs(0);
					let n = server
						.config
						.cleaner
						.as_ref()
						.map_or(1024, |config| config.batch_size);
					let output = match server.cleaner_task_inner(now, ttl, n).await {
						Ok(output) => output,
						Err(error) => {
							progress.error(error);
							break;
						},
					};
					progress.increment("cache", output.cache_entries.len().to_u64().unwrap());
					progress.increment("objects", output.objects.len().to_u64().unwrap());
					progress.increment("processes", output.processes.len().to_u64().unwrap());
					let n =
						output.cache_entries.len() + output.objects.len() + output.processes.len();
					if n == 0 {
						break;
					}
				}

				progress.output(());

				Ok::<_, tg::Error>(())
			}
		}));
		let stream = progress.stream().attach(task);
		Ok(stream)
	}

	pub(crate) async fn cleaner_task(&self, config: &crate::config::Cleaner) -> tg::Result<()> {
		loop {
			let now = time::OffsetDateTime::now_utc().unix_timestamp();
			let ttl = config.ttl;
			let n = config.batch_size;
			let result = self.cleaner_task_inner(now, ttl, n).await;
			match result {
				Ok(output) => {
					let n =
						output.cache_entries.len() + output.objects.len() + output.processes.len();
					if n == 0 {
						tokio::time::sleep(Duration::from_secs(1)).await;
					}
				},
				Err(error) => {
					tracing::error!(?error, "failed to clean");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	async fn clean_count_items(&self) -> tg::Result<Count> {
		match &self.index {
			crate::index::Index::Sqlite(database) => self.clean_count_items_sqlite(database).await,
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => self.clean_count_items_postgres(database).await,
		}
	}

	async fn clean_count_items_sqlite(&self, database: &db::sqlite::Database) -> tg::Result<Count> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get an index connection"))?;
		let statement = indoc!(
			"
				select
					(select count(*) from cache_entries) as cache_entries,
					(select count(*) from objects) as objects,
					(select count(*) from processes) as processes;
				;
			"
		);
		let params = db::params![];
		let count = connection
			.query_one_into::<Count>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(count)
	}

	#[cfg(feature = "postgres")]
	async fn clean_count_items_postgres(
		&self,
		database: &db::postgres::Database,
	) -> tg::Result<Count> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get an index connection"))?;
		let statement = indoc!(
			"
				select
					(select count(*) from cache_entries) as cache_entries,
					(select count(*) from objects) as objects,
					(select count(*) from processes) as processes;
				;
			"
		);
		let params = db::params![];
		let count = connection
			.query_one_into::<Count>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(count)
	}

	async fn cleaner_task_inner(
		&self,
		now: i64,
		ttl: std::time::Duration,
		n: usize,
	) -> tg::Result<InnerOutput> {
		match &self.index {
			crate::index::Index::Sqlite(database) => {
				self.cleaner_task_inner_sqlite(database, now, ttl, n).await
			},
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.cleaner_task_inner_postgres(database, now, ttl, n)
					.await
			},
		}
	}

	async fn cleaner_task_inner_sqlite(
		&self,
		database: &db::sqlite::Database,
		now: i64,
		ttl: std::time::Duration,
		mut n: usize,
	) -> tg::Result<InnerOutput> {
		let max_touched_at = now - ttl.as_secs().to_i64().unwrap();

		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let p = connection.p();
		let statement = formatdoc!(
			"
				select id from cache_entries
				where reference_count = 0 and touched_at <= {p}1
				limit {p}2;
			"
		);
		let params = db::params![max_touched_at, n];
		let cache_entries_ = connection
			.query_all_value_into::<tg::artifact::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		let mut cache_entries = Vec::new();
		for id in cache_entries_ {
			let statement = formatdoc!(
				"
					update cache_entries
						set reference_count = (
							select count(*) from objects where cache_entry = ?1
						),
						reference_count_transaction_id = (
							select id from transaction_id
						)
					where id = ?1
					returning reference_count;
				"
			);
			let params = db::params![id];
			let reference_count = connection
				.query_one_value_into::<u64>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			if reference_count == 0 {
				let statement = formatdoc!(
					"
						delete from cache_entries
						where id = ?1;
					"
				);
				let params = db::params![id];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				cache_entries.push(id);
			}
		}
		n -= cache_entries.len();

		let p = connection.p();
		let statement = formatdoc!(
			"
				select id from objects
				where reference_count = 0 and touched_at <= {p}1
				limit {p}2;
			"
		);
		let params = db::params![max_touched_at, n];
		let objects_ = connection
			.query_all_value_into::<tg::object::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		let mut objects = Vec::new();
		for id in objects_ {
			let statement = formatdoc!(
				"
					update objects
						set reference_count = (
							(select count(*) from object_children where child = ?1) +
							(select count(*) from process_objects where object = ?1) +
							(select count(*) from tags where item = ?1)
						),
						reference_count_transaction_id = (
							select id from transaction_id
						)
					where id = ?1
					returning reference_count;
				"
			);
			let params = db::params![id];
			let reference_count = connection
				.query_one_value_into::<u64>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			if reference_count == 0 {
				let statement = formatdoc!(
					"
						update objects
						set reference_count = reference_count - 1
						where id in (
							select child from object_children where object = ?1
						);
					"
				);
				let params = db::params![id];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						update cache_entries
						set reference_count = reference_count - 1
						where id in (
							select cache_entry from objects where id = ?1
						);
					"
				);
				let params = db::params![id];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						delete from objects
						where id = ?1;
					"
				);
				let params = db::params![id];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						delete from object_children
						where object = ?1;
					"
				);
				let params = db::params![id];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				objects.push(id);
			}
		}
		n -= objects.len();

		let p = connection.p();
		let statement = formatdoc!(
			"
				select id from processes
				where reference_count = 0 and touched_at <= {p}1
				limit {p}2;
			"
		);
		let params = db::params![max_touched_at, n];
		let processes_ = connection
			.query_all_value_into::<tg::process::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		let mut processes = Vec::new();
		for id in processes_ {
			let statement = formatdoc!(
				"
					update processes
						set reference_count = (
							(select count(*) from process_children where child = ?1) +
							(select count(*) from tags where item = ?1)
						),
						reference_count_transaction_id = (
							select id from transaction_id
						)
					where id = ?1
					returning reference_count;
				"
			);
			let params = db::params![id];
			let reference_count = connection
				.query_one_value_into::<u64>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			if reference_count == 0 {
				let statement = formatdoc!(
					"
						update processes
							set reference_count = reference_count - 1
						where id in (
							select child from process_children
							where process = ?1
						);
					"
				);
				let params = db::params![id];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						update objects
							set reference_count = reference_count - 1
						where id in (
							select object from process_objects
							where process = ?1
						);
					"
				);
				let params = db::params![id];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						delete from process_children
						where process = ?1;
					"
				);
				let params = db::params![id];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						delete from process_objects
						where process = ?1;
					"
				);
				let params = db::params![id];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						delete from processes
						where id = ?1;
					"
				);
				let params = db::params![id];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				processes.push(id);
			}
		}

		// Drop the connection.
		drop(connection);

		// Delete cache entries.
		tokio::task::spawn_blocking({
			let server = self.clone();
			let cache_entries = cache_entries.clone();
			move || {
				for artifact in &cache_entries {
					let path = server.cache_path().join(artifact.to_string());
					crate::util::fs::remove_sync(&path).map_err(
						|source| tg::error!(!source, %path = path.display(), "failed to remove the file"),
					)?;
				}
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.unwrap()?;

		// Delete objects.
		let ttl = ttl.as_secs();
		let args = objects
			.iter()
			.cloned()
			.map(|id| crate::store::DeleteArg { id, now, ttl })
			.collect();
		self.store.delete_batch(args).await?;

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Delete processes.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from processes
				where id = {p}1 and touched_at <= {p}2;
			"
		);
		for id in &processes {
			let params = db::params![&id, max_touched_at];
			connection
				.execute(statement.clone().into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the processes"))?;
		}

		// Drop the connection.
		drop(connection);

		// Delete process logs.
		for id in &processes {
			let path = self.logs_path().join(id.to_string());
			tokio::fs::remove_file(path).await.ok();
		}

		// Delete pipes.
		for id in self.pipes.iter().map(|entry| entry.key().clone()) {
			self.messenger
				.delete_stream(id.to_string())
				.await
				.map_err(|source| tg::error!(!source, "failed to delete the stream"))?;
		}
		self.pipes.clear();

		// Delete ptys.
		for id in self.ptys.iter().map(|entry| entry.key().clone()) {
			for name in ["master_writer", "master_reader"] {
				self.messenger
					.delete_stream(format!("{id}_{name}"))
					.await
					.map_err(|source| tg::error!(!source, "failed to delete the stream"))?;
			}
		}
		self.ptys.clear();

		let output = InnerOutput {
			cache_entries,
			objects,
			processes,
		};

		Ok(output)
	}

	#[cfg(feature = "postgres")]
	async fn cleaner_task_inner_postgres(
		&self,
		database: &db::postgres::Database,
		now: i64,
		ttl: std::time::Duration,
		mut n: usize,
	) -> tg::Result<InnerOutput> {
		let max_touched_at = now - ttl.as_secs().to_i64().unwrap();

		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Clean cache entries.
		let statement = "call clean_cache_entries($1, $2, null);";
		let row = connection
			.inner()
			.query_one(statement, &[&max_touched_at, &n.to_i64().unwrap()])
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to call clean_cache_entries procedure")
			})?;
		let cache_entries: Vec<tg::artifact::Id> = row
			.get::<_, Vec<String>>(0)
			.into_iter()
			.map(|s| s.parse())
			.collect::<Result<Vec<_>, _>>()
			.map_err(|source| tg::error!(!source, "failed to parse artifact IDs"))?;
		n -= cache_entries.len();

		// Clean objects.
		let statement = "call clean_objects($1, $2, null);";
		let row = connection
			.inner()
			.query_one(statement, &[&max_touched_at, &n.to_i64().unwrap()])
			.await
			.map_err(|source| tg::error!(!source, "failed to call clean_objects procedure"))?;
		let objects: Vec<tg::object::Id> = row
			.get::<_, Vec<String>>(0)
			.into_iter()
			.map(|s| s.parse())
			.collect::<Result<Vec<_>, _>>()
			.map_err(|source| tg::error!(!source, "failed to parse object IDs"))?;
		n -= objects.len();

		// Clean processes.
		let statement = "call clean_processes($1, $2, null);";
		let row = connection
			.inner()
			.query_one(statement, &[&max_touched_at, &n.to_i64().unwrap()])
			.await
			.map_err(|source| tg::error!(!source, "failed to call clean_processes procedure"))?;
		let processes: Vec<tg::process::Id> = row
			.get::<_, Vec<String>>(0)
			.into_iter()
			.map(|s| s.parse())
			.collect::<Result<Vec<_>, _>>()
			.map_err(|source| tg::error!(!source, "failed to parse process IDs"))?;

		drop(connection);

		// Delete objects.
		let ttl = ttl.as_secs();
		let args = objects
			.iter()
			.cloned()
			.map(|id| crate::store::DeleteArg { id, now, ttl })
			.collect();
		self.store.delete_batch(args).await?;

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Delete processes.
		for id in &processes {
			let statement = formatdoc!(
				"
					delete from processes
					where id = $1 and touched_at <= $2;
				"
			);
			let params = db::params![&id, max_touched_at];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the processes"))?;
		}

		// Drop the database connection.
		drop(connection);

		// Delete process logs.
		for id in &processes {
			let path = self.logs_path().join(id.to_string());
			tokio::fs::remove_file(path).await.ok();
		}

		// Delete pipes.
		for id in self.pipes.iter().map(|entry| entry.key().clone()) {
			self.messenger
				.delete_stream(id.to_string())
				.await
				.map_err(|source| tg::error!(!source, "failed to delete the stream"))?;
		}
		self.pipes.clear();

		// Delete ptys.
		for id in self.ptys.iter().map(|entry| entry.key().clone()) {
			for name in ["master_writer", "master_reader"] {
				self.messenger
					.delete_stream(format!("{id}_{name}"))
					.await
					.map_err(|source| tg::error!(!source, "failed to delete the stream"))?;
			}
		}
		self.ptys.clear();

		let output = InnerOutput {
			cache_entries,
			objects,
			processes,
		};

		Ok(output)
	}

	pub(crate) async fn handle_server_clean_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the stream.
		let stream = handle.clean().await?;

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
