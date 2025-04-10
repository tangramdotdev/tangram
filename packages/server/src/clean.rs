use super::Server;
use futures::{Stream, StreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use time::format_description::well_known::Rfc3339;
use tokio_util::task::AbortOnDropHandle;

struct InnerOutput {
	cache_entries: Vec<tg::artifact::Id>,
	objects: Vec<tg::object::Id>,
	pipes: Vec<tg::pipe::Id>,
	processes: Vec<tg::process::Id>,
	ptys: Vec<tg::pty::Id>,
}

struct Count {
	cache_entries: u64,
	objects: u64,
	pipes: u64,
	processes: u64,
	ptys: u64,
}

impl Server {
	pub async fn clean(
		&self,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let progress = crate::progress::Handle::new();

		let task = tokio::spawn({
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

				let count = server.count_items().await?;

				// Clean until there are no more items to remove.
				if count.cache_entries > 0 {
					progress.start(
						"clean-cache".into(),
						"cache entries".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.cache_entries),
					);
				}
				if count.objects > 0 {
					progress.start(
						"clean-objects".into(),
						"objects".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.objects),
					);
				}
				if count.processes > 0 {
					progress.start(
						"clean-processes".into(),
						"processes".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.processes),
					);
				}
				if count.pipes > 0 {
					progress.start(
						"clean-pipes".into(),
						"pipes".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.pipes),
					);
				}
				if count.ptys > 0 {
					progress.start(
						"clean-ptys".into(),
						"ptys".into(),
						tg::progress::IndicatorFormat::Normal,
						Some(0),
						Some(count.ptys),
					);
				}

				loop {
					let now = time::OffsetDateTime::now_utc().unix_timestamp();
					let ttl = Duration::from_secs(0);
					let batch_size = server
						.config
						.cleaner
						.as_ref()
						.map_or(1024, |config| config.batch_size);
					let output = match server.cleaner_task_inner(now, ttl, batch_size).await {
						Ok(output) => output,
						Err(error) => {
							progress.error(error);
							break;
						},
					};
					progress.increment("clean-cache", output.cache_entries.len().to_u64().unwrap());
					progress.increment("clean-objects", output.objects.len().to_u64().unwrap());
					progress.increment("clean-processes", output.processes.len().to_u64().unwrap());
					progress.increment("clean-pipes", output.pipes.len().to_u64().unwrap());
					progress.increment("clean-ptys", output.ptys.len().to_u64().unwrap());
					let n =
						output.processes.len() + output.objects.len() + output.cache_entries.len();
					if n == 0 {
						break;
					}
				}
				progress.output(());
				Ok::<_, tg::Error>(())
			}
		});
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = progress.stream().attach(abort_handle);
		Ok(stream)
	}

	pub(crate) async fn cleaner_task(&self, config: &crate::config::Cleaner) -> tg::Result<()> {
		loop {
			let now = time::OffsetDateTime::now_utc().unix_timestamp();
			let ttl = config.ttl;
			let batch_size = config.batch_size;
			let result = self.cleaner_task_inner(now, ttl, batch_size).await;
			match result {
				Ok(output) => {
					let n = output.processes.len()
						+ output.objects.len()
						+ output.cache_entries.len()
						+ output.pipes.len()
						+ output.ptys.len();
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

	async fn count_items(&self) -> tg::Result<Count> {
		// Get an index connection.
		let connection = self
			.index
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Count cache entries.
		let statement = formatdoc!(
			"
				select count(*) from cache_entries;
			"
		);
		let params = db::params![];
		let cache_entries = connection
			.query_one_value_into::<u64>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects"))?;

		// Count processes.
		let statement: String = formatdoc!(
			"
				select count(*) from processes;
			"
		);
		let params = db::params![];
		let processes = connection
			.query_one_value_into::<u64>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the processes"))?;

		// Count objects
		let statement = formatdoc!(
			"
				select count(*) from objects;
			"
		);
		let params = db::params![];
		let objects = connection
			.query_one_value_into::<u64>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects"))?;

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Count pipes and ptys.
		let statement = formatdoc!(
			"
				select count(*) from pipes;
			"
		);
		let params = db::params![];
		let pipes = connection
			.query_one_value_into::<u64>(statement.clone().into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the ptys"))?;

		let statement = formatdoc!(
			"
				select count(*) from ptys;
			"
		);
		let params = db::params![];
		let ptys = connection
			.query_one_value_into::<u64>(statement.clone().into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the ptys"))?;
		drop(connection);
		Ok(Count {
			cache_entries,
			objects,
			pipes,
			processes,
			ptys,
		})
	}

	async fn cleaner_task_inner(
		&self,
		now: i64,
		ttl: std::time::Duration,
		batch_size: usize,
	) -> tg::Result<InnerOutput> {
		let max_touched_at =
			time::OffsetDateTime::from_unix_timestamp(now - ttl.as_secs().to_i64().unwrap())
				.unwrap()
				.format(&Rfc3339)
				.unwrap();

		// Get an index connection.
		let connection = self
			.index
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Delete processes from the index.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from processes
				where id in (
					select id from processes
					where reference_count = 0 and touched_at <= {p}1
					limit {p}2
				)
				returning id;
			"
		);
		let params = db::params![max_touched_at, batch_size];
		let processes = connection
			.query_all_value_into::<tg::process::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the processes"))?;

		// Delete objects from the index.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from objects
				where id in (
					select id from objects
					where reference_count = 0 and touched_at <= {p}1
					limit {p}2
				)
				returning id;
			"
		);
		let params = db::params![max_touched_at, batch_size];
		let objects = connection
			.query_all_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects"))?;

		// Delete cache entries.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from cache_entries
				where id in (
					select id from cache_entries
					where reference_count = 0 and touched_at <= {p}1
					limit {p}2
				)
				returning id;
			"
		);
		let params = db::params![max_touched_at, batch_size];
		let cache_entries = connection
			.query_all_value_into::<tg::artifact::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the objects"))?;

		// Drop the connection.
		drop(connection);

		// Delete objects.
		let arg = crate::store::DeleteBatchArg {
			ids: objects.clone(),
			now,
			ttl: ttl.as_secs(),
		};
		self.store.delete_batch(arg).await?;

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Delete processes from the database.
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

		// Delete cache entries.
		tokio::task::spawn_blocking({
			let server = self.clone();
			let cache_entries = cache_entries.clone();
			move || {
				for artifact in &cache_entries {
					let path = server.cache_path().join(artifact.to_string());
					std::fs::remove_file(&path).map_err(|source| {
						tg::error!(!source, ?path, "failed to remove the cache entry")
					})?;
				}
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.unwrap()?;

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Delete pipes and ptys.
		let max_created_at =
			time::OffsetDateTime::from_unix_timestamp(now.saturating_sub(24 * 60 * 60))
				.unwrap()
				.format(&Rfc3339)
				.unwrap();
		let p = connection.p();
		let statement = formatdoc!(
			"
				select id from pipes
				where closed = 1 or created_at < {p}1
				limit {p}2;
			"
		);
		let params = db::params![max_created_at, batch_size];
		let pipes = connection
			.query_all_value_into::<tg::pipe::Id>(statement.clone().into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the pipes"))?;

		let statement = formatdoc!(
			"
				select id from ptys
				where closed = 1 or created_at < {p}1
				limit {p}2;
			"
		);
		let params = db::params![max_created_at, batch_size];
		let ptys = connection
			.query_all_value_into::<tg::pty::Id>(statement.clone().into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the ptys "))?;
		drop(connection);

		for id in &pipes {
			self.delete_pipe(id, tg::pipe::delete::Arg::default())
				.await
				.ok();
		}
		for id in &ptys {
			self.delete_pty(id, tg::pty::delete::Arg::default())
				.await
				.ok();
		}

		let output = InnerOutput {
			cache_entries,
			objects,
			pipes,
			processes,
			ptys,
		};

		Ok(output)
	}
}

impl Server {
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
