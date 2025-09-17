use super::Server;
use futures::{Stream, StreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::Messenger as _;
use tokio_util::task::AbortOnDropHandle;

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

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
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
	> {
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

				let mut output = tg::clean::Output {
					cache: 0,
					objects: 0,
					processes: 0,
				};
				let batch_size = server
					.config
					.cleaner
					.as_ref()
					.map_or(1024, |config| config.batch_size);
				let now = time::OffsetDateTime::now_utc().unix_timestamp();
				let ttl = Duration::from_secs(0);
				let max_touched_at = now - ttl.as_secs().to_i64().unwrap();

				let count = server.clean_count_items(max_touched_at).await?;
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
					let count = server.clean_count_items(max_touched_at).await?;
					progress.set_total("cache", count.cache_entries);
					progress.set_total("objects", count.objects);
					progress.set_total("processes", count.processes);
					let inner_output = match server.cleaner_task_inner(now, ttl, batch_size).await {
						Ok(inner_output) => inner_output,
						Err(error) => {
							progress.error(error);
							break;
						},
					};
					let cache = inner_output.cache_entries.len().to_u64().unwrap();
					let objects = inner_output.objects.len().to_u64().unwrap();
					let processes = inner_output.processes.len().to_u64().unwrap();
					output.cache += cache;
					output.objects += objects;
					output.processes += processes;
					progress.increment("cache", cache);
					progress.increment("objects", objects);
					progress.increment("processes", processes);
					let n = cache + objects + processes;
					if n == 0 {
						break;
					}
				}

				progress.output(output);

				Ok::<_, tg::Error>(())
			}
		}));
		let stream = progress.stream().attach(task);
		Ok(stream)
	}

	async fn clean_count_items(&self, max_touched_at: i64) -> tg::Result<Count> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.clean_count_items_postgres(database, max_touched_at)
					.await
			},
			crate::index::Index::Sqlite(database) => {
				self.clean_count_items_sqlite(database, max_touched_at)
					.await
			},
		}
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

	async fn cleaner_task_inner(
		&self,
		now: i64,
		ttl: Duration,
		n: usize,
	) -> tg::Result<InnerOutput> {
		let max_touched_at = now - ttl.as_secs().to_i64().unwrap();

		let output = match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.cleaner_task_inner_postgres(database, max_touched_at, n)
					.await?
			},
			crate::index::Index::Sqlite(database) => {
				self.cleaner_task_inner_sqlite(database, max_touched_at, n)
					.await?
			},
		};

		// Delete cache entries.
		tokio::task::spawn_blocking({
			let server = self.clone();
			let cache_entries = output.cache_entries.clone();
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
		let args = output
			.objects
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
		for id in &output.processes {
			let params = db::params![id.to_string(), max_touched_at];
			connection
				.execute(statement.clone().into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the processes"))?;
		}

		// Drop the connection.
		drop(connection);

		// Delete process logs.
		for id in &output.processes {
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
