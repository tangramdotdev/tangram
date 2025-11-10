use {
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{panic::AssertUnwindSafe, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{stream::Ext as _, task::Stop},
	tangram_http::{Body, request::Ext as _},
	tangram_messenger::Messenger as _,
	tangram_store::prelude::*,
	tokio_util::task::AbortOnDropHandle,
};

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

struct InnerOutput {
	cache_entries: Vec<tg::artifact::Id>,
	objects: Vec<tg::object::Id>,
	processes: Vec<tg::process::Id>,
}

impl Server {
	pub(crate) async fn clean_with_context(
		&self,
		context: &Context,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + use<>,
	> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		let progress = crate::progress::Handle::new();
		let task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				let result = AssertUnwindSafe(server.clean_inner(&progress))
					.catch_unwind()
					.await;
				match result {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						progress.error(tg::error!(?message, "the task panicked"));
					},
				}
			}
		}));
		let stream = progress.stream().attach(task);
		Ok(stream)
	}

	async fn clean_inner(
		&self,
		progress: &crate::progress::Handle<tg::clean::Output>,
	) -> tg::Result<tg::clean::Output> {
		// Clean the temporary directory.
		tangram_util::fs::remove(self.temp_path())
			.await
			.map_err(|source| tg::error!(!source, "failed to remove the temporary directory"))?;
		tokio::fs::create_dir_all(self.temp_path())
			.await
			.map_err(|error| {
				tg::error!(source = error, "failed to recreate the temporary directory")
			})?;

		let mut output = tg::clean::Output {
			cache: 0,
			objects: 0,
			processes: 0,
		};
		let batch_size = self
			.config
			.cleaner
			.as_ref()
			.map_or(1024, |config| config.batch_size);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let ttl = Duration::from_secs(0);
		loop {
			let inner_output = match self.cleaner_task_inner(now, ttl, batch_size).await {
				Ok(inner_output) => inner_output,
				Err(error) => {
					progress.error(error);
					break;
				},
			};

			// Get the number of items cleaned.
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

		Ok::<_, tg::Error>(output)
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
					tangram_util::fs::remove_sync(&path).map_err(
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
		self.store
			.delete_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete objects"))?;

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Delete processes.
		let p = connection.p();
		for id in &output.processes {
			// Delete process_children.
			let statement = formatdoc!(
				"
					delete from process_children
					where process = {p}1;
				"
			);
			let params = db::params![id.to_string()];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to delete process_children"))?;

			// Delete process_tokens.
			let statement = formatdoc!(
				"
					delete from process_tokens
					where process = {p}1;
				"
			);
			let params = db::params![id.to_string()];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to delete process_tokens"))?;

			// Delete the process.
			let statement = formatdoc!(
				"
					delete from processes
					where id = {p}1 and touched_at <= {p}2;
				"
			);
			let params = db::params![id.to_string(), max_touched_at];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to delete the process"))?;
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

	pub(crate) async fn handle_server_clean_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the stream.
		let stream = self.clean_with_context(context).await?;

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
