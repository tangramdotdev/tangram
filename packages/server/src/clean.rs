use {
	crate::{Context, Server, temp::Temp},
	futures::{FutureExt as _, Stream, StreamExt as _, future},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{panic::AssertUnwindSafe, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{
		stream::Ext as _,
		task::{Stop, Task},
	},
	tangram_http::{Body, request::Ext as _},
	tangram_index::prelude::*,
	tangram_store::prelude::*,
};

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
		if !self.config.advanced.single_process {
			return Err(tg::error!("cannot clean in multi-process mode"));
		}

		let progress = crate::progress::Handle::new();

		// Acquire an exclusive lock.
		let mut guard = self.clean_lock.try_write().ok();
		if guard.is_none() {
			progress.spinner("tasks", "waiting for tasks");
			guard.replace(self.clean_lock.write().await);
			progress.finish("tasks");
		}
		let task = Task::spawn({
			let server = self.clone();
			let progress = progress.clone();
			|_| async move {
				let result = AssertUnwindSafe(server.clean_task(&progress))
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
		});
		let stream = progress.stream().attach(task);
		Ok(stream)
	}

	async fn clean_task(
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
			bytes: 0,
			cache_entries: 0,
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
		progress.start(
			"cache_entries".into(),
			"cache entries".into(),
			tangram_client::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		progress.start(
			"objects".into(),
			"objects".into(),
			tangram_client::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		progress.start(
			"processes".into(),
			"processes".into(),
			tangram_client::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);

		// For manual cleaning, process all partitions.
		let partition_total = self.index.partition_total();
		loop {
			let inner_output = match self
				.cleaner_task_inner(now, ttl, batch_size, 0, partition_total)
				.await
			{
				Ok(inner_output) => inner_output,
				Err(error) => {
					progress.error(error);
					break;
				},
			};

			// Get the number of items cleaned.
			let bytes = inner_output.bytes;
			let cache_entries = inner_output.cache_entries.len().to_u64().unwrap();
			let objects = inner_output.objects.len().to_u64().unwrap();
			let processes = inner_output.processes.len().to_u64().unwrap();
			output.bytes += bytes;
			output.cache_entries += cache_entries;
			output.objects += objects;
			output.processes += processes;
			progress.increment("cache_entries", cache_entries);
			progress.increment("objects", objects);
			progress.increment("processes", processes);
			if inner_output.done {
				break;
			}
		}

		Ok::<_, tg::Error>(output)
	}

	pub(crate) async fn cleaner_task(&self, config: &crate::config::Cleaner) -> tg::Result<()> {
		let partition_start = config.partition_start;
		let partition_count = config.partition_count;
		let concurrency = config.concurrency.to_u64().unwrap();
		loop {
			let now = time::OffsetDateTime::now_utc().unix_timestamp();
			let ttl = config.ttl;
			let n = config.batch_size;

			let futures = (0..config.concurrency).map(|task_index| {
				let task_index = task_index.to_u64().unwrap();
				let partitions_per_task = partition_count / concurrency;
				let extra = partition_count % concurrency;
				let task_start =
					partition_start + task_index * partitions_per_task + task_index.min(extra);
				let task_count = partitions_per_task + u64::from(task_index < extra);
				self.cleaner_task_inner(now, ttl, n, task_start, task_count)
			});
			let result = future::try_join_all(futures).await;
			match result {
				Ok(outputs) => {
					if outputs.iter().all(|output| output.done) {
						tokio::time::sleep(Duration::from_secs(1)).await;
					}
				},
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to clean");
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
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<tangram_index::CleanOutput> {
		let max_touched_at = now - ttl.as_secs().to_i64().unwrap();

		// Clean.
		let output = self
			.index
			.clean(max_touched_at, n, partition_start, partition_count)
			.await?;

		// Delete cache entries.
		tokio::task::spawn_blocking({
			let server = self.clone();
			let cache_entries = output.cache_entries.clone();
			move || {
				let temp = Temp::new(&server);
				let cache_path = server.cache_path();
				for artifact in &cache_entries {
					let path = cache_path.join(artifact.to_string());
					let temp_path = temp.path().join(artifact.to_string());
					std::fs::rename(&path, &temp_path).ok();
					tangram_util::fs::remove_sync(&temp_path).ok();

					for extension in [".tg.js", ".tg.ts"] {
						let path = cache_path.join(format!("{artifact}{extension}"));
						let temp_path = temp.path().join(format!("{artifact}{extension}"));
						std::fs::rename(&path, &temp_path).ok();
						tangram_util::fs::remove_sync(&temp_path).ok();
					}
				}
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "the clean task panicked"))??;

		// Delete objects.
		let ttl = ttl.as_secs();
		let args = output
			.objects
			.iter()
			.cloned()
			.map(|id| crate::store::DeleteObjectArg { id, now, ttl })
			.collect();
		self.store
			.delete_object_batch(args)
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
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the stream.
		let stream = self
			.clean_with_context(context)
			.await
			.map_err(|source| tg::error!(!source, "failed to start the clean task"))?;

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
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
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
