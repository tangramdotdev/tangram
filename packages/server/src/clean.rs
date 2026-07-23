use {
	crate::Session,
	futures::{FutureExt as _, Stream, StreamExt as _},
	num::ToPrimitive as _,
	std::{ops::ControlFlow, panic::AssertUnwindSafe, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn clean(
		&self,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + use<>,
	> {
		if matches!(self.context.principal, tg::Principal::Process(_)) {
			return Err(tg::error!("unauthorized"));
		}

		if !self.server.config.advanced.single_process {
			return Err(tg::error!("cannot clean in multi-process mode"));
		}

		let progress = crate::progress::Handle::new();

		let task = Task::spawn({
			let session = self.clone();
			let progress = progress.clone();
			|_| async move {
				let result = AssertUnwindSafe(session.clean_task(&progress))
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

		let stream = progress
			.stream()
			.attach(task)
			.with_stopper(self.context.stopper.clone());

		Ok(stream)
	}

	async fn clean_task(
		&self,
		progress: &crate::progress::Handle<tg::clean::Output>,
	) -> tg::Result<tg::clean::Output> {
		// Stop the sandbox pool.
		self.stop_sandbox_pool().await;

		// Run the clean task.
		let result = self.clean_task_inner(progress).await;

		// Restart the sandbox pool.
		self.start_sandbox_pool();

		result
	}

	async fn clean_task_inner(
		&self,
		progress: &crate::progress::Handle<tg::clean::Output>,
	) -> tg::Result<tg::clean::Output> {
		// Clean the temporary directory.
		tangram_util::fs::remove(self.server.temp_path())
			.await
			.map_err(|error| tg::error!(!error, "failed to remove the temporary directory"))?;
		tokio::fs::create_dir_all(self.server.temp_path())
			.await
			.map_err(|error| {
				tg::error!(source = error, "failed to recreate the temporary directory")
			})?;

		let mut output = tg::clean::Output {
			bytes: 0,
			cache_entries: 0,
			objects: 0,
			processes: 0,
			sandboxes: 0,
			tags: 0,
		};
		let batch_size = self.server.config.cleaner.batch_size;
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let object_time_to_live = Duration::from_secs(0);
		let process_time_to_live = Duration::from_secs(0);
		let sandbox_time_to_live = Duration::from_secs(0);
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
		progress.start(
			"sandboxes".into(),
			"sandboxes".into(),
			tangram_client::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);

		// For manual cleaning, process all partitions.
		let partition_total = self.server.index.partition_total();
		loop {
			let inner_output = match self
				.server
				.cleaner_task_inner(crate::cleaner::CleanerTaskInnerArg {
					n: batch_size,
					now,
					object_time_to_live,
					partition_end: partition_total,
					partition_start: 0,
					process_time_to_live,
					sandbox_time_to_live,
				})
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
			let sandboxes = inner_output.sandboxes.len().to_u64().unwrap();
			output.bytes += bytes;
			output.cache_entries += cache_entries;
			output.objects += objects;
			output.processes += processes;
			output.sandboxes += sandboxes;
			progress.increment("cache_entries", cache_entries);
			progress.increment("objects", objects);
			progress.increment("processes", processes);
			progress.increment("sandboxes", sandboxes);
			if inner_output.done {
				break;
			}
		}

		// Delete the list cache.
		self.server
			.database
			.run(|transaction| {
				async move { Self::delete_list_cache_with_transaction(transaction).await }.boxed()
			})
			.await?;

		Ok::<_, tg::Error>(output)
	}

	async fn delete_list_cache_with_transaction(
		transaction: &crate::database::Transaction<'_>,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let statement = "delete from list_cache;";
		let result = transaction.execute(statement.into(), db::params![]).await;
		crate::database::retry!(result, "failed to delete the list cache");
		Ok(ControlFlow::Break(()))
	}
}

impl Session {
	pub(crate) async fn clean_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the stream.
		let stream = self
			.clean()
			.await
			.map_err(|error| tg::error!(!error, "failed to start the clean task"))?;

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
				(Some(content_type), BoxBody::with_sse_stream(stream))
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
