use {
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture, stream::BoxStream},
	num::ToPrimitive as _,
	std::{ops::ControlFlow, panic::AssertUnwindSafe, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
	tangram_server_core::Session,
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn clean(
		&self,
		session: &Session,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::clean::Output>>>> {
		session.verify_request_from_host()?;
		if session.server.config.advanced.multi_server {
			return Err(tg::error!("cannot clean in multi-server mode"));
		}

		let capacity = session.server.config.progress.channel_capacity;
		let progress = tg::progress::Handle::new(capacity);

		let task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			let progress = progress.clone();
			|_| async move {
				let result = AssertUnwindSafe(component.clean_task(&session, &progress))
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
			.stream(session.server.config.progress.interval)
			.attach(task)
			.with_stopper(session.context.stopper.clone())
			.boxed();

		Ok(stream)
	}

	async fn clean_task(
		&self,
		session: &Session,
		progress: &tg::progress::Handle<tg::clean::Output>,
	) -> tg::Result<tg::clean::Output> {
		// Stop the sandbox pool.
		session.server.components.runner.stop_sandbox_pool().await;

		// Run the clean task.
		let result = self.clean_task_inner(session, progress).await;

		// Restart the sandbox pool.
		session.server.components.runner.start_sandbox_pool(session);

		result
	}

	async fn clean_task_inner(
		&self,
		session: &Session,
		progress: &tg::progress::Handle<tg::clean::Output>,
	) -> tg::Result<tg::clean::Output> {
		// Clean the temporary directory.
		let result = tangram_util::fs::remove(session.server.temp_path()).await;
		match result {
			Ok(()) => {},
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
			Err(error) => {
				return Err(tg::error!(
					!error,
					"failed to remove the temporary directory"
				));
			},
		}
		tokio::fs::create_dir_all(session.server.temp_path())
			.await
			.map_err(|error| {
				tg::error!(source = error, "failed to recreate the temporary directory")
			})?;

		let mut output = tg::clean::Output {
			bytes: 0,
			checkouts: 0,
			objects: 0,
			processes: 0,
			sandboxes: 0,
			tags: 0,
		};
		let batch_size = session.server.config.indexer.cleaning.batch_size;
		let now = session.server.clock.unix_timestamp();
		let object_time_to_live = Duration::from_secs(0);
		let process_time_to_live = Duration::from_secs(0);
		let sandbox_time_to_live = Duration::from_secs(0);
		progress.start(
			"checkouts".into(),
			"checkouts".into(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		progress.start(
			"objects".into(),
			"objects".into(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		progress.start(
			"processes".into(),
			"processes".into(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		progress.start(
			"sandboxes".into(),
			"sandboxes".into(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);

		// For manual cleaning, process all partitions.
		let partition_total = session.server.index.cleaning_partition_total();
		loop {
			let inner_output = match session
				.server
				.components
				.indexer
				.clean_batch(
					&session.server,
					tangram_server_core::indexer::CleanBatchArg {
						batch_size,
						now,
						object_time_to_live,
						partition_end: partition_total,
						partition_start: 0,
						process_time_to_live,
						sandbox_time_to_live,
					},
				)
				.await
			{
				Ok(inner_output) => inner_output,
				Err(error) => {
					progress.error(error);
					break;
				},
			};

			// Get the number of nodes cleaned.
			let bytes = inner_output.bytes;
			let checkouts = inner_output.checkouts.len().to_u64().unwrap();
			let objects = inner_output.objects.len().to_u64().unwrap();
			let processes = inner_output.processes.len().to_u64().unwrap();
			let sandboxes = inner_output.sandboxes.len().to_u64().unwrap();
			output.bytes += bytes;
			output.checkouts += checkouts;
			output.objects += objects;
			output.processes += processes;
			output.sandboxes += sandboxes;
			progress.increment("checkouts", checkouts);
			progress.increment("objects", objects);
			progress.increment("processes", processes);
			progress.increment("sandboxes", sandboxes);
			if inner_output.done {
				break;
			}
		}

		// Expire the usage data.
		let usage = session.server.config.usage;
		let usage_partition_total = session.server.index.usage_partition_total();
		let now = jiff::Timestamp::new(now, 0).unwrap();
		loop {
			let output = session
				.server
				.index
				.expire_usage(tangram_index::usage::expire::Arg {
					batch_size,
					day_time_to_live: usage.day_time_to_live,
					delta_time_to_live: usage.delta_time_to_live,
					hour_time_to_live: usage.hour_time_to_live,
					month_time_to_live: usage.month_time_to_live,
					now,
					partition_end: usage_partition_total,
					partition_start: 0,
					week_time_to_live: usage.week_time_to_live,
				})
				.await?;
			if output.done {
				break;
			}
		}

		// Delete the remote cache.
		session
			.server
			.database
			.run(|transaction| {
				async move { Self::delete_remote_cache_with_transaction(transaction).await }.boxed()
			})
			.await?;

		Ok::<_, tg::Error>(output)
	}

	async fn delete_remote_cache_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		let statement = "delete from remote_cache;";
		let result = transaction.execute(statement.into(), db::params![]).await;
		tangram_server_core::database::retry!(result, "failed to delete the remote cache");
		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn clean_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the stream.
		let stream = self
			.clean(session)
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
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

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["clean"]) => self.clean_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::clean::Component for Component {
	fn clean<'a>(
		&'a self,
		session: &'a Session,
	) -> BoxFuture<
		'a,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<tg::clean::Output>>>>,
	> {
		self.clean(session).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
