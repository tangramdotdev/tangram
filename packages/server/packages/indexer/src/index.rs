use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, stream::BoxStream},
	std::panic::AssertUnwindSafe,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::{self as index, Index as _},
	tangram_server_core::{Server, Session},
};

pub(crate) mod wait;

impl Component {
	pub(crate) async fn index_batch(
		&self,
		server: &Server,
		arg: index::batch::Arg,
	) -> tg::Result<()> {
		if arg.is_empty() {
			return Ok(());
		}
		if server.config.advanced.multi_server {
			let arg = tangram_server_core::indexer::EnqueueArg {
				index: arg,
				..Default::default()
			};
			self.enqueue(server, arg).await?;

			return Ok(());
		}
		let command_object_permission = arg.items.iter().any(|item| {
			matches!(
				item,
				index::batch::Item::PutObject(arg)
					if arg.id.kind() == tg::object::Kind::Command
			)
		}) && arg.items.iter().any(|item| {
			matches!(
				item,
				index::batch::Item::PutPermission(arg)
					if arg.resource.kind() == tg::id::Kind::Command
						&& arg.subject.is_process()
			)
		});
		let finished_process = arg.items.iter().any(|item| {
			matches!(
				item,
				index::batch::Item::PutProcess(arg)
					if arg.output.is_some()
			)
		});
		let destroyed_sandbox = arg.items.iter().any(|item| {
			matches!(item, index::batch::Item::PutSandbox(arg)
				if arg.data.as_ref().is_some_and(|data| data.data.status.is_destroyed()))
		});
		let child_process = arg.items.iter().any(
			|item| matches!(item, index::batch::Item::PutProcess(arg) if arg.parent.is_some()),
		);
		let started_process = arg.items.iter().any(|item| {
			matches!(item, index::batch::Item::PutProcess(arg)
				if arg.data.as_ref().is_some_and(|data| data.status.is_started()))
		});
		self.index_tasks
			.spawn({
				let component = self.clone();
				let server = server.clone();
				|_| async move {
					tangram_server_core::checkpoint!(
						server,
						"index.batch",
						child_process,
						command_object_permission,
						destroyed_sandbox,
						finished_process,
						started_process
					)
					.await;
					let result = component.index_batch_inner(&server, arg).await;
					if result.is_ok() {
						component.index_changed.notify_waiters();
					}

					let result = result.and_then(std::convert::identity);
					if let Err(error) = &result {
						tracing::error!(error = %error.trace(), "failed to index a batch");
					}
					tangram_server_core::checkpoint!(
						server,
						"index.batch.finished",
						command_object_permission,
						finished_process
					)
					.await;

					result
				}
			})
			.detach();

		Ok(())
	}

	pub(crate) async fn index_batch_inner(
		&self,
		server: &Server,
		arg: index::batch::Arg,
	) -> tg::Result<tg::Result<()>> {
		// Wake index-backed readers after the runner state has been persisted.
		let sandboxes = arg
			.items
			.iter()
			.filter_map(|item| match item {
				index::batch::Item::PutProcess(index::process::put::Arg {
					sandbox: Some(index::process::put::Sandbox::Put(arg)),
					..
				})
				| index::batch::Item::PutSandbox(arg)
					if arg.data.is_some() =>
				{
					Some(arg.id.clone())
				},
				_ => None,
			})
			.collect::<Vec<_>>();
		let processes = arg
			.items
			.iter()
			.filter_map(|item| match item {
				index::batch::Item::PutProcess(arg) if arg.data.is_some() => Some(arg.id.clone()),
				_ => None,
			})
			.collect::<Vec<_>>();
		let result = server.index.batch(arg).await?;
		for id in sandboxes {
			server
				.components
				.sandbox
				.spawn_publish_sandbox_status_task(server, &id);
		}
		for id in processes {
			server
				.components
				.process
				.spawn_publish_process_status_task(server, &id);
		}
		Ok(result)
	}
}

impl Component {
	pub(crate) async fn index(
		&self,
		session: &Session,
		arg: tg::index::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<()>>>> {
		if !session
			.server
			.config
			.roles
			.contains(&tangram_server_core::config::Role::Indexer)
			&& !session.server.config.advanced.multi_server
		{
			return Err(tg::error!("cannot index when the indexer is disabled"));
		}
		let capacity = session.server.config.progress.channel_capacity;
		let progress = tg::progress::Handle::new(capacity);
		let task = Task::spawn({
			let progress = progress.clone();
			let component = self.clone();
			let server = session.server.clone();
			|_| async move {
				let result = AssertUnwindSafe(component.index_task(&server, arg, &progress))
					.catch_unwind()
					.await;
				match result {
					Ok(Ok(())) => {
						progress.output(());
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

	async fn index_task(
		&self,
		server: &Server,
		arg: tg::index::Arg,
		progress: &tg::progress::Handle<()>,
	) -> tg::Result<()> {
		// Wait for indexing.
		progress.spinner("index", "waiting for indexing");
		self.index_inner().await?;
		progress.finish("index");

		// Wait for the permission capture queue to be empty, and then for the index updates that the captures produced.
		if arg.capture || arg.usage {
			progress.spinner("capture", "waiting for permission capture");
			self.wait_for_permission_capture(server).await?;
			self.index_inner().await?;
			progress.finish("capture");
		}

		// Aggregate usage for every hour before the current hour.
		if arg.usage {
			progress.spinner("usage", "aggregating usage");
			Self::aggregate_usage_through_now(server).await?;
			progress.finish("usage");
		}

		Ok(())
	}

	async fn wait_for_permission_capture(&self, server: &Server) -> tg::Result<()> {
		let partition_total = server.index.permission_update_partition_total();
		loop {
			// Wake this server's permission capture tasks, so that they do not wait out their poll interval.
			self.permission_capture_wake.notify_waiters();

			// Check whether the permission capture queue is empty.
			let entries = server
				.index
				.permission_capture_batch(1, 0, partition_total)
				.await
				.map_err(|error| {
					tg::error!(!error, "failed to read the permission capture queue")
				})?;
			if entries.is_empty() {
				break;
			}

			tokio::time::sleep(server.config.indexer.request.poll_interval).await;
		}

		Ok(())
	}

	async fn aggregate_usage_through_now(server: &Server) -> tg::Result<()> {
		let now = server.clock.now();
		let partition_total = server.index.usage_partition_total();
		loop {
			let arg = tangram_index::usage::aggregate::Arg {
				batch_size: server.config.indexer.usage.aggregation.batch_size,
				delta_time_to_live: server.config.usage.delta_time_to_live,
				now,
				partition_end: partition_total,
				partition_start: 0,
			};
			let output = server
				.index
				.aggregate_usage(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to aggregate usage"))?;
			if output.count == 0 {
				break;
			}
		}

		Ok(())
	}
}

impl Component {
	pub(crate) async fn index_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		session.verify_request_from_host()?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self
			.index(session, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the index task"))?;

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
}
