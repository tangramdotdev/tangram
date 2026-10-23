use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, future, stream::BoxStream},
	std::{ops::ControlFlow, panic::AssertUnwindSafe, sync::Arc},
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
			let config = &server.config.object.index_queue;
			let bytes: bytes::Bytes = arg.serialize()?.into();
			let fragments = bytes
				.chunks(config.fragment_size)
				.map(|fragment| bytes.slice_ref(fragment))
				.collect();
			let batch = tangram_server_core::cache::index::queue::batch::Id::new(
				uuid::Uuid::now_v7().into_bytes(),
			);
			self.enqueue_index_batch(server, batch, fragments).await?;

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
		server
			.index_tasks
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
						server.index_changed.notify_waiters();
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
				index::batch::Item::PutSandbox(arg) if arg.data.is_some() => Some(arg.id.clone()),
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
			server.spawn_publish_sandbox_status_task(&id);
		}
		for id in processes {
			server.spawn_publish_process_status_task(&id);
		}
		Ok(result)
	}

	async fn enqueue_index_batch(
		&self,
		server: &Server,
		batch: tangram_server_core::cache::index::queue::batch::Id,
		fragments: Vec<bytes::Bytes>,
	) -> tg::Result<()> {
		let fragment_count = u64::try_from(fragments.len())
			.map_err(|_| tg::error!("the index batch has too many fragments"))?;
		let fragments: Arc<[bytes::Bytes]> = fragments.into();
		let start = rand::random::<u64>();
		let mut attempt = 0u64;
		let retry =
			tangram_futures::retry::Options::from(server.config.indexer.batch.retry.clone());
		tangram_futures::retry(&retry, || {
			let fragments = fragments.clone();
			let index = start.wrapping_add(attempt);
			let refresh = attempt > 0;
			attempt = attempt.wrapping_add(1);
			async move {
				let indexer = match server
					.components
					.indexer
					.select_indexer(server, index, refresh)
					.await
				{
					Ok(indexer) => indexer,
					Err(error) => return Ok(ControlFlow::Continue(error)),
				};
				let result = self
					.enqueue_index_batch_with_indexer(
						server,
						&indexer,
						batch,
						fragment_count,
						&fragments,
					)
					.await;
				match result {
					Ok(()) => Ok(ControlFlow::Break(())),
					Err(error) => Ok(ControlFlow::Continue(error)),
				}
			}
		})
		.await?;

		Ok(())
	}

	async fn enqueue_index_batch_with_indexer(
		&self,
		server: &Server,
		indexer: &tg::indexer::Id,
		batch: tangram_server_core::cache::index::queue::batch::Id,
		fragment_count: u64,
		fragments: &[bytes::Bytes],
	) -> tg::Result<()> {
		let requests = fragments
			.iter()
			.cloned()
			.enumerate()
			.map(|(fragment, payload)| {
				let arg = tangram_server_core::indexer::RequestArg::Index(
					tangram_server_core::indexer::IndexRequestArg {
						batch,
						fragment: u64::try_from(fragment).unwrap(),
						fragments: fragment_count,
						payload,
					},
				);
				async {
					let output = server
						.components
						.indexer
						.send_indexer_request(server, Some(indexer), arg)
						.await
						.map_err(|source| tg::error!(!source, "failed to send an index request"))?
						.map_err(|source| {
							tg::error!(!source, "the indexer failed to enqueue an index fragment")
						})?;
					output
						.try_unwrap_index()
						.map_err(|_| tg::error!("expected an index response"))?;

					Ok::<_, tg::Error>(())
				}
			});
		let request = future::try_join_all(requests);
		tokio::time::timeout(server.config.indexer.batch.timeout, request)
			.await
			.map_err(|source| tg::error!(!source, "timed out enqueueing an index batch"))??;

		Ok(())
	}
}

impl Component {
	pub(crate) async fn index(
		&self,
		session: &Session,
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
			|_| async move {
				let result = AssertUnwindSafe(component.index_task(&progress))
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

	async fn index_task(&self, progress: &tg::progress::Handle<()>) -> tg::Result<()> {
		progress.spinner("index", "waiting for indexing");
		self.index_inner().await?;
		progress.finish("index");
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

		// Get the stream.
		let stream = self
			.index(session)
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
