use self::{
	graph::{Graph, NodeInner},
	progress::Progress,
};
use crate::Server;
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future, stream,
};
use std::{
	pin::Pin,
	sync::{Arc, Mutex},
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

mod graph;
mod index;
mod progress;
mod store;

const PROCESS_COMPLETE_BATCH_SIZE: usize = 8;
const PROCESS_COMPLETE_CONCURRENCY: usize = 8;
const OBJECT_COMPLETE_BATCH_SIZE: usize = 64;
const OBJECT_COMPLETE_CONCURRENCY: usize = 8;
const INDEX_MESSAGE_MAX_BYTES: usize = 1_000_000;

impl Server {
	pub async fn import(
		&self,
		mut arg: tg::import::Arg,
		mut stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::import::Event>> + Send + 'static> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let client = self.get_remote_client(remote.clone()).await?;
			let stream = client.import(arg, stream).await?;
			return Ok(stream.boxed());
		}

		// Check if the items are already complete.
		let complete = self.import_items_complete(&arg).await.unwrap_or_default();
		if complete {
			let event = tg::import::Event::End;
			let stream = stream::once(future::ok(event)).boxed();
			return Ok(stream);
		}

		// Create the progress.
		let processes = arg.items.iter().flatten().any(Either::is_left);
		let progress = Arc::new(Progress::new(processes));

		// Create the channels.
		let (event_sender, event_receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::import::Event>>(256);
		let (process_index_sender, process_index_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ProcessItem>(256);
		let (object_index_sender, object_index_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ObjectItem>(256);
		let (process_store_sender, process_store_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ProcessItem>(256);
		let (object_store_sender, object_store_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ObjectItem>(256);

		// Create the graph.
		let graph = Arc::new(Mutex::new(Graph::new()));

		// Spawn the index task.
		self.import_index_tasks
			.lock()
			.unwrap()
			.as_mut()
			.unwrap()
			.spawn({
				let server = self.clone();
				let graph = graph.clone();
				let event_sender = event_sender.clone();
				async move {
					let result = server
						.import_index_task(
							graph,
							process_index_receiver,
							object_index_receiver,
							event_sender,
						)
						.await;
					if let Err(error) = result {
						tracing::error!(?error, "the index task failed");
					}
				}
			});

		// Spawn the store task.
		let store_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				server
					.import_store_task(
						graph,
						process_store_receiver,
						object_store_receiver,
						progress,
					)
					.await
			}
		}));

		// Spawn a task that sends items from the stream to the index and store tasks.
		let task = AbortOnDropHandle::new(tokio::spawn({
			let event_sender = event_sender.clone();
			async move {
				// Read the items from the stream and send them to the tasks.
				let mut success = true;
				loop {
					let event = match stream.try_next().await {
						Ok(Some(event)) => event,
						Ok(None) => break,
						Err(error) => {
							event_sender.send(Err(error)).await.ok();
							return;
						},
					};
					let tg::export::Event::Item(item) = event else {
						continue;
					};
					let index_sender_future = match &item {
						tg::export::Item::Process(item) => process_index_sender
							.send(item.clone())
							.map_err(|_| ())
							.left_future(),
						tg::export::Item::Object(item) => object_index_sender
							.send(item.clone())
							.map_err(|_| ())
							.right_future(),
					};
					let (process_send_future, object_send_future) = match item {
						tg::export::Item::Process(item) => (
							process_store_sender
								.send(item)
								.map_err(|_| ())
								.left_future(),
							future::ok(()).left_future(),
						),
						tg::export::Item::Object(item) => (
							future::ok(()).right_future(),
							object_store_sender
								.send(item)
								.map_err(|_| ())
								.right_future(),
						),
					};
					let result = futures::try_join!(
						index_sender_future,
						process_send_future,
						object_send_future
					);
					if result.is_err() {
						success = false;
					}
				}

				// Close the store channels.
				drop(process_store_sender);
				drop(object_store_sender);

				// Await the store task.
				let result = store_task.await;

				// Close the index channels.
				drop(process_index_sender);
				drop(object_index_sender);

				match result {
					Ok(Ok(())) => {
						if !success {
							event_sender
								.send(Err(tg::error!(?result, "failed to send the item")))
								.await
								.ok();
							return;
						}
						let event = tg::import::Event::End;
						event_sender.send(Ok(event)).await.ok();
					},
					Ok(Err(error)) => {
						event_sender.send(Err(error)).await.ok();
					},
					Err(error) => {
						let error = tg::error!(!error, "the task panicked");
						event_sender.send(Err(error)).await.ok();
					},
				}
			}
		}));

		// Create the stream.
		let event_stream = ReceiverStream::new(event_receiver);
		let progress_stream = progress.stream().map_ok(tg::import::Event::Progress);
		let stream = stream::select(event_stream, progress_stream)
			.take_while_inclusive(|event| {
				future::ready(!matches!(event, Err(_) | Ok(tg::import::Event::End)))
			})
			.attach(task);

		Ok(stream.boxed())
	}

	async fn import_items_complete(&self, arg: &tg::import::Arg) -> tg::Result<bool> {
		let Some(items) = &arg.items else {
			return Ok(false);
		};
		let processes = items
			.iter()
			.filter_map(|item| item.clone().left())
			.collect::<Vec<_>>();
		let objects = items
			.iter()
			.filter_map(|item| item.clone().right())
			.collect::<Vec<_>>();
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let (process_outputs, object_outputs) = futures::try_join!(
			self.try_touch_process_and_get_complete_and_metadata_batch(&processes, touched_at),
			self.try_touch_object_and_get_complete_and_metadata_batch(&objects, touched_at),
		)?;
		let processes_complete = process_outputs.iter().all(|option| {
			option.as_ref().is_some_and(|(complete, _)| {
				complete.children && complete.commands && complete.outputs
			})
		});
		let objects_complete = object_outputs
			.iter()
			.all(|option| option.as_ref().is_some_and(|(complete, _)| *complete));
		let complete = processes_complete && objects_complete;
		Ok(complete)
	}

	pub(crate) async fn handle_import_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the arg.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Validate the request content type.
		let content_type = request
			.parse_header::<mime::Mime, _>(http::header::CONTENT_TYPE)
			.transpose()?;
		if content_type != Some(tg::import::CONTENT_TYPE.parse().unwrap()) {
			return Err(tg::error!(?content_type, "invalid content type"));
		}

		// Get the stop signal.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		// Create the request stream.
		let body = request.reader();
		let stream = stream::try_unfold(body, |mut reader| async move {
			let Some(event) = tg::export::Event::from_reader(&mut reader).await? else {
				return Ok(None);
			};
			if let tg::export::Event::Item(tg::export::Item::Object(object)) = &event {
				let bytes = object.data.serialize()?;
				let actual = tg::object::Id::new(object.id.kind(), &bytes);
				if object.id != actual {
					return Err(tg::error!(%expected = object.id, %actual, "invalid object id"));
				}
			}
			Ok(Some((event, reader)))
		})
		.boxed();

		// Create the outgoing stream.
		let stream = handle.import(arg, stream).await?;

		// Stop the output stream when the server stops.
		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);

		// Create the response body.
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
