use crate::{Server, store::Store};
use futures::{
	FutureExt as _, Stream, StreamExt, TryFutureExt as _, TryStreamExt as _, future,
	stream::{self, FuturesUnordered},
};
use num::ToPrimitive as _;
use std::{
	pin::{Pin, pin},
	sync::{Arc, atomic::AtomicU64},
	time::Duration,
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::prelude::*;
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use tokio_util::task::AbortOnDropHandle;

const PROCESS_COMPLETE_BATCH_SIZE: usize = 8;
const PROCESS_COMPLETE_CONCURRENCY: usize = 8;
const OBJECT_COMPLETE_BATCH_SIZE: usize = 64;
const OBJECT_COMPLETE_CONCURRENCY: usize = 8;

#[derive(Debug)]
struct Progress {
	processes: Option<AtomicU64>,
	objects: AtomicU64,
	bytes: AtomicU64,
}

impl Server {
	pub async fn import(
		&self,
		mut arg: tg::import::Arg,
		mut stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Item>> + Send + 'static>>,
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
		let processes = arg.items.iter().any(Either::is_left);
		let progress = Arc::new(Progress::new(processes));

		// Create the channels.
		let (event_sender, event_receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::import::Event>>(256);
		let (process_complete_sender, process_complete_receiver) =
			tokio::sync::mpsc::channel::<tg::process::Id>(256);
		let (object_complete_sender, object_complete_receiver) =
			tokio::sync::mpsc::channel::<tg::object::Id>(256);
		let (process_sender, process_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ProcessItem>(256);
		let (object_sender, object_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ObjectItem>(256);

		// Spawn the complete task.
		let complete_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let event_sender = event_sender.clone();
			async move {
				server
					.import_complete_task(
						process_complete_receiver,
						object_complete_receiver,
						&event_sender,
					)
					.await
			}
		}));

		// Spawn the processes task.
		let processes_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				server
					.import_processes_task(process_receiver, &progress)
					.await
			}
		}));

		// Spawn the objects task.
		let objects_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move { server.import_objects_task(object_receiver, &progress).await }
		}));

		// Spawn a task that sends items from the stream to the other tasks.
		let task = AbortOnDropHandle::new(tokio::spawn({
			let event_sender = event_sender.clone();
			async move {
				// Read the items from the stream and send them to the tasks.
				loop {
					let item = match stream.try_next().await {
						Ok(Some(item)) => item,
						Ok(None) => break,
						Err(error) => {
							event_sender.send(Err(error)).await.ok();
							return;
						},
					};
					let complete_sender_future = match &item {
						tg::export::Item::Process(item) => process_complete_sender
							.send(item.id.clone())
							.map_err(|_| ())
							.left_future(),
						tg::export::Item::Object(item) => object_complete_sender
							.send(item.id.clone())
							.map_err(|_| ())
							.right_future(),
					};
					let (process_future, object_future) = match item {
						tg::export::Item::Process(item) => (
							process_sender.send(item).map_err(|_| ()).left_future(),
							future::ok(()).left_future(),
						),
						tg::export::Item::Object(item) => (
							future::ok(()).right_future(),
							object_sender.send(item).map_err(|_| ()).right_future(),
						),
					};
					let result =
						futures::try_join!(complete_sender_future, process_future, object_future);
					if result.is_err() {
						event_sender
							.send(Err(tg::error!(?result, "failed to send the item")))
							.await
							.ok();
						return;
					}
				}

				// Close the channels
				drop(process_complete_sender);
				drop(object_complete_sender);
				drop(process_sender);
				drop(object_sender);

				// Join the processes and objects tasks.
				let result = futures::try_join!(processes_task, objects_task);

				// Abort the complete task.
				complete_task.abort();

				match result {
					Ok((Ok(()), Ok(()))) => {
						let event = tg::import::Event::End;
						event_sender.send(Ok(event)).await.ok();
					},
					Ok((_, Err(error)) | (Err(error), _)) => {
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
		let processes = arg
			.items
			.iter()
			.filter_map(|item| item.clone().left())
			.collect::<Vec<_>>();
		let objects = arg
			.items
			.iter()
			.filter_map(|item| item.clone().right())
			.collect::<Vec<_>>();
		let (process_completes, object_completes) = futures::try_join!(
			self.try_get_process_complete_batch(&processes),
			self.try_get_object_complete_batch(&objects),
		)?;
		for process_complete in process_completes {
			let Some(process_complete) = process_complete else {
				return Ok(false);
			};
			let complete = process_complete.complete
				&& process_complete.commands_complete
				&& process_complete.outputs_complete;
			if !complete {
				return Ok(false);
			}
		}
		for object_complete in object_completes {
			let Some(object_complete) = object_complete else {
				return Ok(false);
			};
			if !object_complete {
				return Ok(false);
			}
		}
		Ok(true)
	}

	async fn import_complete_task(
		&self,
		process_complete_receiver: tokio::sync::mpsc::Receiver<tg::process::Id>,
		object_complete_receiver: tokio::sync::mpsc::Receiver<tg::object::Id>,
		event_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::import::Event>>,
	) -> tg::Result<()> {
		// Create the process future.
		let process_stream = ReceiverStream::new(process_complete_receiver);
		let process_stream = pin!(process_stream);
		let process_future = process_stream
			.ready_chunks(PROCESS_COMPLETE_BATCH_SIZE)
			.for_each_concurrent(PROCESS_COMPLETE_CONCURRENCY, {
				let server = self.clone();
				let event_sender = event_sender.clone();
				move |ids| {
					let server = server.clone();
					let event_sender = event_sender.clone();
					async move {
						let result = server.try_get_process_complete_batch(&ids).await;
						let outputs = match result {
							Ok(outputs) => outputs,
							Err(error) => {
								tracing::error!(?error, "failed to get the process complete batch");
								return;
							},
						};
						for (id, output) in std::iter::zip(ids, outputs) {
							let Some(output) = output else {
								continue;
							};
							if output.complete
								|| output.commands_complete
								|| output.outputs_complete
							{
								let complete =
									tg::import::Complete::Process(tg::import::ProcessComplete {
										commands_complete: output.commands_complete,
										complete: output.complete,
										id,
										outputs_complete: output.outputs_complete,
									});
								let event = tg::import::Event::Complete(complete);
								event_sender.send(Ok(event)).await.ok();
							}
						}
					}
				}
			});

		// Create the object future.
		let object_stream = ReceiverStream::new(object_complete_receiver);
		let object_stream = pin!(object_stream);
		let object_future = object_stream
			.ready_chunks(OBJECT_COMPLETE_BATCH_SIZE)
			.for_each_concurrent(OBJECT_COMPLETE_CONCURRENCY, {
				let server = self.clone();
				let event_sender = event_sender.clone();
				move |ids| {
					let server = server.clone();
					let event_sender = event_sender.clone();
					async move {
						let result = server.try_get_object_complete_batch(&ids).await;
						let outputs = match result {
							Ok(outputs) => outputs,
							Err(error) => {
								tracing::error!(?error, "failed to get the object complete batch");
								return;
							},
						};
						for (id, output) in std::iter::zip(ids, outputs) {
							let Some(output) = output else {
								continue;
							};
							if output {
								let complete =
									tg::import::Complete::Object(tg::import::ObjectComplete { id });
								let event = tg::import::Event::Complete(complete);
								event_sender.send(Ok(event)).await.ok();
							}
						}
					}
				}
			});

		// Join the futures.
		futures::join!(process_future, object_future);

		Ok(())
	}

	async fn import_processes_task(
		&self,
		process_receiver: tokio::sync::mpsc::Receiver<tg::export::ProcessItem>,
		progress: &Progress,
	) -> tg::Result<()> {
		let stream = ReceiverStream::new(process_receiver);
		let mut stream = pin!(stream);
		while let Some(item) = stream.next().await {
			let arg = tg::process::put::Arg { data: item.data };
			self.put_process(&item.id, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to put the process"))?;
			progress.increment_processes();
		}
		Ok(())
	}

	async fn import_objects_task(
		&self,
		object_receiver: tokio::sync::mpsc::Receiver<tg::export::ObjectItem>,
		progress: &Arc<Progress>,
	) -> tg::Result<()> {
		// Choose the batch parameters.
		let (concurrency, max_objects_per_batch, max_bytes_per_batch) = match &self.store {
			#[cfg(feature = "foundationdb")]
			Store::Fdb(_) => (8, 1_000, 5_000_000),
			Store::Lmdb(_) => (1, 1_000, 5_000_000),
			Store::Memory(_) => (1, 1, u64::MAX),
			Store::S3(_) => (256, 1, u64::MAX),
		};

		// Create a stream of batches.
		struct State {
			object_receiver: tokio::sync::mpsc::Receiver<tg::export::ObjectItem>,
			item: Option<tg::export::ObjectItem>,
		}
		let state = State {
			item: None,
			object_receiver,
		};
		let stream = stream::unfold(state, |mut state| async {
			let mut batch_bytes = state
				.item
				.as_ref()
				.map(|item| item.bytes.len().to_u64().unwrap())
				.unwrap_or_default();
			let mut batch = state.item.take().map(|item| vec![item]).unwrap_or_default();
			while let Some(item) = state.object_receiver.recv().await {
				let size = item.bytes.len().to_u64().unwrap();
				if !batch.is_empty()
					&& (batch.len() + 1 >= max_objects_per_batch
						|| batch_bytes + size >= max_bytes_per_batch)
				{
					state.item.replace(item);
					return Some((batch, state));
				}
				batch_bytes += size;
				batch.push(item);
			}
			if batch.is_empty() {
				return None;
			}
			Some((batch, state))
		});

		// Write the batches.
		stream
			.map(Ok)
			.try_for_each_concurrent(concurrency, |batch| {
				self.import_objects_task_inner(batch, progress)
			})
			.await?;

		Ok(())
	}

	async fn import_objects_task_inner(
		&self,
		batch: Vec<tg::export::ObjectItem>,
		progress: &Arc<Progress>,
	) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Create the store future.
		let store_future = {
			async {
				let batch = batch
					.clone()
					.into_iter()
					.map(|item| (item.id, Some(item.bytes), None))
					.collect();
				let arg = crate::store::PutBatchArg {
					objects: batch,
					touched_at,
				};
				self.store.put_batch(arg).await?;
				Ok::<_, tg::Error>(())
			}
		};

		// Create the messenger future.
		let messenger_future = {
			async {
				batch
					.iter()
					.map(|item| async {
						let data =
							tg::object::Data::deserialize(item.id.kind(), item.bytes.clone())?;
						let message =
							crate::index::Message::PutObject(crate::index::PutObjectMessage {
								cache_reference: None,
								children: data.children().collect(),
								id: item.id.clone(),
								size: item.bytes.len().to_u64().unwrap(),
								touched_at,
							});
						let message = serde_json::to_vec(&message).map_err(|source| {
							tg::error!(!source, "failed to serialize the message")
						})?;
						let _published = self
							.messenger
							.stream_publish("index".to_owned(), message.into())
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to publish the message")
							})?;
						Ok::<_, tg::Error>(())
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect::<()>()
					.await?;
				Ok::<_, tg::Error>(())
			}
		};

		// Join the store and messenger futures.
		futures::try_join!(store_future, messenger_future)?;

		// Update the progress.
		let objects = batch.len().to_u64().unwrap();
		let bytes = batch
			.iter()
			.map(|item| item.bytes.len().to_u64().unwrap())
			.sum();
		progress.increment_objects(objects);
		progress.increment_bytes(bytes);

		Ok(())
	}

	pub(crate) async fn handle_import_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the arg.
		let arg = request
			.query_params::<tg::import::QueryArg>()
			.transpose()?
			.ok_or_else(|| tg::error!("query parameters required"))?
			.into();

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
			let Some(item) = tg::export::Item::from_reader(&mut reader).await? else {
				return Ok(None);
			};
			if let tg::export::Item::Object(object) = &item {
				let actual = tg::object::Id::new(object.id.kind(), &object.bytes);
				if object.id != actual {
					return Err(tg::error!(%expected = object.id, %actual, "invalid object id"));
				}
			}
			Ok(Some((item, reader)))
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

impl Progress {
	fn new(processes: bool) -> Self {
		Self {
			processes: processes.then(|| AtomicU64::new(0)),
			objects: AtomicU64::new(0),
			bytes: AtomicU64::new(0),
		}
	}

	fn increment_processes(&self) {
		self.processes
			.as_ref()
			.unwrap()
			.fetch_update(
				std::sync::atomic::Ordering::SeqCst,
				std::sync::atomic::Ordering::SeqCst,
				|current| Some(current + 1),
			)
			.unwrap();
	}

	fn increment_objects(&self, objects: u64) {
		self.objects
			.fetch_update(
				std::sync::atomic::Ordering::SeqCst,
				std::sync::atomic::Ordering::SeqCst,
				|current| Some(current + objects),
			)
			.unwrap();
	}

	fn increment_bytes(&self, bytes: u64) {
		self.bytes
			.fetch_update(
				std::sync::atomic::Ordering::SeqCst,
				std::sync::atomic::Ordering::SeqCst,
				|current| Some(current + bytes),
			)
			.unwrap();
	}

	fn stream(self: Arc<Self>) -> impl Stream<Item = tg::Result<tg::import::Progress>> {
		let progress = self.clone();
		let interval = Duration::from_millis(100);
		let interval = tokio::time::interval(interval);
		IntervalStream::new(interval).skip(1).map(move |_| {
			let processes = progress
				.processes
				.as_ref()
				.map(|processes| processes.swap(0, std::sync::atomic::Ordering::SeqCst));
			let objects = progress
				.objects
				.swap(0, std::sync::atomic::Ordering::SeqCst);
			let bytes = progress.bytes.swap(0, std::sync::atomic::Ordering::SeqCst);
			Ok(tg::import::Progress {
				processes,
				objects,
				bytes,
			})
		})
	}
}
