use crate::Server;
use futures::{
	FutureExt, Stream, StreamExt as _, TryFutureExt as _, TryStreamExt as _, future, stream,
};
use indoc::formatdoc;
use num::ToPrimitive;
use std::{
	pin::{Pin, pin},
	sync::{Arc, atomic::AtomicU64},
	time::Duration,
};
use tangram_client as tg;
use tangram_database::{self as db, Database as _, Query as _};
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{Body, request::Ext};
use tokio::task::JoinSet;
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};
use tokio_util::task::AbortOnDropHandle;

#[derive(Debug)]
struct Progress {
	processes: Option<AtomicU64>,
	objects: AtomicU64,
	bytes: AtomicU64,
}

impl Server {
	pub async fn import(
		&self,
		arg: tg::import::Arg,
		mut stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Item>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::import::Event>> + Send + 'static> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote {
			let client = self.get_remote_client(remote.clone()).await?;
			let arg = tg::import::Arg {
				remote: None,
				..arg
			};
			let stream = client.import(arg, stream).await?;
			return Ok(stream.left_stream());
		}

		// Create the progress.
		let processes = arg.items.iter().any(Either::is_left);
		let progress = Arc::new(Progress::new(processes));

		// Create the channels.
		let (event_sender, event_receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::import::Event>>(256);
		let (complete_sender, complete_receiver) =
			tokio::sync::mpsc::channel::<tg::export::Item>(256);
		let (process_sender, process_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ProcessItem>(256);
		let (object_sender, object_receiver) =
			tokio::sync::mpsc::channel::<tg::export::ObjectItem>(256);

		// Create the complete task.
		let complete_task = tokio::spawn({
			let server = self.clone();
			let event_sender = event_sender.clone();
			async move {
				let result = server
					.import_complete_task(complete_receiver, &event_sender)
					.await;
				if let Err(error) = result {
					event_sender.send(Err(error)).await.ok();
				}
			}
		});
		let complete_task_abort_handle = complete_task.abort_handle();
		let complete_task_abort_handle =
			scopeguard::guard(complete_task_abort_handle, |complete_task_abort_handle| {
				complete_task_abort_handle.abort();
			});

		// Create the processes task.
		let processes_task = tokio::spawn({
			let server = self.clone();
			let event_sender = event_sender.clone();
			let progress = progress.clone();
			async move {
				let result = server
					.import_processes_task(process_receiver, &event_sender, &progress)
					.await;
				if let Err(error) = result {
					event_sender.send(Err(error)).await.ok();
				}
			}
		});
		let processes_task_abort_handle = processes_task.abort_handle();
		let processes_task_abort_handle =
			scopeguard::guard(processes_task_abort_handle, |processes_task_abort_handle| {
				processes_task_abort_handle.abort();
			});

		// Create the objects task.
		let objects_task = tokio::spawn({
			let server = self.clone();
			let event_sender = event_sender.clone();
			let progress = progress.clone();
			async move {
				let result = server.import_objects_task(object_receiver, &progress).await;
				if let Err(error) = result {
					event_sender.send(Err(error)).await.ok();
				}
			}
		});
		let objects_task_abort_handle = objects_task.abort_handle();
		let objects_task_abort_handle =
			scopeguard::guard(objects_task_abort_handle, |objects_task_abort_handle| {
				objects_task_abort_handle.abort();
			});

		// Spawn a task that sends items from the stream to the other tasks.
		let task = tokio::spawn({
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
					let complete_sender_future = complete_sender.send(item.clone()).map_err(|_| ());
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
				drop(complete_sender);
				drop(process_sender);
				drop(object_sender);

				// Join the processes and objects tasks.
				let result = futures::try_join!(processes_task, objects_task);

				match result {
					Ok(_) => {
						let event = tg::import::Event::End;
						event_sender.send(Ok(event)).await.ok();
					},
					Err(error) => {
						let error = tg::error!(!error, "failed to join the task");
						event_sender.send(Err(error)).await.ok();
					},
				}
			}
		});

		// Create the stream.
		let event_stream = ReceiverStream::new(event_receiver);
		let progress_stream = progress.stream().map_ok(tg::import::Event::Progress);
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = stream::select(event_stream, progress_stream)
			.take_while(|event| future::ready(!matches!(event, Ok(tg::import::Event::End))))
			.attach(abort_handle)
			.attach(complete_task_abort_handle)
			.attach(processes_task_abort_handle)
			.attach(objects_task_abort_handle);

		Ok(stream.right_stream())
	}

	async fn import_complete_task(
		&self,
		complete_receiver: tokio::sync::mpsc::Receiver<tg::export::Item>,
		event_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::import::Event>>,
	) -> tg::Result<()> {
		let stream = ReceiverStream::new(complete_receiver);
		let mut stream = pin!(stream);
		let mut join_set = JoinSet::new();
		while let Some(item) = stream.next().await {
			join_set.spawn({
				let server = self.clone();
				let event_sender = event_sender.clone();
				async move {
					match item {
						tg::export::Item::Process(tg::export::ProcessItem { id, .. }) => {
							let result = server.try_get_import_process_complete(&id).await;
							let process_complete = match result {
								Ok(process_complete) => process_complete,
								Err(error) => {
									tracing::error!(?error, "failed to get process complete");
									return;
								},
							};
							let Some(process_complete) = process_complete else {
								return;
							};
							if !(process_complete.complete
								|| process_complete.commands_complete
								|| process_complete.logs_complete
								|| process_complete.outputs_complete)
							{
								return;
							}
							let event = tg::import::Event::Complete(tg::import::Complete::Process(
								process_complete,
							));
							event_sender.send(Ok(event)).await.ok();
						},
						tg::export::Item::Object(tg::export::ObjectItem { id, .. }) => {
							let result = server.try_get_import_object_complete(&id).await;
							let object_complete = match result {
								Ok(object_complete) => object_complete,
								Err(error) => {
									tracing::error!(?error, "failed to get object complete");
									return;
								},
							};
							let Some(object_complete) = object_complete else {
								return;
							};
							if !object_complete {
								return;
							}
							let event = tg::import::Event::Complete(tg::import::Complete::Object(
								tg::import::ObjectComplete { id },
							));
							event_sender.send(Ok(event)).await.ok();
						},
					}
				}
			});
		}
		Ok(())
	}

	async fn try_get_import_process_complete(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::import::ProcessComplete>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the process complete fields.
		#[derive(serde::Deserialize)]
		struct Row {
			complete: bool,
			commands_complete: bool,
			logs_complete: bool,
			outputs_complete: bool,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					complete,
					commands_complete,
					logs_complete,
					outputs_complete
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let Some(row) = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let output = tg::import::ProcessComplete {
			commands_complete: row.commands_complete,
			complete: row.complete,
			id: id.clone(),
			logs_complete: row.logs_complete,
			outputs_complete: row.outputs_complete,
		};

		Ok(Some(output))
	}

	async fn try_get_import_object_complete(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<bool>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the object complete field.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select complete
				from objects
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let output = connection
			.query_optional_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	async fn import_processes_task(
		&self,
		database_process_receiver: tokio::sync::mpsc::Receiver<tg::export::ProcessItem>,
		event_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::import::Event>>,
		progress: &Progress,
	) -> tg::Result<()> {
		let stream = ReceiverStream::new(database_process_receiver);
		let mut stream = pin!(stream);
		while let Some(item) = stream.next().await {
			// Put the process.
			let arg = tg::process::put::Arg { data: item.data };
			self.put_process(&item.id, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to put the process"))?;
			progress.increment_processes();

			let process_complete = self
				.try_get_import_process_complete(&item.id)
				.await?
				.ok_or_else(|| tg::error!("expected the process to exist"))?;
			if process_complete.complete
				|| process_complete.commands_complete
				|| process_complete.logs_complete
				|| process_complete.outputs_complete
			{
				let event =
					tg::import::Event::Complete(tg::import::Complete::Process(process_complete));
				event_sender.send(Ok(event)).await.ok();
			}
		}
		Ok(())
	}

	async fn import_objects_task(
		&self,
		object_receiver: tokio::sync::mpsc::Receiver<tg::export::ObjectItem>,
		progress: &Progress,
	) -> tg::Result<()> {
		let stream = ReceiverStream::new(object_receiver);
		let mut stream = pin!(stream);
		while let Some(item) = stream.next().await {
			let size = item.bytes.len().to_u64().unwrap();
			self.store
				.as_ref()
				.unwrap()
				.put(item.id, item.bytes)
				.await?;
			progress.increment_objects(1);
			progress.increment_bytes(size);
		}
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

		// Create the incoming stream.
		let body = request.reader();
		let stream = stream::try_unfold(body, |mut reader| async move {
			let Some(item) = tg::export::Item::from_reader(&mut reader).await? else {
				return Ok(None);
			};
			Ok(Some((item, reader)))
		})
		.boxed();

		// Create the outgoing stream.
		let stream = handle.import(arg, stream).await?;

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
