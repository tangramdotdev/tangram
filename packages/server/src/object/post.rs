use crate::Server;
use futures::{future, stream, Stream, StreamExt as _, TryStreamExt as _};
use std::pin::{pin, Pin};
use tangram_client as tg;
use tangram_futures::stream::Ext as _;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
use tokio::task::JoinSet;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tokio_util::task::AbortOnDropHandle;

impl Server {
	pub(crate) async fn post_objects(
		&self,
		mut stream: Pin<
			Box<dyn Stream<Item = tg::Result<tg::object::post::Item>> + Send + 'static>,
		>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::post::Event>> + Send + 'static> {
		let (event_sender, event_receiver) =
			tokio::sync::mpsc::unbounded_channel::<tg::Result<tg::object::post::Event>>();
		let (database_sender, database_receiver) =
			tokio::sync::mpsc::channel::<tg::object::post::Item>(100);
		let (store_sender, mut store_receiver) =
			tokio::sync::mpsc::channel::<tg::object::post::Item>(100);

		// Create the database task.
		let database_task = tokio::spawn({
			let server = self.clone();
			let event_sender = event_sender.clone();
			async move {
				let stream = ReceiverStream::new(database_receiver);
				let mut stream = pin!(stream);
				while let Some(item) = stream.next().await {
					let arg = tg::object::put::Arg { bytes: item.bytes };
					let output = server.put_object(&item.id, arg).await?;
					let event = if output.complete {
						tg::object::post::Event::Complete(item.id.clone())
					} else {
						tg::object::post::Event::Incomplete(item.id.clone())
					};
					event_sender.send(Ok(event)).ok();
				}
				Ok(())
			}
		});

		// Create the store task.
		let store_task = tokio::spawn({
			let server = self.clone();
			async move {
				let mut join_set = JoinSet::new();
				loop {
					let Some(item) = store_receiver.recv().await else {
						break;
					};
					if let Some(store) = server.store.clone() {
						join_set.spawn(async move { store.put(item.id, item.bytes).await });
						while let Some(result) = join_set.try_join_next() {
							result
								.map_err(|source| tg::error!(!source, "a store task panicked"))??;
						}
					}
				}
				while let Some(result) = join_set.join_next().await {
					result.map_err(|source| tg::error!(!source, "a store task panicked"))??;
				}
				Ok(())
			}
		});

		// Spawn a task that sends items from the stream to the database and store tasks.
		let task = tokio::spawn({
			let event_sender = event_sender.clone();
			async move {
				loop {
					let item = match stream.try_next().await {
						Ok(Some(item)) => item,
						Ok(None) => break,
						Err(error) => {
							event_sender.send(Err(error)).ok();
							return;
						},
					};
					let result = future::try_join(
						database_sender.send(item.clone()),
						store_sender.send(item.clone()),
					)
					.await;
					if result.is_err() {
						event_sender
							.send(Err(tg::error!("failed to send the item")))
							.ok();
						return;
					}
				}

				// Close the channels
				drop(database_sender);
				drop(store_sender);

				// Join the tasks.
				let result = future::try_join(database_task, store_task).await;
				if let (Err(error), _) | (_, Err(error)) = result.unwrap() {
					event_sender.send(Err(error)).ok();
				}

				// Send the end event.
				event_sender.send(Ok(tg::object::post::Event::End)).ok();
			}
		});

		// Create the event stream.
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = UnboundedReceiverStream::new(event_receiver).attach(abort_handle);

		Ok(stream)
	}
}

impl Server {
	pub(crate) async fn handle_post_object_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Create the incoming stream.
		let body = request.reader();
		let stream = stream::try_unfold(body, |mut reader| async move {
			let Some(item) = tg::object::post::Item::try_deserialize(&mut reader).await? else {
				return Ok(None);
			};
			Ok(Some((item, reader)))
		})
		.boxed();

		// Create the outgoing stream.
		let stream = handle.post_objects(stream).await?.boxed();

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
				(Some(content_type), Outgoing::sse(stream))
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
