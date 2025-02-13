use crate::Server;
use futures::{stream, Stream, StreamExt as _, TryStreamExt as _};
use std::pin::{pin, Pin};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{request::Ext, Body};
use tokio::task::JoinSet;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tokio_util::task::AbortOnDropHandle;

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

		let (event_sender, event_receiver) =
			tokio::sync::mpsc::unbounded_channel::<tg::Result<tg::import::Event>>();
		let (store_sender, mut store_receiver) =
			tokio::sync::mpsc::channel::<tg::export::Item>(256);
		let (complete_sender, complete_receiver) =
			tokio::sync::mpsc::channel::<tg::export::Item>(256);

		// Create the complete task.
		let complete_task = tokio::spawn({
			let event_sender = event_sender.clone();
			let server = self.clone();
			async move {
				let stream = ReceiverStream::new(complete_receiver);
				let mut stream = pin!(stream);
				while let Some(item) = stream.next().await {
					match item {
						tg::export::Item::Process { .. } => todo!(),
						tg::export::Item::Object { id, .. } => {
							let complete = server
								.try_get_object_metadata_local(&id)
								.await?
								.is_some_and(|metadata| metadata.complete);
							if complete {
								let event = tg::import::Event::Complete(Either::Right(id));
								event_sender.send(Ok(event)).ok();
							}
						},
					}
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
					match item {
						tg::export::Item::Process { .. } => todo!(),
						tg::export::Item::Object { id, bytes, .. } => {
							join_set.spawn({
								let server = server.clone();
								async move {
									if let Some(store) = &server.store {
										store.put(id, bytes).await?;
									}
									Ok::<_, tg::Error>(())
								}
							});
							while let Some(result) = join_set.try_join_next() {
								result.map_err(|source| {
									tg::error!(!source, "a store task panicked")
								})??;
							}
						},
					}
				}
				while let Some(result) = join_set.join_next().await {
					result.map_err(|source| tg::error!(!source, "a store task panicked"))??;
				}
				Ok(())
			}
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
							event_sender.send(Err(error)).ok();
							return;
						},
					};
					let result = futures::try_join!(
						complete_sender.send(item.clone()),
						store_sender.send(item.clone()),
					);
					if result.is_err() {
						event_sender
							.send(Err(tg::error!(?result, "failed to send the item")))
							.ok();
						return;
					}
				}

				// Close the channels
				drop(complete_sender);
				drop(store_sender);

				// Join the tasks.
				let result = futures::try_join!(complete_task, store_task);
				if let (Err(error), _) | (_, Err(error)) = result.unwrap() {
					event_sender.send(Err(error)).ok();
				}
			}
		});

		// Create the stream.
		let stream = UnboundedReceiverStream::new(event_receiver);
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = stream.attach(abort_handle);

		Ok(stream.right_stream())
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
