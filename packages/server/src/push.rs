use crate::Server;
use futures::{Stream, StreamExt as _};
use std::pin::pin;
use tangram_client as tg;
use tangram_futures::stream::Ext;
use tangram_http::{request::Ext as _, Body};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

impl Server {
	pub async fn push(
		&self,
		arg: tg::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let remote = self.get_remote_client(arg.remote.clone()).await?;
		Self::push_or_pull(self, &remote, &arg).await
	}

	pub(crate) async fn push_or_pull<S, D>(
		src: &S,
		dst: &D,
		arg: &tg::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static + use<S, D>,
	>
	where
		S: tg::Handle,
		D: tg::Handle,
	{
		let (export_item_sender, export_item_receiver) = tokio::sync::mpsc::channel(1024);
		let (progress_event_sender, progress_event_receiver) = tokio::sync::mpsc::channel(1024);
		let (import_event_sender, import_event_receiver) = tokio::sync::mpsc::channel(1024);
		let export_arg = tg::export::Arg {
			commands: arg.commands,
			items: arg.items.clone(),
			logs: arg.logs,
			outputs: arg.outputs,
			recursive: arg.recursive,
			remote: None,
		};
		let import_event_stream = ReceiverStream::new(import_event_receiver);
		let export_event_stream = src.export(export_arg, import_event_stream.boxed()).await?;
		let import_arg = tg::import::Arg {
			items: arg.items.clone(),
			remote: None,
		};
		let export_item_stream = ReceiverStream::new(export_item_receiver);
		let import_event_stream = dst.import(import_arg, export_item_stream.boxed()).await?;
		let task = tokio::spawn(async move {
			let export_future = {
				let progress_event_sender = progress_event_sender.clone();
				async move {
					let mut export_event_stream = pin!(export_event_stream);
					while let Some(result) = export_event_stream.next().await {
						match result {
							Ok(tg::export::Event::Item(item)) => {
								let result = export_item_sender.send(Ok(item)).await;
								if let Err(error) = result {
									progress_event_sender
										.send(Err(tg::error!(!error, "failed to send export item")))
										.await
										.ok();
									break;
								}
							},
							Ok(tg::export::Event::Progress(tg::progress::Event::Output(()))) => (),
							Ok(tg::export::Event::Progress(event)) => {
								progress_event_sender.send(Ok(event)).await.ok();
							},
							Err(error) => {
								progress_event_sender.send(Err(error)).await.ok();
							},
						}
					}
				}
			};
			let import_future = {
				let progress_event_sender = progress_event_sender.clone();
				async move {
					let mut import_event_stream = pin!(import_event_stream);
					while let Some(result) = import_event_stream.next().await {
						if let Err(error) = &result {
							progress_event_sender.send(Err(error.clone())).await.ok();
						}
						import_event_sender.send(result).await.ok();
					}
				}
			};
			futures::join!(export_future, import_future);
			progress_event_sender
				.send(Ok(tg::progress::Event::Output(())))
				.await
				.ok();
		});
		let abort_handle = AbortOnDropHandle::new(task);
		let progress_event_stream =
			ReceiverStream::new(progress_event_receiver).attach(abort_handle);
		Ok(progress_event_stream)
	}
}

impl Server {
	pub(crate) async fn handle_push_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.push(arg).await?;

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
