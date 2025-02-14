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
		Self::push_or_pull(&remote, self, &arg).await
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
		let export_arg = tg::export::Arg {
			items: arg.items.clone(),
			remote: None,
		};
		let (export_event_sender, export_event_receiver) = tokio::sync::mpsc::channel(1024);
		let export_event_stream = ReceiverStream::new(export_event_receiver);
		let export_item_stream = src.export(export_arg, export_event_stream.boxed()).await?;
		let import_arg = tg::import::Arg {
			items: arg.items.clone(),
			remote: None,
		};
		let import_event_stream = dst.import(import_arg, export_item_stream.boxed()).await?;
		let (progress_event_sender, progress_event_receiver) = tokio::sync::mpsc::channel(1024);
		let task = tokio::spawn(async move {
			let mut import_event_stream = pin!(import_event_stream);
			while let Some(result) = import_event_stream.next().await {
				match result {
					Ok(tg::import::Event::Progress(event)) => {
						progress_event_sender.send(Ok(event)).await.unwrap();
					},
					Ok(tg::import::Event::Complete(id)) => {
						export_event_sender
							.send(Ok(tg::export::Event::Complete(id)))
							.await
							.unwrap();
					},
					Err(error) => {
						progress_event_sender.send(Err(error)).await.unwrap();
					},
				}
			}
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
