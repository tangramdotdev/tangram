use crate::Server;
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::task::Stop;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn try_get_process_wait_stream(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::process::wait::Event>> + Send + 'static>>
	{
		let Some(stream) = self.try_get_process_status(id).await? else {
			return Ok(None);
		};
		let server = self.clone();
		let id = id.clone();
		let stream = stream
			.try_filter_map(move |status| {
				let server = server.clone();
				let status = status.clone();
				let id = id.clone();
				async move {
					let status = match status {
						tg::process::Status::Canceled
						| tg::process::Status::Succeeded
						| tg::process::Status::Failed => status,
						_ => return Ok(None),
					};
					let output = server.get_process(&id).await?;
					let output = tg::process::wait::event::Output {
						error: output.error,
						exit: output.exit,
						output: output.output,
						status,
					};
					Ok::<_, tg::Error>(Some(tg::process::wait::Event::Output(output)))
				}
			})
			.boxed();
		Ok(Some(stream))
	}
}

impl Server {
	pub(crate) async fn handle_get_process_wait_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Get the stream.
		let Some(stream) = handle.try_get_process_wait_stream(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
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
