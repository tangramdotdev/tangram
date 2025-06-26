use crate::Server;
use futures::{Stream, StreamExt as _};
use tangram_client as tg;
use tangram_futures::task::Stop;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::prelude::*;

impl Server {
	pub(crate) async fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::signal::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + 'static>,
	> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let stream = remote
				.try_get_process_signal_stream(id, arg)
				.await?
				.map(futures::StreamExt::boxed);
			return Ok(stream);
		}

		// Check if the process exists locally.
		if self.try_get_process_local(id).await?.is_none() {
			return Ok(None);
		}

		let stream = self
			.messenger
			.subscribe(format!("processes.{id}.signal"), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream"))?
			.map(|message| {
				serde_json::from_slice::<tg::process::signal::get::Event>(&message.payload)
					.map_err(|_| tg::error!("failed to deserialize the message"))
			})
			.boxed();

		Ok(Some(stream))
	}

	pub(crate) async fn handle_get_process_signal_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the args.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Get the stream.
		let Some(stream) = handle.try_get_process_signal_stream(&id, arg).await? else {
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
