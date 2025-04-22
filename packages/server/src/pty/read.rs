use crate::Server;
use futures::{Stream, StreamExt as _, TryStreamExt as _, future};
use tangram_client as tg;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _};
use tangram_messenger::Messenger as _;

impl Server {
	pub async fn read_pty(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::read::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let stream = remote.read_pty(id, arg).await?;
			return Ok(stream.left_stream());
		}

		// Create the stream.
		let stream = if arg.master {
			format!("{id}_master_reader")
		} else {
			format!("{id}_master_writer")
		};
		let stream = self
			.messenger
			.stream_subscribe(stream, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the stream"))?
			.map_err(|source| tg::error!(!source, "failed to get a message"))
			.and_then(|message| async move {
				message
					.acker
					.ack()
					.await
					.map_err(|source| tg::error!(!source, "failed to ack the message"))?;
				let event = serde_json::from_slice::<tg::pty::Event>(&message.payload)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok::<_, tg::Error>(event)
			})
			.take_while_inclusive(|result| {
				future::ready(!matches!(result, Ok(tg::pty::Event::End) | Err(_)))
			});

		Ok(stream.right_stream())
	}

	pub(crate) async fn handle_read_pty_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the stream.
		let stream = handle.read_pty(&id, arg).await?;

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

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
