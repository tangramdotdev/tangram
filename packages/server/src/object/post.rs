use crate::Server;
use futures::{Stream, StreamExt, TryStreamExt};
use http_body_util::{BodyExt, BodyStream};

use std::pin::Pin;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext, Incoming, Outgoing};

impl Server {
	pub(crate) async fn post_object(
		&self,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::object::post::Object>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::post::Event>> + Send + 'static> {
		let server = self.clone();

		let stream = stream
			.try_filter_map(move |object| {
				let server = server.clone();
				async move {
					let arg = tg::object::put::Arg { bytes: object.data };
					let output = server.put_object(&object.id, arg).await?;
					if output.complete {
						let event = tg::object::post::Event::Complete(object.id);
						Ok(Some(event))
					} else {
						Ok(None)
					}
				}
			})
			.boxed();
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
		let body = request
			.into_body()
			.map_err(|source| tg::error!(!source, "failed to read the body"));
		let stream = BodyStream::new(body)
			.and_then(|frame| async {
				match frame.into_data() {
					Ok(bytes) => tg::object::post::Object::deserialize(bytes),
					Err(frame) => {
						let trailers = frame.into_trailers().unwrap();
						let event = trailers
							.get("x-tg-event")
							.ok_or_else(|| tg::error!("missing event"))?
							.to_str()
							.map_err(|source| tg::error!(!source, "invalid event"))?;
						match event {
							"error" => {
								let data = trailers
									.get("x-tg-data")
									.ok_or_else(|| tg::error!("missing data"))?
									.to_str()
									.map_err(|source| tg::error!(!source, "invalid data"))?;
								let error = serde_json::from_str(data).map_err(|source| {
									tg::error!(!source, "failed to deserialize the header value")
								})?;
								Err(error)
							},
							_ => Err(tg::error!("invalid event")),
						}
					},
				}
			})
			.boxed();

		// Create the outgoing stream.
		let stream = handle.post_object(stream).await?.boxed();

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
