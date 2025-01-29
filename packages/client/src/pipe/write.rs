use crate::{self as tg, Client};
use futures::{Stream, StreamExt as _};
use http_body_util::StreamBody;
use std::pin::Pin;
use tangram_http::{incoming::response::Ext as _, Outgoing};

impl Client {
	pub async fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>>,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/pipes/{id}/write");

		// Create the body.
		let body = Outgoing::body(StreamBody::new(stream.map(|result| match result {
			Ok(event) => match event {
				tg::pipe::Event::Chunk(bytes) => Ok(hyper::body::Frame::data(bytes)),
				tg::pipe::Event::End => {
					let mut trailers = http::HeaderMap::new();
					trailers.insert("x-tg-event", http::HeaderValue::from_static("end"));
					Ok(hyper::body::Frame::trailers(trailers))
				},
			},
			Err(error) => {
				let mut trailers = http::HeaderMap::new();
				trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
				let json = serde_json::to_string(&error).unwrap();
				trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
				Ok(hyper::body::Frame::trailers(trailers))
			},
		})));

		// Create the request.
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.unwrap();

		// Send the request.
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}

		Ok(())
	}
}
