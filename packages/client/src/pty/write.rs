use crate::{self as tg, Client};
use futures::{Stream, StreamExt as _};
use std::pin::Pin;
use tangram_http::{Body, response::Ext as _};

#[derive(Default, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
	pub master: bool,
}

impl Client {
	pub async fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>>,
	) -> tg::Result<()> {
		let method = http::Method::POST;
		let query = serde_urlencoded::to_string(arg).unwrap();
		let uri = format!("/ptys/{id}?{query}");

		// Create the body.
		let body = Body::with_stream(stream.map(move |result| {
			let event = match result {
				Ok(event) => match event {
					tg::pty::Event::Chunk(bytes) => hyper::body::Frame::data(bytes),
					tg::pty::Event::Size(size) => {
						let mut trailers = http::HeaderMap::new();
						trailers.insert("x-tg-event", http::HeaderValue::from_static("size"));
						let json = serde_json::to_string(&size).unwrap();
						trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
						hyper::body::Frame::trailers(trailers)
					},
					tg::pty::Event::End => {
						let mut trailers = http::HeaderMap::new();
						trailers.insert("x-tg-event", http::HeaderValue::from_static("end"));
						hyper::body::Frame::trailers(trailers)
					},
				},
				Err(error) => {
					let mut trailers = http::HeaderMap::new();
					trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
					let json = serde_json::to_string(&error).unwrap();
					trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
					hyper::body::Frame::trailers(trailers)
				},
			};
			Ok::<_, tg::Error>(event)
		}));

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
