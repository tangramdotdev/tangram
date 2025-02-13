use crate as tg;
use bytes::Bytes;
use futures::{stream, Stream};
use std::pin::Pin;
use tangram_either::Either;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	items: Vec<Either<tg::process::Id, tg::object::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	remote: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Event {
	pub id: Either<tg::process::Id, tg::object::Id>,
	pub bytes: Bytes,
}

impl tg::Client {
	pub async fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static> {
		// // Create the request.
		// let method = http::Method::POST;
		// let uri = "/export";
		// let request = http::request::Builder::default()
		// 	.method(method)
		// 	.uri(uri)
		// 	.header(http::header::ACCEPT, mime::TEXT_EVENT_STREAM.to_string())
		// 	.json(arg)
		// 	.unwrap();

		// // Send the request.
		// let response = self.send(request).await?;

		// // Check for an error.
		// if !response.status().is_success() {
		// 	let error = response.json().await?;
		// 	return Err(error);
		// }

		// // Create stream from the response.
		// let stream = response
		// 	.sse()
		// 	.map_err(|source| tg::error!(!source, "failed to read an event"))
		// 	.and_then(|event| {
		// 		future::ready(
		// 			if event.event.as_deref().is_some_and(|event| event == "error") {
		// 				match event.try_into() {
		// 					Ok(error) | Err(error) => Err(error),
		// 				}
		// 			} else {
		// 				event.try_into()
		// 			},
		// 		)
		// 	});

		// Ok(stream)
		Ok(stream::empty())
	}
}
