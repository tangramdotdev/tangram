use crate as tg;
use bytes::Bytes;
use http_body_util::BodyExt as _;

pub type Incoming = hyper::body::Incoming;

pub type Outgoing = http_body_util::combinators::UnsyncBoxBody<Bytes, tg::Error>;

#[must_use]
pub fn empty() -> Outgoing {
	http_body_util::Empty::new()
		.map_err(|_| unreachable!())
		.boxed_unsync()
}

#[must_use]
pub fn full(chunk: impl Into<Bytes>) -> Outgoing {
	http_body_util::Full::new(chunk.into())
		.map_err(|_| unreachable!())
		.boxed_unsync()
}

// // Create a URI from a path and a query.
// pub fn uri(path: impl std::fmt::Display, query: &impl serde::Serialize) -> tg::Result<http::Uri> {
// 	let query = serde_urlencoded::to_string(query)
// 		.map_err(|source| tg::error!(!source, "failed to serialize the query"))?;
// 	let mut path_and_query = path.to_string();
// 	if !query.is_empty() {
// 		path_and_query.push('?');
// 		path_and_query.push_str(&query);
// 	}
// 	let uri = http::Uri::builder()
// 		.path_and_query(path_and_query)
// 		.build()
// 		.map_err(|source| tg::error!(!source, "failed to build the URI"))?;
// 	Ok(uri)
// }

// pub enum Outgoing {
// 	Empty,
// 	Full(Option<Bytes>),
// 	Json(Option<Box<dyn erased_serde::Serialize>>),
// 	Stream(BoxStream<'static, Result<Bytes, ()>>),
// 	Body(http_body_util::combinators::UnsyncBoxBody<Bytes, ()>),
// }

// impl Outgoing {
// 	pub fn empty() -> Self {
// 		Self::Empty
// 	}

// 	pub fn full(bytes: Bytes) -> Self {
// 		Self::Full(Some(bytes))
// 	}

// 	pub fn json(json: impl Into<Box<dyn erased_serde::Serialize>>) -> Self {
// 		Self::Json(Some(json.into()))
// 	}

// 	pub fn stream(stream: impl Into<BoxStream<'static, Result<Bytes, tg::Error>>>) -> Self {
// 		Self::Stream(stream.into())
// 	}
// }

// impl hyper::body::Body for Outgoing {
// 	type Data = Bytes;

// 	type Error = tg::Error;

// 	fn poll_frame(
// 		self: std::pin::Pin<&mut Self>,
// 		cx: &mut std::task::Context<'_>,
// 	) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
// 		match self.get_mut() {
// 			Outgoing::Empty => std::task::Poll::Ready(None),
// 			Outgoing::Full(option) => {
// 				let option = option.take().map(hyper::body::Frame::data).map(Ok);
// 				std::task::Poll::Ready(option)
// 			},
// 			Outgoing::Json(option) => {
// 				let take = option
// 					.take()
// 					.map(|value| serde_json::to_string(&value))
// 					.map(|result| result.map(Bytes::from).map(hyper::body::Frame::data));
// 				std::task::Poll::Ready(take)
// 			},
// 			Outgoing::Stream(stream) => stream
// 				.poll_next_unpin(cx)
// 				.map(|option| option.map(|result| result.map(hyper::body::Frame::data))),
// 			Outgoing::Body(body) => pin!(body).poll_frame(cx),
// 		}
// 	}
// }
