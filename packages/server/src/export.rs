use crate::Server;
use dashmap::DashMap;
use futures::{
	future,
	stream::{self, FuturesUnordered},
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use itertools::Itertools as _;
use num::ToPrimitive as _;
use smallvec::SmallVec;
use std::{
	panic::AssertUnwindSafe,
	pin::{pin, Pin},
	sync::{Arc, RwLock, Weak},
	time::Duration,
};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{request::Ext, Body};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

impl Server {
	pub async fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static> {
		Ok(stream::empty())
	}

	pub(crate) async fn handle_export_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the arg.
		let arg = request
			.query_params::<tg::export::QueryArg>()
			.transpose()?
			.ok_or_else(|| tg::error!("query parameters required"))?
			.into();

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Create the incoming stream.
		let stream = request
			.sse()
			.map_err(|source| tg::error!(!source, "failed to read an event"))
			.and_then(|event| {
				future::ready(
					if event.event.as_deref().is_some_and(|event| event == "error") {
						match event.try_into() {
							Ok(error) | Err(error) => Err(error),
						}
					} else {
						event.try_into()
					},
				)
			})
			.boxed();

		// Create the outgoing stream.
		let stream = handle.export(arg, stream).await?;

		// Create the response body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::APPLICATION, mime::OCTET_STREAM)) => {
				let content_type = mime::APPLICATION_OCTET_STREAM;
				let stream = stream.then(|result| async {
					let frame = match result {
						Ok(item) => {
							let bytes = item.to_bytes().await;
							hyper::body::Frame::data(bytes)
						},
						Err(error) => {
							let mut trailers = http::HeaderMap::new();
							trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
							let json = serde_json::to_string(&error).unwrap();
							trailers
								.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
							hyper::body::Frame::trailers(trailers)
						},
					};
					Ok::<_, tg::Error>(frame)
				});
				(Some(content_type), Body::with_stream(stream))
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
