use crate::Server;
use futures::{future, stream, Stream, StreamExt as _, TryStreamExt as _};
use std::{
	collections::BTreeMap,
	pin::Pin,
	sync::{Arc, RwLock},
};
use tangram_client as tg;
use tangram_http::{request::Ext, Body};

#[derive(Clone)]
struct Graph {
	inner: Arc<RwLock<GraphInner>>,
}

struct GraphInner {
	nodes: Vec<usize>,
	indices: BTreeMap<tg::object::Id, usize>,
}

impl Server {
	pub async fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Item>> + Send + 'static> {
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
			.query_params()
			.transpose()?
			.ok_or_else(|| tg::error!("query parameters required"))?;

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

impl Graph {
	const COMPLETE: usize = usize::MAX;

	fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(GraphInner {
				nodes: Vec::new(),
				indices: BTreeMap::new(),
			})),
		}
	}

	fn insert(&self, parent: Option<&tg::object::Id>, child: &tg::object::Id) {
		let mut inner = self.inner.write().unwrap();
		let index = inner.nodes.len();
		inner.indices.insert(child.clone(), index);
		if let Some(parent) = parent {
			let parent = inner.indices.get(parent).copied().unwrap();
			inner.nodes.push(parent);
		} else {
			inner.nodes.push(index);
		}
	}

	fn mark_complete(&self, object: &tg::object::Id) {
		let mut inner = self.inner.write().unwrap();
		let index = inner.indices.get(object).copied().unwrap();
		inner.nodes[index] = Self::COMPLETE;
	}

	fn is_complete(&self, object: &tg::object::Id) -> bool {
		let inner = self.inner.read().unwrap();
		let mut index = inner.indices.get(object).copied().unwrap();
		while inner.nodes[index] != index {
			if inner.nodes[index] == Self::COMPLETE {
				return true;
			}
			index = inner.nodes[index];
		}
		false
	}
}
