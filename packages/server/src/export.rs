use crate::Server;
use futures::{future, stream, FutureExt as _, Stream, StreamExt as _, TryStreamExt as _};
use num::ToPrimitive as _;
use std::{
	collections::BTreeMap,
	panic::AssertUnwindSafe,
	pin::{pin, Pin},
	sync::{Arc, RwLock},
};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{request::Ext, Body};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

#[derive(Clone)]
struct Graph<T> {
	inner: Arc<RwLock<GraphInner<T>>>,
}

struct GraphInner<T> {
	nodes: Vec<usize>,
	indices: BTreeMap<T, usize>,
}

pub(crate) struct InnerOutput {
	pub(crate) count: u64,
	pub(crate) depth: u64,
	pub(crate) weight: u64,
}

impl Server {
	pub async fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::export::Event>> + Send + 'static> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote {
			let client = self.get_remote_client(remote.clone()).await?;
			let arg = tg::export::Arg {
				remote: None,
				..arg
			};
			let stream = client.export(arg, stream).await?;
			return Ok(stream.left_stream());
		}

		let progress = crate::progress::Handle::new();
		progress.start(
			"objects".to_owned(),
			"objects".to_owned(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
		progress.start(
			"bytes".to_owned(),
			"bytes".to_owned(),
			tg::progress::IndicatorFormat::Bytes,
			Some(0),
			None,
		);
		let (item_sender, item_receiver) = tokio::sync::mpsc::channel(256);
		let task = tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			let items = arg.items;
			async move {
				let result =
					AssertUnwindSafe(server.export_inner(items, stream, &item_sender, &progress))
						.catch_unwind()
						.await;
				progress.finish("objects");
				progress.finish("bytes");
				match result {
					Ok(Ok(output)) => {
						progress.output(output);
					},
					Ok(Err(error)) => {
						progress.error(error);
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						progress.error(tg::error!(?message, "the task panicked"));
					},
				}
			}
		});
		let item_receiver_stream = ReceiverStream::new(item_receiver);
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = stream::select(
			item_receiver_stream.map_ok(tg::export::Event::Item),
			progress.stream().map_ok(tg::export::Event::Progress),
		)
		.attach(abort_handle);

		Ok(stream.right_stream())
	}

	pub async fn export_inner(
		&self,
		items: Vec<Either<tg::process::Id, tg::object::Id>>,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>>,
		item_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::export::Item>>,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// Create the object graph.
		let object_graph = Graph::new();

		// Create the item send future.
		let item_send_futures = future::try_join_all(items.into_iter().map(|item| {
			let server = self.clone();
			let object_graph = object_graph.clone();
			async move {
				match item {
					Either::Left(_) => todo!(),
					Either::Right(object) => {
						object_graph.insert(None, &object);
						let result = server
							.export_inner_object(&object, &object_graph, item_sender, progress)
							.boxed()
							.await;
						if let Err(error) = result {
							item_sender.send(Err(error)).await.ok();
						}
					},
				}
				Ok::<_, tg::Error>(())
			}
		}));

		// Create the import event future.
		let import_event_future = {
			let object_graph = object_graph.clone();
			async move {
				let mut stream = pin!(stream);
				while let Some(event) = stream.try_next().await? {
					match event {
						tg::import::Event::Complete(item) => match item {
							Either::Left(_) => todo!(),
							Either::Right(id) => {
								object_graph.mark_complete(&id);
							},
						},
					}
				}
				Ok::<_, tg::Error>(())
			}
		};

		futures::try_join!(item_send_futures, import_event_future)?;

		Ok(())
	}

	async fn export_inner_object(
		&self,
		object: &tg::object::Id,
		object_graph: &Graph<tg::object::Id>,
		item_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::export::Item>>,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<InnerOutput> {
		// Get the object.
		let tg::object::get::Output { bytes, metadata } = self
			.get_object(object)
			.await
			.map_err(|source| tg::error!(!source, %object, "failed to get the object"))?;
		let metadata = metadata.ok_or_else(|| tg::error!("expected the metadata to be set"))?;

		let data = tg::object::Data::deserialize(object.kind(), &bytes)?;
		let size = bytes.len().to_u64().unwrap();

		// Send the object if it is not complete.
		if !object_graph.is_complete(object) {
			let item = tg::export::Item::Object {
				id: object.clone(),
				bytes: bytes.clone(),
			};
			item_sender
				.send(Ok(item))
				.await
				.map_err(|source| tg::error!(!source, "failed to send"))?;
		}

		// Recurse into the incomplete children if not complete.
		let (incomplete_count, incomplete_depth, incomplete_weight) = if object_graph
			.is_complete(object)
		{
			(0, 0, 0)
		} else {
			let mut count = 0;
			let mut depth = 0;
			let mut weight = 0;

			for child in data.children() {
				object_graph.insert(Some(object), &child);
				let output =
					Box::pin(self.export_inner_object(&child, object_graph, item_sender, progress))
						.await?;
				count += output.count;
				depth = depth.max(1 + output.depth);
				weight += output.weight;
			}

			(count, depth, weight)
		};

		// Increment the count and add the object's size to the weight.
		progress.increment("objects", 1);
		progress.increment("bytes", size);

		// If the count is set, then add the count not yet added.
		if let Some(count) = metadata.count {
			progress.increment("objects", count - 1 - incomplete_count);
		}

		// If the weight is set, then add the weight not yet added.
		if let Some(weight) = metadata.weight {
			progress.increment("bytes", weight - size - incomplete_weight);
		}

		// Compute the count and weight from this call.
		let count = metadata.count.unwrap_or_else(|| 1 + incomplete_count);
		let depth = metadata.depth.unwrap_or_else(|| 1 + incomplete_depth);
		let weight = metadata.weight.unwrap_or_else(|| size + incomplete_weight);

		Ok(InnerOutput {
			count,
			depth,
			weight,
		})
	}
}

impl Server {
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

impl<T> Graph<T>
where
	T: Clone + Eq + Ord,
{
	const COMPLETE: usize = usize::MAX;

	fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(GraphInner {
				nodes: Vec::new(),
				indices: BTreeMap::new(),
			})),
		}
	}

	fn insert(&self, parent: Option<&T>, child: &T) {
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

	fn mark_complete(&self, object: &T) {
		let mut inner = self.inner.write().unwrap();
		let index = inner.indices.get(object).copied().unwrap();
		inner.nodes[index] = Self::COMPLETE;
	}

	fn is_complete(&self, object: &T) -> bool {
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
