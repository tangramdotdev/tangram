use crate::Server;
use futures::{
	stream::FuturesUnordered, FutureExt as _, Stream, StreamExt as _, TryFutureExt as _,
	TryStreamExt as _,
};
use num::ToPrimitive as _;
use std::{
	collections::BTreeMap,
	panic::AssertUnwindSafe,
	pin::pin,
	sync::{Arc, RwLock},
};
use tangram_client::Handle;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::stream::Ext as _;
use tangram_http::{request::Ext as _, Body};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

pub(crate) struct InnerOutput {
	pub(crate) count: u64,
	pub(crate) depth: u64,
	pub(crate) weight: u64,
}

impl Server {
	pub async fn push_object(
		&self,
		object: &tg::object::Id,
		arg: tg::object::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let remote = self.get_remote_client(arg.remote.clone()).await?;
		self.push_object_inner(object, remote).await
	}

	async fn push_object_inner(
		&self,
		root: &tg::object::Id,
		remote: tg::Client,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let metadata = self.try_get_object_metadata(root).await?;
		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let progress = progress.clone();
			let server = self.clone();
			let root = root.clone();
			async move {
				progress.start(
					"objects".to_owned(),
					"objects".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					metadata.as_ref().and_then(|metadata| metadata.count),
				);
				progress.start(
					"bytes".to_owned(),
					"bytes".to_owned(),
					tg::progress::IndicatorFormat::Bytes,
					Some(0),
					metadata.as_ref().and_then(|metadata| metadata.weight),
				);
				let result = AssertUnwindSafe(
					server.push_object_with_post_objects_stream(root, remote, &progress),
				)
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
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = progress.stream().attach(abort_handle);
		Ok(stream)
	}

	pub async fn push_object_with_post_objects_stream(
		&self,
		root: tg::object::Id,
		remote: tg::Client,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// Create the complete tree.
		let complete_tree = CompleteTree::new();

		// Create the channels.
		let limit = 128;
		let (mut send_item, recv_item) = tokio::sync::mpsc::channel(limit);

		// Create a task to send items.
		let send_task = tokio::spawn({
			let server = self.clone();
			let complete_tree = complete_tree.clone();
			let object = root.clone();
			let progress = progress.clone();
			async move {
				// Initialize the complete tree.
				complete_tree.insert(None, &object);

				// Push the object from the root.
				let result = Box::pin(server.push_object_with_push_objects_stream_inner(
					&object,
					&complete_tree,
					&mut send_item,
					&progress,
				))
				.await;

				// Send an error if necessary.
				if let Err(error) = result {
					send_item.send(Err(error)).await.ok();
				}
			}
		});

		// Create the stream.
		let stream = remote
			.post_objects(ReceiverStream::new(recv_item).boxed())
			.await?;

		// Create a task to receive ack messages.
		let recv_task = tokio::spawn(async move {
			// Drain the stream.
			let mut stream = pin!(stream);
			while let Some(event) = stream.try_next().await? {
				// Break if necessary.
				if matches!(event, tg::object::post::Event::End) {
					break;
				}

				// Mark the object as complete if necessary.
				if let tg::object::post::Event::Complete(object) = event {
					complete_tree.mark_complete(&object);
				}
			}
			Ok::<_, tg::Error>(())
		});

		futures::try_join!(send_task, recv_task).unwrap().1
	}

	async fn push_object_with_push_objects_stream_inner(
		&self,
		object: &tg::object::Id,
		complete_tree: &CompleteTree,
		send: &mut tokio::sync::mpsc::Sender<tg::Result<tg::object::post::Item>>,
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

		// Send the object if not complete.
		if !complete_tree.is_complete(object) {
			let item = tg::object::post::Item {
				id: object.clone(),
				bytes: bytes.clone(),
			};
			send.send(Ok(item))
				.await
				.map_err(|source| tg::error!(!source, "failed to send"))?;
		}

		// Recurse into the incomplete children if not complete.
		let (incomplete_count, incomplete_depth, incomplete_weight) =
			if complete_tree.is_complete(object) {
				(0, 0, 0)
			} else {
				let mut count = 0;
				let mut depth = 0;
				let mut weight = 0;

				for child in data.children() {
					complete_tree.insert(Some(object), &child);
					let output = Box::pin(self.push_object_with_push_objects_stream_inner(
						&child,
						complete_tree,
						send,
						progress,
					))
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
	pub(crate) async fn push_or_pull_object<S, D>(
		src: &S,
		dst: &D,
		object: &tg::object::Id,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static + use<S, D>,
	>
	where
		S: tg::Handle,
		D: tg::Handle,
	{
		let metadata = src.try_get_object_metadata(object).await?;
		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let src = src.clone();
			let dst = dst.clone();
			let object = object.clone();
			let progress = progress.clone();
			async move {
				progress.start(
					"objects".to_owned(),
					"objects".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					metadata.as_ref().and_then(|metadata| metadata.count),
				);
				progress.start(
					"bytes".to_owned(),
					"bytes".to_owned(),
					tg::progress::IndicatorFormat::Bytes,
					Some(0),
					metadata.as_ref().and_then(|metadata| metadata.weight),
				);
				let result = AssertUnwindSafe(
					Self::push_or_pull_object_inner(&src, &dst, &object, &progress).map_ok(|_| ()),
				)
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
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = progress.stream().attach(abort_handle);
		Ok(stream)
	}

	pub(crate) async fn push_or_pull_object_inner(
		src: &impl tg::Handle,
		dst: &impl tg::Handle,
		object: &tg::object::Id,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<InnerOutput> {
		// Get the object.
		let tg::object::get::Output { bytes, metadata } = src
			.get_object(object)
			.await
			.map_err(|source| tg::error!(!source, %object, "failed to get the object"))?;
		let metadata = metadata.ok_or_else(|| tg::error!("expected the metadata to be set"))?;
		let data = tg::object::Data::deserialize(object.kind(), &bytes)?;
		let size = bytes.len().to_u64().unwrap();

		// Put the object.
		let arg = tg::object::put::Arg { bytes };
		let output = dst
			.put_object(object, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;

		// Increment the count and add the object's size to the weight.
		progress.increment("objects", 1);
		progress.increment("bytes", size);

		// Recurse into the incomplete children.
		let (incomplete_count, incomplete_depth, incomplete_weight) = if output.complete {
			(0, 0, 0)
		} else {
			data.children()
				.into_iter()
				.map(|object| async move {
					Self::push_or_pull_object_inner(src, dst, &object, progress).await
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?
				.into_iter()
				.fold((0, 0, 0), |(count, depth, weight), output| {
					(
						count + output.count,
						depth.max(1 + output.depth),
						weight + output.weight,
					)
				})
		};

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
	pub(crate) async fn handle_push_object_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.push_object(&id, arg).await?;

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

struct CompleteTree {
	inner: Arc<RwLock<CompleteTreeInner>>,
}

struct CompleteTreeInner {
	nodes: Vec<usize>,
	indices: BTreeMap<tg::object::Id, usize>,
}

impl Clone for CompleteTree {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

impl CompleteTree {
	const COMPLETE: usize = usize::MAX;

	fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(CompleteTreeInner {
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
