use crate::Server;
use futures::{
	stream::FuturesUnordered, FutureExt as _, Stream, StreamExt as _, TryFutureExt as _,
	TryStreamExt as _,
};
use num::ToPrimitive as _;
use std::panic::AssertUnwindSafe;
use std::pin::pin;
use tangram_client::Handle;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::stream::Ext as _;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
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
				let result =
					AssertUnwindSafe(server.push_object_with_post_objects_stream(root, remote, &progress))
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
				};
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
		// Create the channels.
		let limit = 128;
		let (send_event, mut recv_event) = tokio::sync::mpsc::channel(limit);
		let (mut send_item, recv_item) = tokio::sync::mpsc::channel(limit);

		// Create a task to send items.
		let send_task = tokio::spawn({
			let server = self.clone();
			let object = root.clone();
			let progress = progress.clone();
			async move {
				let result = Box::pin(server.push_object_with_push_objects_stream_inner(
					&object,
					&mut send_item,
					&mut recv_event,
					&progress,
				))
				.await;
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
			while let Some(event) = stream
				.try_next()
				.await?
			{
				if matches!(event, tg::object::post::Event::End) {
					break;
				}
				if send_event.send(Ok(event)).await.is_err() {
					break;
				}
			}
			Ok::<_, tg::Error>(())
		});

		futures::try_join!(send_task, recv_task).unwrap().1
	}

	async fn push_object_with_push_objects_stream_inner(
		&self,
		object: &tg::object::Id,
		send: &mut tokio::sync::mpsc::Sender<tg::Result<tg::object::post::Item>>,
		recv: &mut tokio::sync::mpsc::Receiver<tg::Result<tg::object::post::Event>>,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<InnerOutput> {
		// Get the object.
		let tg::object::get::Output { bytes, metadata } = self
			.get_object(object)
			.await
			.map_err(|source| tg::error!(!source, %object, "failed to get the object"))?;
		let data = tg::object::Data::deserialize(object.kind(), &bytes)?;
		let size = bytes.len().to_u64().unwrap();

		// Send the object.
		let item = tg::object::post::Item {
			id: object.clone(),
			bytes: bytes.clone(),
		};
		send.send(Ok(item))
			.await
			.map_err(|source| tg::error!(!source, "failed to send"))?;

		// Wait for ack.
		let ack = recv
			.recv()
			.await
			.ok_or_else(|| tg::error!("expected an ack"))?
			.map_err(|source| tg::error!(!source, "post stream error"))?;
		let complete = match ack {
			tg::object::post::Event::Complete(id) if &id == object => true,
			tg::object::post::Event::Incomplete(id) if &id == object => false,
			tg::object::post::Event::End => return Err(tg::error!("unexpected end of stream")),
			tg::object::post::Event::Complete(id) | tg::object::post::Event::Incomplete(id) => {
				return Err(tg::error!(%expected = object, %received = id, "unexpected id"))
			},
		};

		// Increment the count and add the object's size to the weight.
		progress.increment("objects", 1);
		progress.increment("bytes", size);

		// Recurse into the incomplete children.
		let (incomplete_count, incomplete_depth, incomplete_weight) = if complete {
			(0, 0, 0)
		} else {
			let mut incomplete_count = 0;
			let mut incomplete_depth = 0;
			let mut incomplete_weight = 0;
			for object in data.children() {
				let result =
					Box::pin(self.push_object_with_push_objects_stream_inner(&object, send, recv, progress))
						.await;
				match result {
					Ok(output) => {
						incomplete_count += output.count;
						incomplete_depth = incomplete_depth.max(1 + output.depth);
						incomplete_weight += output.weight;
					},
					Err(error) => {
						send.send(Err(error)).await.ok();
					},
				}
			}
			(incomplete_count, incomplete_depth, incomplete_weight)
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
				};
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
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
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
