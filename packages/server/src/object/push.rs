use crate::Server;
use futures::{
	stream::FuturesUnordered, FutureExt as _, Stream, StreamExt as _, TryFutureExt as _,
	TryStreamExt as _,
};
use num::ToPrimitive as _;
use std::panic::AssertUnwindSafe;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::stream::Ext as _;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
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
		Self::push_or_pull_object(self, &remote, object).await
	}

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
