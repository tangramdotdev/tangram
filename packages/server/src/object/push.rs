use crate::{util, Server};
use futures::{stream::FuturesUnordered, Stream, TryStreamExt as _};
use num::ToPrimitive as _;
use std::sync::{atomic::AtomicU64, Arc};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};

struct State {
	count: ProgressState,
	weight: ProgressState,
}

struct ProgressState {
	current: AtomicU64,
	total: Option<AtomicU64>,
}

struct InnerOutput {
	count: u64,
	weight: u64,
}

impl Server {
	pub async fn push_object(
		&self,
		object: &tg::object::Id,
		arg: tg::object::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static> {
		let remote = self
			.remotes
			.get(&arg.remote)
			.ok_or_else(|| tg::error!("failed to find the remote"))?
			.clone();
		Self::push_or_pull_object(self, &remote, object).await
	}

	pub(crate) async fn push_or_pull_object(
		src: &impl tg::Handle,
		dst: &impl tg::Handle,
		object: &tg::object::Id,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::Progress<()>>> + Send + 'static> {
		// Get the metadata.
		let metadata = src.get_object_metadata(object).await?;

		// Create the state.
		let count = ProgressState {
			current: AtomicU64::new(0),
			total: metadata.count.map(AtomicU64::new),
		};
		let weight = ProgressState {
			current: AtomicU64::new(0),
			total: metadata.weight.map(AtomicU64::new),
		};
		let state = Arc::new(State { count, weight });

		let stream = tg::progress::progress_stream({
			let src = src.clone();
			let dst = dst.clone();
			let object = object.clone();
			let state = state.clone();
			|_state| async move {
				Self::push_or_pull_object_inner(&src, &dst, &object, &state).await?;
				Ok(())
			}
		});

		Ok(stream)
	}

	async fn push_or_pull_object_inner(
		src: &impl tg::Handle,
		dst: &impl tg::Handle,
		object: &tg::object::Id,
		state: &State,
	) -> tg::Result<InnerOutput> {
		// Get the object.
		let tg::object::get::Output { bytes, metadata } = src
			.get_object(object)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the object"))?;
		let size = bytes.len().to_u64().unwrap();

		// Put the object.
		let arg = tg::object::put::Arg { bytes };
		let output = dst
			.put_object(object, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;

		// Increment the count and add the object's size to the weight.
		state
			.count
			.current
			.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		state
			.weight
			.current
			.fetch_add(size, std::sync::atomic::Ordering::Relaxed);

		// Recurse into the incomplete children.
		let (incomplete_count, incomplete_weight) = output
			.incomplete
			.into_iter()
			.map(
				|object| async move { Self::push_or_pull_object_inner(src, dst, &object, state).await },
			)
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.fold((0, 0), |(count, weight), output| {
				(count + output.count, weight + output.weight)
			});

		// If the count is set, then add the count not yet added.
		if let Some(count) = metadata.count {
			state.count.current.fetch_add(
				count - 1 - incomplete_count,
				std::sync::atomic::Ordering::Relaxed,
			);
		}

		// If the weight is set, then add the weight not yet added.
		if let Some(weight) = metadata.weight {
			state.weight.current.fetch_add(
				weight - size - incomplete_weight,
				std::sync::atomic::Ordering::Relaxed,
			);
		}

		// Compute the count and weight from this call.
		let count = metadata.count.unwrap_or_else(|| 1 + incomplete_count);
		let weight = metadata.weight.unwrap_or_else(|| size + incomplete_weight);

		Ok(InnerOutput { count, weight })
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
		let id = id.parse()?;
		let arg = request.json().await?;
		let stream = handle.push_object(&id, arg).await?;
		Ok(util::progress::sse(stream))
	}
}
