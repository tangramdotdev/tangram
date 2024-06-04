use crate::Server;
use futures::{
	stream::{self, FuturesUnordered},
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use num::ToPrimitive as _;
use std::sync::{atomic::AtomicU64, Arc};
use tangram_client::{self as tg, Handle as _};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tokio_stream::wrappers::IntervalStream;

struct State {
	objects: ProgressState,
	bytes: ProgressState,
}

struct ProgressState {
	current: AtomicU64,
	total: Option<AtomicU64>,
}

impl Server {
	pub async fn push_object(
		&self,
		object: &tg::object::Id,
		arg: tg::object::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::push::Event>> + Send + 'static> {
		// Get the remote.
		let remote = self
			.remotes
			.get(&arg.remote)
			.ok_or_else(|| tg::error!("failed to find the remote"))?
			.clone();

		// Get the metadata.
		let metadata = self.get_object_metadata(object).await?;

		// Create the state.
		let state = Arc::new(State {
			objects: ProgressState {
				current: AtomicU64::new(0),
				total: metadata.count.map(AtomicU64::new),
			},
			bytes: ProgressState {
				current: AtomicU64::new(0),
				total: metadata.weight.map(AtomicU64::new),
			},
		});

		// Spawn the task.
		let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
		tokio::spawn({
			let server = self.clone();
			let object = object.clone();
			let state = state.clone();
			async move {
				let result = server
					.push_object_inner(&object, &remote, &state)
					.await
					.map(|_| ());
				result_sender.send(result).unwrap();
			}
		});

		// Create the stream.
		let interval = std::time::Duration::from_millis(100);
		let interval = tokio::time::interval(interval);
		let result = result_receiver.map(Result::unwrap).shared();
		let stream = IntervalStream::new(interval)
			.map(move |_| {
				let current = state
					.objects
					.current
					.load(std::sync::atomic::Ordering::Relaxed);
				let total = state
					.objects
					.total
					.as_ref()
					.map(|total| total.load(std::sync::atomic::Ordering::Relaxed));
				let objects = tg::Progress { current, total };
				let current = state
					.bytes
					.current
					.load(std::sync::atomic::Ordering::Relaxed);
				let total = state
					.bytes
					.total
					.as_ref()
					.map(|total| total.load(std::sync::atomic::Ordering::Relaxed));
				let bytes = tg::Progress { current, total };
				let progress = tg::object::push::Progress { objects, bytes };
				Ok(tg::object::push::Event::Progress(progress))
			})
			.take_until(result.clone())
			.chain(stream::once(result.map(|result| match result {
				Ok(_) => Ok(tg::object::push::Event::End),
				Err(error) => Err(error),
			})));

		Ok(stream)
	}

	async fn push_object_inner(
		&self,
		object: &tg::object::Id,
		remote: &tg::Client,
		state: &State,
	) -> tg::Result<(u64, u64)> {
		// Get the object.
		let tg::object::get::Output { bytes, metadata } = self
			.get_object(object)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the object"))?;
		let size = bytes.len().to_u64().unwrap();

		// Put the object.
		let arg = tg::object::put::Arg { bytes };
		let output = remote
			.put_object(object, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;

		// Increment the count and add the object's size to the weight.
		state
			.objects
			.current
			.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		state
			.bytes
			.current
			.fetch_add(size, std::sync::atomic::Ordering::Relaxed);

		// Recurse into the incomplete children.
		let (incomplete_count, incomplete_weight) = output
			.incomplete
			.into_iter()
			.map(|object| async move { self.push_object_inner(&object, remote, state).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.fold((0, 0), |(count, weight), (c, w)| (count + c, weight + w));

		// If the count is set, then add the count not yet added.
		if let Some(count) = metadata.count {
			state.objects.current.fetch_add(
				count - 1 - incomplete_count,
				std::sync::atomic::Ordering::Relaxed,
			);
		}

		// If the weight is set, then add the weight not yet added.
		if let Some(weight) = metadata.weight {
			state.bytes.current.fetch_add(
				weight - size - incomplete_weight,
				std::sync::atomic::Ordering::Relaxed,
			);
		}

		// Compute the count and weight that this call added.
		let count = metadata.count.unwrap_or_else(|| 1 + incomplete_count);
		let weight = metadata.weight.unwrap_or_else(|| size + incomplete_weight);

		Ok((count, weight))
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
		let sse = stream.map(|result| match result {
			Ok(tg::object::push::Event::Progress(progress)) => {
				let data = serde_json::to_string(&progress).unwrap();
				let event = tangram_http::sse::Event {
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Ok(tg::object::push::Event::End) => {
				let event = "end".to_owned();
				let event = tangram_http::sse::Event {
					event: Some(event),
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Err(error) => {
				let data = serde_json::to_string(&error).unwrap();
				let event = "error".to_owned();
				let event = tangram_http::sse::Event {
					data,
					event: Some(event),
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
		});
		let body = Outgoing::sse(sse);
		let response = http::Response::builder().ok().body(body).unwrap();
		Ok(response)
	}
}
