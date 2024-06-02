use crate::Server;
use futures::{
	future,
	stream::{self, FuturesUnordered},
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use num::ToPrimitive as _;
use std::sync::{atomic::AtomicU64, Arc, Mutex};
use tangram_client::{self as tg, Handle as _};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tokio_stream::wrappers::IntervalStream;

impl Server {
	pub async fn pull_object(
		&self,
		object: &tg::object::Id,
		arg: tg::object::pull::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::pull::Event>> + Send + 'static> {
		// Get the remote.
		let remote = self
			.remotes
			.get(&arg.remote)
			.ok_or_else(|| tg::error!("failed to find the remote"))?
			.clone();

		// Get the metadata.
		let metadata = self.get_object_metadata(object).await?;
		let total_count = metadata.count;
		let total_weight = metadata.weight;

		// Create the state.
		let current_count = Arc::new(AtomicU64::new(0));
		let current_weight = Arc::new(AtomicU64::new(0));
		let result = Arc::new(Mutex::new(None));

		// Spawn the task.
		tokio::spawn({
			let server = self.clone();
			let object = object.clone();
			let current_count = current_count.clone();
			let current_weight = current_weight.clone();
			let result = result.clone();
			async move {
				let result_ = server
					.pull_object_inner(&object, &remote, current_count, current_weight)
					.await
					.map(|_| ());
				result.lock().unwrap().replace(result_);
			}
		});

		// Create the stream.
		let interval = std::time::Duration::from_millis(100);
		let interval = tokio::time::interval(interval);
		let interval = IntervalStream::new(interval);
		struct State {
			interval: IntervalStream,
			current_count: Arc<AtomicU64>,
			current_weight: Arc<AtomicU64>,
			result: Arc<Mutex<Option<tg::Result<()>>>>,
		}
		let state = State {
			interval,
			current_count,
			current_weight,
			result: result.clone(),
		};
		let stream = stream::try_unfold(state, move |mut state| async move {
			let result = state.result.lock().unwrap().take();
			if let Some(result) = result {
				match result {
					Ok(()) => {
						return Ok(None);
					},
					Err(error) => {
						return Err(error);
					},
				}
			}
			state.interval.next().await;
			let current_count = state
				.current_count
				.load(std::sync::atomic::Ordering::Relaxed);
			let current_weight = state
				.current_weight
				.load(std::sync::atomic::Ordering::Relaxed);
			let progress = tg::object::push::Progress {
				current_count,
				total_count,
				current_weight,
				total_weight,
			};
			let event = tg::object::push::Event::Progress(progress);
			Ok(Some((event, state)))
		})
		.chain(stream::once(future::ok(tg::object::push::Event::End)));

		Ok(stream)
	}

	async fn pull_object_inner(
		&self,
		object: &tg::object::Id,
		remote: &tg::Client,
		current_count: Arc<AtomicU64>,
		current_weight: Arc<AtomicU64>,
	) -> tg::Result<(u64, u64)> {
		// Get the object.
		let tg::object::get::Output { bytes, metadata } = remote
			.get_object(object)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the object"))?;
		let size = bytes.len().to_u64().unwrap();

		// Put the object.
		let arg = tg::object::put::Arg { bytes };
		let output = self
			.put_object(object, arg)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;

		// Increment the count and add the objects size to the weight.
		current_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		current_weight.fetch_add(size, std::sync::atomic::Ordering::Relaxed);

		// Recurse into the incomplete children.
		let (incomplete_count, incomplete_weight) = output
			.incomplete
			.into_iter()
			.map(|object| {
				let count = current_count.clone();
				let weight = current_weight.clone();
				async move { self.pull_object_inner(&object, remote, count, weight).await }
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.fold((0, 0), |(count, weight), (c, w)| (count + c, weight + w));

		// If the count is set, then add the count not yet added.
		if let Some(count) = metadata.count {
			current_count.fetch_add(
				count - 1 - incomplete_count,
				std::sync::atomic::Ordering::Relaxed,
			);
		}

		// If the weight is set, then add the weight not yet added.
		if let Some(weight) = metadata.weight {
			current_weight.fetch_add(
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
	pub(crate) async fn handle_pull_object_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let stream = handle.pull_object(&id, arg).await?;
		let sse = stream.map(|result| match result {
			Ok(tg::object::pull::Event::Progress(progress)) => {
				let data = serde_json::to_string(&progress).unwrap();
				let event = tangram_http::sse::Event {
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Ok(tg::object::pull::Event::End) => {
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
