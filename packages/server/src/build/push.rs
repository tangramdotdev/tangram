use crate::Server;
use futures::{
	stream::{self, FuturesUnordered},
	FutureExt as _, Stream, StreamExt as _, TryStreamExt,
};
use std::sync::{atomic::AtomicU64, Arc};
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tokio_stream::wrappers::IntervalStream;

struct State {
	build_count: ProgressState,
	object_count: ProgressState,
	object_weight: ProgressState,
}

struct ProgressState {
	current: AtomicU64,
	total: Option<AtomicU64>,
}

struct InnerOutput {
	build_count: u64,
	object_count: u64,
	object_weight: u64,
}

struct Metadata {
	build_count: Option<u64>,
	object_count: Option<u64>,
	object_weight: Option<u64>,
}

impl Server {
	pub async fn push_build(
		&self,
		build: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::build::push::Event>> + Send + 'static> {
		let remote = self
			.remotes
			.get(&arg.remote)
			.ok_or_else(|| tg::error!("failed to find the remote"))?
			.clone();
		Self::push_or_pull_build(self, &remote, build, arg).await
	}

	pub(crate) async fn push_or_pull_build(
		src: &impl tg::Handle,
		dst: &impl tg::Handle,
		build: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::build::push::Event>> + Send + 'static> {
		// Get the build.
		let output = src.get_build(build).await?;

		// Get the metadata.
		let metadata = Self::get_build_metadata(src, &output, &arg).await?;

		// Create the state.
		let build_count_progress_state = ProgressState {
			current: AtomicU64::new(0),
			total: metadata.build_count.map(AtomicU64::new),
		};
		let object_count_progress_state = ProgressState {
			current: AtomicU64::new(0),
			total: metadata.object_count.map(AtomicU64::new),
		};
		let object_weight_progress_state = ProgressState {
			current: AtomicU64::new(0),
			total: metadata.object_weight.map(AtomicU64::new),
		};
		let state = Arc::new(State {
			build_count: build_count_progress_state,
			object_count: object_count_progress_state,
			object_weight: object_weight_progress_state,
		});

		// Spawn the task.
		let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
		tokio::spawn({
			let src = src.clone();
			let dst = dst.clone();
			let build = build.clone();
			let state = state.clone();
			async move {
				let result = Self::push_or_pull_build_inner(&src, &dst, &build, arg, &state)
					.await
					.map(|_| ());
				result_sender.send(result).ok();
			}
		});

		// Create the stream.
		let interval = std::time::Duration::from_millis(100);
		let interval = tokio::time::interval(interval);
		let result = result_receiver.map(Result::unwrap).shared();
		let stream = IntervalStream::new(interval)
			.map(move |_| {
				let current = state
					.build_count
					.current
					.load(std::sync::atomic::Ordering::Relaxed);
				let total = state
					.build_count
					.total
					.as_ref()
					.map(|total| total.load(std::sync::atomic::Ordering::Relaxed));
				let build_count = tg::Progress { current, total };
				let current = state
					.object_count
					.current
					.load(std::sync::atomic::Ordering::Relaxed);
				let total = state
					.object_count
					.total
					.as_ref()
					.map(|total| total.load(std::sync::atomic::Ordering::Relaxed));
				let object_count = tg::Progress { current, total };
				let current = state
					.object_weight
					.current
					.load(std::sync::atomic::Ordering::Relaxed);
				let total = state
					.object_weight
					.total
					.as_ref()
					.map(|total| total.load(std::sync::atomic::Ordering::Relaxed));
				let object_weight = tg::Progress { current, total };
				let progress = tg::build::push::Progress {
					build_count,
					object_count,
					object_weight,
				};
				Ok(tg::build::push::Event::Progress(progress))
			})
			.take_until(result.clone())
			.chain(stream::once(result.map(|result| match result {
				Ok(()) => Ok(tg::build::push::Event::End),
				Err(error) => Err(error),
			})));

		Ok(stream)
	}

	async fn push_or_pull_build_inner(
		src: &impl tg::Handle,
		dst: &impl tg::Handle,
		build: &tg::build::Id,
		arg: tg::build::push::Arg,
		state: &State,
	) -> tg::Result<InnerOutput> {
		// Get the build.
		let output = src
			.get_build(build)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the build"))?;

		// Get the metadata.
		let metadata = Self::get_build_metadata(src, &output, &arg).await?;

		// Get the children.
		let children_arg = tg::build::children::get::Arg::default();
		let children = src
			.get_build_children(build, children_arg)
			.await?
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.flat_map(|chunk| chunk.data)
			.collect();

		// Put the object.
		let put_arg = tg::build::put::Arg {
			id: build.clone(),
			children,
			host: output.host,
			log: output.log.clone(),
			outcome: output.outcome,
			retry: output.retry,
			status: output.status,
			target: output.target.clone(),
			created_at: output.created_at,
			dequeued_at: output.dequeued_at,
			started_at: output.started_at,
			finished_at: output.finished_at,
		};
		let tg::build::put::Output { incomplete } = dst
			.put_build(build, put_arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;

		// Update the state.
		state
			.build_count
			.current
			.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

		// Handle the incomplete objects.
		let mut incomplete_objects: Vec<tg::object::Id> = Vec::new();
		if arg.logs && incomplete.log {
			if let Some(log) = output.log.clone() {
				incomplete_objects.push(log.clone().into());
			}
		}
		if arg.outcomes {
			incomplete_objects.extend(incomplete.outcome);
		}
		if arg.targets && incomplete.target {
			incomplete_objects.push(output.target.clone().into());
		}
		let incomplete_object_count_and_weight = incomplete_objects
			.iter()
			.map(|object| async {
				let mut stream = Self::push_or_pull_object(src, dst, object).await?.boxed();
				let mut count = 0;
				let mut weight = 0;
				while let Some(tg::object::push::Event::Progress(event)) = stream.try_next().await?
				{
					count = event.count.current;
					weight = event.weight.current;
					state
						.object_count
						.current
						.store(count, std::sync::atomic::Ordering::Relaxed);
					state
						.object_weight
						.current
						.store(weight, std::sync::atomic::Ordering::Relaxed);
				}
				Ok::<_, tg::Error>((count, weight))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		let incomplete_object_count = incomplete_object_count_and_weight
			.iter()
			.map(|(count, _)| count)
			.sum::<u64>();
		let incomplete_object_weight = incomplete_object_count_and_weight
			.iter()
			.map(|(_, weight)| weight)
			.sum::<u64>();

		// Handle the incomplete children.
		let InnerOutput {
			build_count: incomplete_children_build_count,
			object_count: incomplete_children_object_count,
			object_weight: incomplete_children_object_weight,
		} = if arg.recursive {
			let outputs = incomplete
				.children
				.keys()
				.map(|child| Self::push_or_pull_build_inner(src, dst, child, arg.clone(), state))
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?;
			outputs.into_iter().fold(
				InnerOutput {
					build_count: 0,
					object_count: 0,
					object_weight: 0,
				},
				|a, b| InnerOutput {
					build_count: a.build_count + b.build_count,
					object_count: a.object_count + b.object_count,
					object_weight: a.object_weight + b.object_weight,
				},
			)
		} else {
			InnerOutput {
				build_count: 0,
				object_count: 0,
				object_weight: 0,
			}
		};

		// Update the build count.
		let build_count = metadata.build_count.map_or_else(
			|| 1 + incomplete_children_build_count,
			|build_count| build_count - 1 - incomplete_children_build_count,
		);
		state
			.build_count
			.current
			.fetch_add(build_count, std::sync::atomic::Ordering::Relaxed);

		// Update the object count.
		let object_count = metadata.object_count.map_or_else(
			|| incomplete_object_count + incomplete_children_object_count,
			|object_count| {
				object_count - incomplete_object_count - incomplete_children_object_count
			},
		);
		state
			.object_count
			.current
			.fetch_add(object_count, std::sync::atomic::Ordering::Relaxed);

		// Update the object count.
		let object_weight = metadata.object_weight.map_or_else(
			|| incomplete_object_weight + incomplete_children_object_weight,
			|object_weight| {
				object_weight - incomplete_object_weight - incomplete_children_object_weight
			},
		);
		state
			.object_weight
			.current
			.fetch_add(object_weight, std::sync::atomic::Ordering::Relaxed);

		Ok(InnerOutput {
			build_count,
			object_count,
			object_weight,
		})
	}

	async fn get_build_metadata(
		src: &impl tg::Handle,
		output: &tg::build::get::Output,
		arg: &tg::build::push::Arg,
	) -> tg::Result<Metadata> {
		let build_count = if arg.recursive { output.count } else { Some(1) };
		let (object_count, object_weight) = if arg.recursive {
			// If the push is recursive, then use the logs', outcomes', and targets' counts and weights.
			let logs_count = if arg.logs { output.logs_count } else { Some(0) };
			let outcomes_count = if arg.outcomes {
				output.outcomes_count
			} else {
				Some(0)
			};
			let targets_count = if arg.targets {
				output.targets_count
			} else {
				Some(0)
			};
			let count = std::iter::empty()
				.chain(Some(logs_count))
				.chain(Some(outcomes_count))
				.chain(Some(targets_count))
				.sum::<Option<u64>>();
			let logs_weight = if arg.logs {
				output.logs_weight
			} else {
				Some(0)
			};
			let outcomes_weight = if arg.outcomes {
				output.outcomes_weight
			} else {
				Some(0)
			};
			let targets_weight = if arg.targets {
				output.targets_weight
			} else {
				Some(0)
			};
			let weight = std::iter::empty()
				.chain(Some(logs_weight))
				.chain(Some(outcomes_weight))
				.chain(Some(targets_weight))
				.sum::<Option<u64>>();
			(count, weight)
		} else {
			// If the push is not recursive, then use the count and weight of the log, outcome, and target.
			let (log_count, log_weight) = if arg.logs {
				if let Some(log) = output.log.as_ref() {
					if let Some(metadata) = src.try_get_object_metadata(&log.clone().into()).await?
					{
						(metadata.count, metadata.weight)
					} else {
						(Some(0), Some(0))
					}
				} else {
					(Some(0), Some(0))
				}
			} else {
				(Some(0), Some(0))
			};
			let (outcome_count, outcome_weight) = if arg.outcomes {
				if let Some(tg::build::outcome::Data::Succeeded(output)) = output.outcome.as_ref() {
					let metadata = output
						.children()
						.iter()
						.map(|child| src.try_get_object_metadata(child))
						.collect::<FuturesUnordered<_>>()
						.try_collect::<Vec<_>>()
						.await?;
					let count = metadata
						.iter()
						.map(|metadata| metadata.as_ref().and_then(|metadata| metadata.count))
						.sum::<Option<u64>>();
					let weight = metadata
						.iter()
						.map(|metadata| metadata.as_ref().and_then(|metadata| metadata.weight))
						.sum::<Option<u64>>();
					(count, weight)
				} else {
					(Some(0), Some(0))
				}
			} else {
				(Some(0), Some(0))
			};
			let (target_count, target_weight) = {
				if let Some(metadata) = src
					.try_get_object_metadata(&output.target.clone().into())
					.await?
				{
					(metadata.count, metadata.weight)
				} else {
					(Some(0), Some(0))
				}
			};
			let count = std::iter::empty()
				.chain(Some(log_count))
				.chain(Some(outcome_count))
				.chain(Some(target_count))
				.sum::<Option<u64>>();
			let weight = std::iter::empty()
				.chain(Some(log_weight))
				.chain(Some(outcome_weight))
				.chain(Some(target_weight))
				.sum::<Option<u64>>();
			(count, weight)
		};
		Ok(Metadata {
			build_count,
			object_count,
			object_weight,
		})
	}
}

impl Server {
	pub(crate) async fn handle_push_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let stream = handle.push_build(&id, arg).await?;
		let sse = stream.map(|result| match result {
			Ok(tg::build::push::Event::Progress(progress)) => {
				let data = serde_json::to_string(&progress).unwrap();
				let event = tangram_http::sse::Event {
					data,
					..Default::default()
				};
				Ok::<_, tg::Error>(event)
			},
			Ok(tg::build::push::Event::End) => {
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
