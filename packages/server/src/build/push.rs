use crate::Server;
use futures::{
	stream::FuturesUnordered, FutureExt as _, Stream, StreamExt as _, TryFutureExt as _,
	TryStreamExt,
};
use std::panic::AssertUnwindSafe;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::stream::Ext as _;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
use tokio_util::task::AbortOnDropHandle;

struct InnerOutput {
	build_count: u64,
	object_count: u64,
	object_weight: u64,
}

struct Metadata {
	build_count: Option<u64>,
	object_count: Option<u64>,
	#[allow(dead_code)]
	object_depth: Option<u64>,
	object_weight: Option<u64>,
}

impl Server {
	pub async fn push_build(
		&self,
		build: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let client = self.get_remote_client(arg.remote.clone()).await?;
		Self::push_or_pull_build(self, &client, build, arg).await
	}

	pub(crate) async fn push_or_pull_build<S, D>(
		src: &S,
		dst: &D,
		build: &tg::build::Id,
		arg: tg::build::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static + use<S, D>,
	>
	where
		S: tg::Handle,
		D: tg::Handle,
	{
		let output = src.get_build(build).await?;
		let metadata = Self::get_build_metadata(src, &output, &arg).await?;
		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let src = src.clone();
			let dst = dst.clone();
			let build = build.clone();
			let progress = progress.clone();
			async move {
				progress.start(
					"builds".to_owned(),
					"builds".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					metadata.build_count,
				);
				progress.start(
					"objects".to_owned(),
					"objects".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					metadata.object_count,
				);
				progress.start(
					"bytes".to_owned(),
					"bytes".to_owned(),
					tg::progress::IndicatorFormat::Bytes,
					Some(0),
					metadata.object_weight,
				);
				let result = AssertUnwindSafe(
					Self::push_or_pull_build_inner(&src, &dst, &build, arg, &progress)
						.map_ok(|_| ()),
				)
				.catch_unwind()
				.await;
				progress.finish("builds");
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

	async fn push_or_pull_build_inner(
		src: &impl tg::Handle,
		dst: &impl tg::Handle,
		build: &tg::build::Id,
		arg: tg::build::push::Arg,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<InnerOutput> {
		// Get the build.
		let output = src
			.get_build(build)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the build"))?;

		// Return an error if the build is not finished.
		if !output.status.is_finished() {
			return Err(tg::error!(%build, "build is not finished"));
		}

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
			depth: output.depth,
			error: output.error,
			host: output.host,
			log: output.log.clone(),
			output: output.output,
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

		// Update the progress.
		progress.increment("builds", 1);

		// Handle the incomplete objects.
		let mut incomplete_objects: Vec<tg::object::Id> = Vec::new();
		if arg.logs && incomplete.log {
			if let Some(log) = output.log.clone() {
				incomplete_objects.push(log.clone().into());
			}
		}
		if arg.outputs {
			incomplete_objects.extend(incomplete.output);
		}
		if arg.targets && incomplete.target {
			incomplete_objects.push(output.target.clone().into());
		}
		let incomplete_object_count_and_weight = incomplete_objects
			.iter()
			.map(|object| async {
				let stream = Self::push_or_pull_object(src, dst, object).await?.boxed();
				stream.last().await;
				Ok::<_, tg::Error>((0, 0))
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
				.map(|child| Self::push_or_pull_build_inner(src, dst, child, arg.clone(), progress))
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

		// Update the progress.
		let build_count = metadata.build_count.map_or_else(
			|| 1 + incomplete_children_build_count,
			|build_count| build_count - 1 - incomplete_children_build_count,
		);
		progress.increment("builds", build_count);
		let object_count = metadata.object_count.map_or_else(
			|| incomplete_object_count + incomplete_children_object_count,
			|object_count| {
				object_count - incomplete_object_count - incomplete_children_object_count
			},
		);
		progress.increment("objects", object_count);
		let object_weight = metadata.object_weight.map_or_else(
			|| incomplete_object_weight + incomplete_children_object_weight,
			|object_weight| {
				object_weight - incomplete_object_weight - incomplete_children_object_weight
			},
		);
		progress.increment("bytes", object_weight);

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
		let (object_count, object_depth, object_weight) = if arg.recursive {
			// If the push is recursive, then use the logs', outputs', and targets' counts and weights.
			let logs_count = if arg.logs { output.logs_count } else { Some(0) };
			let outputs_count = if arg.outputs {
				output.outputs_count
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
				.chain(Some(outputs_count))
				.chain(Some(targets_count))
				.sum::<Option<u64>>();
			let logs_depth = if arg.logs { output.logs_depth } else { Some(0) };
			let outputs_depth = if arg.outputs {
				output.outputs_depth
			} else {
				Some(0)
			};
			let targets_depth = if arg.targets {
				output.targets_depth
			} else {
				Some(0)
			};
			let depth = std::iter::empty()
				.chain(Some(logs_depth))
				.chain(Some(outputs_depth))
				.chain(Some(targets_depth))
				.max()
				.unwrap();
			let logs_weight = if arg.logs {
				output.logs_weight
			} else {
				Some(0)
			};
			let outputs_weight = if arg.outputs {
				output.outputs_weight
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
				.chain(Some(outputs_weight))
				.chain(Some(targets_weight))
				.sum::<Option<u64>>();
			(count, depth, weight)
		} else {
			// If the push is not recursive, then use the count, depth, and weight of the log, output, and target.
			let (log_count, log_depth, log_weight) = if arg.logs {
				if let Some(log) = output.log.as_ref() {
					if let Some(metadata) = src.try_get_object_metadata(&log.clone().into()).await?
					{
						(metadata.count, metadata.depth, metadata.weight)
					} else {
						(Some(0), Some(0), Some(0))
					}
				} else {
					(Some(0), Some(0), Some(0))
				}
			} else {
				(Some(0), Some(0), Some(0))
			};
			let (output_count, output_depth, output_weight) = if arg.outputs {
				if output.status.is_succeeded() {
					let metadata = output
						.output
						.as_ref()
						.map(|value| value.children())
						.iter()
						.flatten()
						.map(|child| src.try_get_object_metadata(child))
						.collect::<FuturesUnordered<_>>()
						.try_collect::<Vec<_>>()
						.await?;
					let count = metadata
						.iter()
						.map(|metadata| metadata.as_ref().and_then(|metadata| metadata.count))
						.sum::<Option<u64>>();
					let depth = metadata.iter().try_fold(0, |depth, metadata| {
						metadata
							.clone()
							.and_then(|metadata| metadata.depth)
							.map(|d| depth.max(d))
					});
					let weight = metadata
						.iter()
						.map(|metadata| metadata.as_ref().and_then(|metadata| metadata.weight))
						.sum::<Option<u64>>();
					(count, depth, weight)
				} else {
					(Some(0), Some(0), Some(0))
				}
			} else {
				(Some(0), Some(0), Some(0))
			};
			let (target_count, target_depth, target_weight) = {
				if let Some(metadata) = src
					.try_get_object_metadata(&output.target.clone().into())
					.await?
				{
					(metadata.count, metadata.depth, metadata.weight)
				} else {
					(Some(0), Some(0), Some(0))
				}
			};
			let count = std::iter::empty()
				.chain(Some(log_count))
				.chain(Some(output_count))
				.chain(Some(target_count))
				.sum::<Option<u64>>();
			let depth = std::iter::empty()
				.chain(Some(log_depth))
				.chain(Some(output_depth))
				.chain(Some(target_depth))
				.max()
				.unwrap();
			let weight = std::iter::empty()
				.chain(Some(log_weight))
				.chain(Some(output_weight))
				.chain(Some(target_weight))
				.sum::<Option<u64>>();
			(count, depth, weight)
		};
		Ok(Metadata {
			build_count,
			object_count,
			object_depth,
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
		// Parse the ID.
		let id = id.parse()?;

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the stream.
		let stream = handle.push_build(&id, arg).await?;

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
