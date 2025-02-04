use crate::Server;
use futures::{
	stream::FuturesUnordered, FutureExt as _, Stream, StreamExt as _, TryFutureExt as _,
	TryStreamExt,
};
use itertools::Itertools as _;
use std::panic::AssertUnwindSafe;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_futures::stream::Ext as _;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
use tokio_util::task::AbortOnDropHandle;

struct InnerOutput {
	process_count: u64,
	object_count: u64,
	object_weight: u64,
}

struct Stats {
	process_count: Option<u64>,
	object_count: Option<u64>,
	#[allow(dead_code)]
	object_depth: Option<u64>,
	object_weight: Option<u64>,
}

impl Server {
	pub async fn push_process(
		&self,
		process: &tg::process::Id,
		arg: tg::process::push::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static> {
		let client = self.get_remote_client(arg.remote.clone()).await?;
		Self::push_or_pull_process(self, &client, process, arg).await
	}

	pub(crate) async fn push_or_pull_process<S, D>(
		src: &S,
		dst: &D,
		process: &tg::process::Id,
		arg: tg::process::push::Arg,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static + use<S, D>,
	>
	where
		S: tg::Handle,
		D: tg::Handle,
	{
		let output = src.get_process(process).await?;
		let stats = Self::get_process_stats(src, &output, &arg).await?;
		let progress = crate::progress::Handle::new();
		let task = tokio::spawn({
			let src = src.clone();
			let dst = dst.clone();
			let process = process.clone();
			let progress = progress.clone();
			async move {
				progress.start(
					"processes".to_owned(),
					"processes".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					stats.process_count,
				);
				progress.start(
					"objects".to_owned(),
					"objects".to_owned(),
					tg::progress::IndicatorFormat::Normal,
					Some(0),
					stats.object_count,
				);
				progress.start(
					"bytes".to_owned(),
					"bytes".to_owned(),
					tg::progress::IndicatorFormat::Bytes,
					Some(0),
					stats.object_weight,
				);
				let result = AssertUnwindSafe(
					Self::push_or_pull_process_inner(&src, &dst, &process, arg, &progress)
						.map_ok(|_| ()),
				)
				.catch_unwind()
				.await;
				progress.finish("processes");
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

	async fn push_or_pull_process_inner(
		src: &impl tg::Handle,
		dst: &impl tg::Handle,
		process: &tg::process::Id,
		arg: tg::process::push::Arg,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<InnerOutput> {
		// Get the process.
		let output = src
			.get_process(process)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?;

		// Return an error if the process is not finished.
		if !output.status.is_finished() {
			return Err(tg::error!(%process, "process is not finished"));
		}

		// Get the stats.
		let stats = Self::get_process_stats(src, &output, &arg).await?;

		// Get the children.
		let children_arg = tg::process::children::get::Arg::default();
		let children = src
			.get_process_children(process, children_arg)
			.await?
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.flat_map(|chunk| chunk.data)
			.collect_vec();

		// Put the process.
		let put_arg = tg::process::put::Arg {
			checksum: output.checksum,
			children: children.clone(),
			command: output.command.clone(),
			created_at: output.created_at,
			cwd: output.cwd,
			dequeued_at: output.dequeued_at,
			enqueued_at: output.enqueued_at,
			env: output.env,
			error: output.error,
			finished_at: output.finished_at,
			host: output.host,
			id: process.clone(),
			log: output.log.clone(),
			network: output.network,
			output: output.output.clone(),
			retry: output.retry,
			started_at: output.started_at,
			status: output.status,
		};
		let put_output = dst
			.put_process(process, put_arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;

		// Update the progress.
		progress.increment("processes", 1);

		// Handle the command, log, and output.
		let mut objects: Vec<tg::object::Id> = Vec::new();
		if arg.commands {
			objects.push(output.command.clone().into());
		}
		if arg.logs {
			if let Some(log) = output.log.clone() {
				objects.push(log.clone().into());
			}
		}
		if arg.outputs {
			if let Some(output_objects) = output.output.as_ref().map(tg::value::Data::children) {
				objects.extend(output_objects);
			}
		}
		let self_object_count_and_weight = objects
			.iter()
			.map(|object| async {
				let output = Self::push_or_pull_object_inner(src, dst, object, progress).await?;
				Ok::<_, tg::Error>((output.count, output.weight))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		let self_object_count = self_object_count_and_weight
			.iter()
			.map(|(count, _)| count)
			.sum::<u64>();
		let self_object_weight = self_object_count_and_weight
			.iter()
			.map(|(_, weight)| weight)
			.sum::<u64>();

		// Recurse into the children.
		let InnerOutput {
			process_count: children_process_count,
			object_count: children_object_count,
			object_weight: children_object_weight,
		} = if put_output.complete || !arg.recursive {
			InnerOutput {
				process_count: 0,
				object_count: 0,
				object_weight: 0,
			}
		} else {
			let outputs = children
				.iter()
				.map(|child| {
					Self::push_or_pull_process_inner(src, dst, child, arg.clone(), progress)
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?;
			outputs.into_iter().fold(
				InnerOutput {
					process_count: 0,
					object_count: 0,
					object_weight: 0,
				},
				|a, b| InnerOutput {
					process_count: a.process_count + b.process_count,
					object_count: a.object_count + b.object_count,
					object_weight: a.object_weight + b.object_weight,
				},
			)
		};

		// Update the progress.
		let process_count = stats.process_count.map_or_else(
			|| 1 + children_process_count,
			|process_count| process_count - 1 - children_process_count,
		);
		progress.increment("processes", process_count);
		let object_count = stats.object_count.map_or_else(
			|| self_object_count + children_object_count,
			|object_count| object_count - self_object_count - children_object_count,
		);
		progress.increment("objects", object_count);
		let object_weight = stats.object_weight.map_or_else(
			|| self_object_weight + children_object_weight,
			|object_weight| object_weight - self_object_weight - children_object_weight,
		);
		progress.increment("bytes", object_weight);

		Ok(InnerOutput {
			process_count,
			object_count,
			object_weight,
		})
	}

	async fn get_process_stats(
		src: &impl tg::Handle,
		output: &tg::process::get::Output,
		arg: &tg::process::push::Arg,
	) -> tg::Result<Stats> {
		let process_count = if arg.recursive { output.count } else { Some(1) };
		let (object_count, object_depth, object_weight) = if arg.recursive {
			// If the push is recursive, then use the logs', outputs', and commands' counts and weights.
			let logs_count = if arg.logs { output.logs_count } else { Some(0) };
			let outputs_count = if arg.outputs {
				output.outputs_count
			} else {
				Some(0)
			};
			let commands_count = if arg.commands {
				output.commands_count
			} else {
				Some(0)
			};
			let count = std::iter::empty()
				.chain(Some(logs_count))
				.chain(Some(outputs_count))
				.chain(Some(commands_count))
				.sum::<Option<u64>>();
			let logs_depth = if arg.logs { output.logs_depth } else { Some(0) };
			let outputs_depth = if arg.outputs {
				output.outputs_depth
			} else {
				Some(0)
			};
			let commands_depth = if arg.commands {
				output.commands_depth
			} else {
				Some(0)
			};
			let depth = std::iter::empty()
				.chain(Some(logs_depth))
				.chain(Some(outputs_depth))
				.chain(Some(commands_depth))
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
			let commands_weight = if arg.commands {
				output.commands_weight
			} else {
				Some(0)
			};
			let weight = std::iter::empty()
				.chain(Some(logs_weight))
				.chain(Some(outputs_weight))
				.chain(Some(commands_weight))
				.sum::<Option<u64>>();
			(count, depth, weight)
		} else {
			// If the push is not recursive, then use the count, depth, and weight of the log, output, and command.
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
						.map(tg::value::Data::children)
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
			let (command_count, command_depth, command_weight) = {
				if let Some(metadata) = src
					.try_get_object_metadata(&output.command.clone().into())
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
				.chain(Some(command_count))
				.sum::<Option<u64>>();
			let depth = std::iter::empty()
				.chain(Some(log_depth))
				.chain(Some(output_depth))
				.chain(Some(command_depth))
				.max()
				.unwrap();
			let weight = std::iter::empty()
				.chain(Some(log_weight))
				.chain(Some(output_weight))
				.chain(Some(command_weight))
				.sum::<Option<u64>>();
			(count, depth, weight)
		};
		Ok(Stats {
			process_count,
			object_count,
			object_depth,
			object_weight,
		})
	}
}

impl Server {
	pub(crate) async fn handle_push_process_request<H>(
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
		let stream = handle.push_process(&id, arg).await?;

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
