use crate::Server;
use futures::{
	future,
	stream::{self, FuturesUnordered},
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use itertools::Itertools as _;
use num::ToPrimitive as _;
use std::{
	collections::BTreeMap,
	panic::AssertUnwindSafe,
	pin::{pin, Pin},
	sync::{Arc, RwLock},
	time::Duration,
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

pub(crate) struct InnerObjectOutput {
	pub(crate) count: u64,
	pub(crate) weight: u64,
}

pub(crate) struct InnerProcessOutput {
	pub(crate) process_count: u64,
	pub(crate) object_count: u64,
	pub(crate) object_weight: u64,
}

struct ProcessStats {
	process_count: Option<u64>,
	object_count: Option<u64>,
	object_weight: Option<u64>,
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

		// Create the progress handle and add the indicators.
		let progress = crate::progress::Handle::new();
		progress.start(
			"processes".to_owned(),
			"processes".to_owned(),
			tg::progress::IndicatorFormat::Normal,
			Some(0),
			None,
		);
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

		// Spawn a task to set the indicator totals as soon as they are ready.
		let indicator_total_task = tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			let arg = arg.clone();
			async move {
				server
					.set_export_progress_indicator_totals(&arg, &progress)
					.await;
			}
		});
		let indicator_total_task_abort_handle = AbortOnDropHandle::new(indicator_total_task);

		// Create the task.
		let (item_sender, item_receiver) = tokio::sync::mpsc::channel(256);
		let task = tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				let result =
					AssertUnwindSafe(server.export_inner(arg, stream, &item_sender, &progress))
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
				}
			}
		});
		let item_receiver_stream = ReceiverStream::new(item_receiver);
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = stream::select(
			item_receiver_stream.map_ok(tg::export::Event::Item),
			progress.stream().map_ok(tg::export::Event::Progress),
		)
		.attach(abort_handle)
		.attach(indicator_total_task_abort_handle);

		Ok(stream.right_stream())
	}

	async fn set_export_progress_indicator_totals(
		&self,
		arg: &tg::export::Arg,
		progress: &crate::progress::Handle<()>,
	) {
		let mut metadata_futures = arg
			.items
			.iter()
			.map(|item| {
				let server = self.clone();
				async move {
					loop {
						match item {
							tangram_either::Either::Left(ref process) => {
								let tg::process::get::Output { metadata, .. } =
									server.get_process(process).await.map_err(|source| {
										tg::error!(!source, "failed to get the process")
									})?;
								let metadata = metadata
									.ok_or_else(|| tg::error!("expected the metadata to be set"))?;
								let mut complete = metadata.count.is_some();
								if arg.commands {
									complete = complete
										&& metadata.commands_count.is_some()
										&& metadata.commands_weight.is_some();
								}
								if arg.logs {
									complete = complete
										&& metadata.logs_count.is_some()
										&& metadata.logs_weight.is_some();
								}
								if arg.outputs {
									complete = complete
										&& metadata.outputs_count.is_some()
										&& metadata.outputs_weight.is_some();
								}
								if complete {
									break Ok::<_, tg::Error>(Either::Left(metadata));
								}
							},
							tangram_either::Either::Right(ref id) => {
								let metadata = server
									.try_get_object_metadata_local(id)
									.await?
									.ok_or_else(|| tg::error!("expected the metadata to be set"))?;

								if metadata.count.is_some() && metadata.weight.is_some() {
									break Ok::<_, tg::Error>(Either::Right(metadata));
								}
							},
						}
						tokio::time::sleep(Duration::from_secs(1)).await;
					}
				}
			})
			.collect::<FuturesUnordered<_>>();
		let mut total_processes: u64 = 0;
		let mut total_objects: u64 = 0;
		let mut total_bytes: u64 = 0;
		while let Some(Ok(metadata)) = metadata_futures.next().await {
			match metadata {
				Either::Left(metadata) => {
					if let Some(count) = metadata.count {
						total_processes += count;
						progress.set_total("processes", total_processes);
					}
					if arg.commands {
						if let Some(commands_count) = metadata.commands_count {
							total_objects += commands_count;
						}
						if let Some(commands_weight) = metadata.commands_weight {
							total_bytes += commands_weight;
						}
					}
					if arg.logs {
						if let Some(logs_count) = metadata.logs_count {
							total_objects += logs_count;
						}
						if let Some(logs_weight) = metadata.logs_weight {
							total_bytes += logs_weight;
						}
					}
					if arg.outputs {
						if let Some(outputs_count) = metadata.outputs_count {
							total_objects += outputs_count;
						}
						if let Some(outputs_weight) = metadata.outputs_weight {
							total_bytes += outputs_weight;
						}
					}
					progress.set_total("objects", total_objects);
					progress.set_total("bytes", total_bytes);
				},
				Either::Right(metadata) => {
					if let Some(count) = metadata.count {
						total_objects += count;
						progress.set_total("objects", total_objects);
					}
					if let Some(weight) = metadata.weight {
						total_bytes += weight;
						progress.set_total("bytes", total_bytes);
					}
				},
			}
		}
	}

	async fn export_inner(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Event>> + Send + 'static>>,
		item_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::export::Item>>,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<()> {
		// Create the process graph.
		let process_graph = Graph::new();

		// Create the object graph.
		let object_graph = Graph::new();

		// Spawn a task to receive import events and update the graphs.
		let import_event_task = tokio::spawn({
			let object_graph = object_graph.clone();
			let process_graph = process_graph.clone();
			async move {
				let mut stream = pin!(stream);
				while let Some(event) = stream.try_next().await? {
					match event {
						tg::import::Event::Complete(item) => match item {
							Either::Left(id) => {
								process_graph.mark_complete(&id);
							},
							Either::Right(id) => {
								object_graph.mark_complete(&id);
							},
						},
					}
				}
				Ok::<_, tg::Error>(())
			}
		});
		scopeguard::defer! {
			import_event_task.abort();
		}

		// Export the items.
		arg.items
			.iter()
			.map(|item| {
				let server = self.clone();
				let object_graph = object_graph.clone();
				let process_graph = process_graph.clone();
				let arg = arg.clone();
				async move {
					match item {
						Either::Left(process) => {
							process_graph.insert(None, process);
							let result = server
								.export_inner_process(
									arg,
									process,
									&object_graph,
									&process_graph,
									item_sender,
									progress,
								)
								.boxed()
								.await;
							if let Err(error) = result {
								item_sender.send(Err(error)).await.ok();
							}
						},
						Either::Right(object) => {
							object_graph.insert(None, object);
							let result = server
								.export_inner_object(object, &object_graph, item_sender, progress)
								.boxed()
								.await;
							if let Err(error) = result {
								item_sender.send(Err(error)).await.ok();
							}
						},
					}
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;

		Ok(())
	}

	async fn export_inner_object(
		&self,
		object: &tg::object::Id,
		object_graph: &Graph<tg::object::Id>,
		item_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::export::Item>>,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<InnerObjectOutput> {
		// Get the object.
		let tg::object::get::Output { bytes, metadata } = self
			.get_object(object)
			.await
			.map_err(|source| tg::error!(!source, %object, "failed to get the object"))?;
		let metadata = metadata.ok_or_else(|| tg::error!("expected the metadata to be set"))?;
		let data = tg::object::Data::deserialize(object.kind(), &bytes)?;
		let size = bytes.len().to_u64().unwrap();

		// If the object has been marked complete, then update the progress and return.
		if object_graph.is_complete(object) {
			let count = metadata.count.unwrap_or(1);
			let weight = metadata.weight.unwrap_or(size);
			progress.increment("objects", count);
			progress.increment("bytes", weight);
			let output = InnerObjectOutput { count, weight };
			return Ok(output);
		}

		// Send the object.
		let item = tg::export::Item::Object {
			id: object.clone(),
			bytes: bytes.clone(),
		};
		item_sender
			.send(Ok(item))
			.await
			.map_err(|source| tg::error!(!source, "failed to send"))?;

		// Increment the count and add the object's size to the weight.
		progress.increment("objects", 1);
		progress.increment("bytes", size);

		// Recurse into the children.
		let mut children_count = 0;
		let mut children_weight = 0;
		for child in data.children() {
			object_graph.insert(Some(object), &child);
			let output =
				Box::pin(self.export_inner_object(&child, object_graph, item_sender, progress))
					.await?;
			children_count += output.count;
			children_weight += output.weight;
		}
		if let Some(count) = metadata.count {
			progress.increment("objects", count - 1 - children_count);
		}
		if let Some(weight) = metadata.weight {
			progress.increment("bytes", weight - size - children_weight);
		}

		// Create the output.
		let count = metadata.count.unwrap_or_else(|| 1 + children_count);
		let weight = metadata.weight.unwrap_or_else(|| size + children_weight);
		let output = InnerObjectOutput { count, weight };

		Ok(output)
	}

	async fn export_inner_process(
		&self,
		arg: tg::export::Arg,
		process: &tg::process::Id,
		object_graph: &Graph<tg::object::Id>,
		process_graph: &Graph<tg::process::Id>,
		item_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::export::Item>>,
		progress: &crate::progress::Handle<()>,
	) -> tg::Result<InnerProcessOutput> {
		// Get the process
		let tg::process::get::Output { data, metadata } = self
			.get_process(process)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?;
		let metadata = metadata.ok_or_else(|| tg::error!("expected the metadata to be set"))?;

		// Return an error if the process is not finished.
		if !data.status.is_finished() {
			return Err(tg::error!(%process, "process is not finished"));
		}

		// Get the stats.
		let stats = Self::get_process_stats_local(&self.clone(), &arg, &data, &metadata).await?;

		// Get the children.
		let children_arg = tg::process::children::get::Arg::default();
		let children = self
			.get_process_children(process, children_arg)
			.await?
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.flat_map(|chunk| chunk.data)
			.collect_vec();

		// Send the process if it is not complete.
		if !process_graph.is_complete(process) {
			let item = tg::export::Item::Process {
				id: process.clone(),
				data: data.clone(),
			};
			item_sender
				.send(Ok(item))
				.await
				.map_err(|source| tg::error!(!source, "failed to send"))?;
		}

		// Update the progress.
		progress.increment("processes", 1);

		// Handle the command, log, and output.
		let mut objects: Vec<tg::object::Id> = Vec::new();
		if arg.commands {
			objects.push(data.command.clone().into());
		}
		if arg.logs {
			if let Some(log) = data.log.clone() {
				objects.push(log.clone().into());
			}
		}
		if arg.outputs {
			if let Some(output_objects) = data.output.as_ref().map(tg::value::Data::children) {
				objects.extend(output_objects);
			}
		}
		let self_object_count_and_weight = objects
			.iter()
			.map(|object| async {
				object_graph.insert(None, object);
				let output = self
					.export_inner_object(object, object_graph, item_sender, progress)
					.await?;
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
		let InnerProcessOutput {
			process_count: children_process_count,
			object_count: children_object_count,
			object_weight: children_object_weight,
		} = if process_graph.is_complete(process) || !arg.recursive {
			InnerProcessOutput {
				process_count: 0,
				object_count: 0,
				object_weight: 0,
			}
		} else {
			let outputs = children
				.iter()
				.map(|child| {
					self.export_inner_process(
						arg.clone(),
						child,
						object_graph,
						process_graph,
						item_sender,
						progress,
					)
				})
				.collect::<FuturesUnordered<_>>()
				.try_collect::<Vec<_>>()
				.await?;
			outputs.into_iter().fold(
				InnerProcessOutput {
					process_count: 0,
					object_count: 0,
					object_weight: 0,
				},
				|a, b| InnerProcessOutput {
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

		Ok(InnerProcessOutput {
			process_count,
			object_count,
			object_weight,
		})
	}

	async fn get_process_stats_local(
		src: &impl tg::Handle,
		arg: &tg::export::Arg,
		data: &tg::process::Data,
		metadata: &tg::process::Metadata,
	) -> tg::Result<ProcessStats> {
		let process_count = if arg.recursive {
			metadata.count
		} else {
			Some(1)
		};
		let (object_count, object_weight) = if arg.recursive {
			// If the push is recursive, then use the logs', outputs', and commands' counts and weights.
			let logs_count = if arg.logs {
				metadata.logs_count
			} else {
				Some(0)
			};
			let outputs_count = if arg.outputs {
				metadata.outputs_count
			} else {
				Some(0)
			};
			let commands_count = if arg.commands {
				metadata.commands_count
			} else {
				Some(0)
			};
			let count = std::iter::empty()
				.chain(Some(logs_count))
				.chain(Some(outputs_count))
				.chain(Some(commands_count))
				.sum::<Option<u64>>();
			let logs_weight = if arg.logs {
				metadata.logs_weight
			} else {
				Some(0)
			};
			let outputs_weight = if arg.outputs {
				metadata.outputs_weight
			} else {
				Some(0)
			};
			let commands_weight = if arg.commands {
				metadata.commands_weight
			} else {
				Some(0)
			};
			let weight = std::iter::empty()
				.chain(Some(logs_weight))
				.chain(Some(outputs_weight))
				.chain(Some(commands_weight))
				.sum::<Option<u64>>();
			(count, weight)
		} else {
			// If the push is not recursive, then use the count and weight of the log, output, and command.
			let (log_count, log_weight) = if arg.logs {
				if let Some(log) = data.log.as_ref() {
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
			let (output_count, output_weight) = if arg.outputs {
				if data.status.is_succeeded() {
					let metadata = data
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
			let (command_count, command_weight) = {
				if let Some(metadata) = src
					.try_get_object_metadata(&data.command.clone().into())
					.await?
				{
					(metadata.count, metadata.weight)
				} else {
					(Some(0), Some(0))
				}
			};
			let count = std::iter::empty()
				.chain(Some(log_count))
				.chain(Some(output_count))
				.chain(Some(command_count))
				.sum::<Option<u64>>();
			let weight = std::iter::empty()
				.chain(Some(log_weight))
				.chain(Some(output_weight))
				.chain(Some(command_weight))
				.sum::<Option<u64>>();
			(count, weight)
		};
		Ok(ProcessStats {
			process_count,
			object_count,
			object_weight,
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
			.query_params::<tg::export::QueryArg>()
			.transpose()?
			.ok_or_else(|| tg::error!("query parameters required"))?
			.into();

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
