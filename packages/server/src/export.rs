use crate::Server;
use dashmap::DashMap;
use futures::{
	future,
	stream::{self, FuturesUnordered},
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _,
};
use itertools::Itertools as _;
use num::ToPrimitive as _;
use smallvec::SmallVec;
use std::{
	panic::AssertUnwindSafe,
	pin::{pin, Pin},
	sync::{Arc, RwLock, Weak},
	time::Duration,
};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{request::Ext, Body};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

#[derive(Clone)]
struct Graph {
	nodes: Arc<DashMap<Either<tg::process::Id, tg::object::Id>, Arc<Node>, fnv::FnvBuildHasher>>,
}

struct Node {
	output: RwLock<Either<tg::process::put::Output, tg::object::put::Output>>,
	parents: RwLock<SmallVec<[Weak<Self>; 1]>>,
	children: RwLock<Vec<Arc<Self>>>,
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
		// Create the graph.
		let graph = Graph::new();

		// Spawn a task to receive import events and update the graphs.
		let import_event_task = tokio::spawn({
			let graph = graph.clone();
			async move {
				let mut stream = pin!(stream);
				while let Some(event) = stream.try_next().await? {
					match event {
						tg::import::Event::Complete(item) => {
							graph.mark_complete(&item.clone());
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
				let graph = graph.clone();
				let arg = arg.clone();
				async move {
					match item {
						Either::Left(process) => {
							let result = server
								.export_inner_process(
									None,
									process,
									&graph,
									item_sender,
									arg,
									progress,
								)
								.boxed()
								.await;
							if let Err(error) = result {
								item_sender.send(Err(error)).await.ok();
							}
						},
						Either::Right(object) => {
							let result = server
								.export_inner_object(None, object, &graph, item_sender, progress)
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
		parent: Option<&Either<tg::process::Id, tg::object::Id>>,
		object: &tg::object::Id,
		graph: &Graph,
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

		// If the object has already been sent or is complete, then update the progress and return.
		if !graph.insert(parent, &Either::Right(object.clone()))
			|| graph.is_complete(&Either::Right(object.clone()))
		{
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
			let output = Box::pin(self.export_inner_object(
				Some(&Either::Right(object.clone())),
				&child,
				graph,
				item_sender,
				progress,
			))
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
		parent: Option<&tg::process::Id>,
		process: &tg::process::Id,
		graph: &Graph,
		item_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::export::Item>>,
		arg: tg::export::Arg,
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

		if !graph.insert(
			parent.cloned().map(Either::Left).as_ref(),
			&Either::Left(process.clone()),
		) {
			return Ok(InnerProcessOutput {
				process_count: stats.process_count.unwrap_or(1),
				object_count: stats.object_count.unwrap_or(0),
				object_weight: stats.object_weight.unwrap_or(0),
			});
		}

		// Send the process if it is not complete.
		if !graph.is_complete(&Either::Left(process.clone())) {
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
				let output = self
					.export_inner_object(
						Some(&Either::Left(process.clone())),
						object,
						graph,
						item_sender,
						progress,
					)
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
		} = if arg.recursive {
			let outputs = children
				.iter()
				.map(|child| {
					self.export_inner_process(
						Some(process),
						child,
						graph,
						item_sender,
						arg.clone(),
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
		} else {
			InnerProcessOutput {
				process_count: 0,
				object_count: 0,
				object_weight: 0,
			}
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

impl Graph {
	fn new() -> Self {
		Self {
			nodes: Arc::new(DashMap::default()),
		}
	}

	fn insert(
		&self,
		parent: Option<&Either<tg::process::Id, tg::object::Id>>,
		item: &Either<tg::process::Id, tg::object::Id>,
	) -> bool {
		// Check if the node already exists.
		if self.nodes.contains_key(item) {
			return false;
		}

		// Create the node.
		let new_node = match item {
			Either::Left(_) => Node::process(),
			Either::Right(_) => Node::object(),
		};
		let new_node = Arc::new(new_node);

		// If there's a parent, create the relationship.
		if let Some(parent) = parent {
			// Add the new node as a child of the parent.
			if let Some(parent_node) = self.nodes.get(parent) {
				parent_node
					.children
					.write()
					.unwrap()
					.push(Arc::clone(&new_node));
				// Add the parent as a weak reference to the new node.
				new_node
					.parents
					.write()
					.unwrap()
					.push(Arc::downgrade(&parent_node));
			} else {
				tracing::debug!("parent not found");
			}
		}

		// Insert the new node.
		self.nodes.insert(item.clone(), new_node);

		true
	}

	fn mark_complete(&self, object: &Either<tg::process::Id, tg::object::Id>) {
		if let Some(node) = self.nodes.get(object) {
			// Mark the node as complete.
			let mut output = node.output.write().unwrap();
			match *output {
				Either::Left(ref mut process_output) => process_output.complete = true, // FIXME respect arg.
				Either::Right(ref mut object_output) => object_output.complete = true,
			}
		} else {
			tracing::debug!("attempted to mark a node as complete that does not exist");
		}
	}

	// TODO take optional arg to determine whether logs/outputs/command are part of the deal.
	fn is_complete(&self, object: &Either<tg::process::Id, tg::object::Id>) -> bool {
		let Some(node) = self.nodes.get(&object.clone()) else {
			return false;
		};

		// If this node is complete, return true.
		if node.is_complete() {
			return true;
		}

		// If this node is not complete, check ancestors recursively.

		// Track a list of nodes to a specific ancestor.
		let mut completion_path = Vec::new();
		let found_complete_ancestor = Self::check_ancestor_completion(&node, &mut completion_path);

		// if we found a path to complete, mark intermediate nodes as complete.
		if found_complete_ancestor {
			for node in completion_path {
				let mut output = node.output.write().unwrap();
				match *output {
					Either::Left(ref mut process_output) => process_output.complete = true, // FIXME respect arg.
					Either::Right(ref mut object_output) => object_output.complete = true,
				}
			}
			true
		} else {
			false
		}
	}

	fn check_ancestor_completion(node: &Node, completion_path: &mut Vec<Arc<Node>>) -> bool {
		let parents = node.parents.read().unwrap();

		for parent_weak in parents.iter() {
			if let Some(parent) = parent_weak.upgrade() {
				// If this parent is complete, mark this path complete.
				if parent.is_complete() {
					return true;
				}

				// Cycle detection - if we already stored this node, keep looking.
				if completion_path
					.iter()
					.any(|node| Arc::ptr_eq(node, &parent))
				{
					continue;
				}

				// Add this path to the completion path and and check ancestors.
				completion_path.push(Arc::clone(&parent));
				if Self::check_ancestor_completion(&parent, completion_path) {
					// If we found one we're done.
					return true;
				}
				// If not, remove ourselves from the path and try again.
				completion_path.pop();
			} else {
				tracing::debug!("failed to upgrade weak pointer");
			}
		}

		false
	}

	fn update_output(
		&self,
		item: &Either<tg::process::Id, tg::object::Id>,
		new_output: Either<tg::process::put::Output, tg::object::put::Output>,
	) -> bool {
		if let Some(node) = self.nodes.get(item) {
			let mut output = node.output.write().unwrap();
			match (&*output, &new_output) {
				(Either::Left(_), Either::Left(_)) | (Either::Right(_), Either::Right(_)) => {
					*output = new_output;
					true
				},
				_ => {
					tracing::error!("attempted to update output with mismatched type");
					false
				},
			}
		} else {
			tracing::debug!("attempted to update output for non-existent node");
			false
		}
	}
}

impl Node {
	fn object() -> Self {
		Self {
			parents: RwLock::new(SmallVec::new()),
			children: RwLock::new(Vec::new()),
			output: RwLock::new(Either::Right(tg::object::put::Output { complete: false })),
		}
	}

	fn process() -> Self {
		Self {
			parents: RwLock::new(SmallVec::new()),
			children: RwLock::new(Vec::new()),
			output: RwLock::new(Either::Left(tg::process::put::Output {
				complete: false,
				commands_complete: false,
				logs_complete: false,
				outputs_complete: false,
			})),
		}
	}

	fn is_complete(&self) -> bool {
		let output = self.output.read().unwrap();
		match *output {
			Either::Left(ref process_output) => {
				// TODO logs/command/outputs?
				process_output.complete
			},
			Either::Right(ref object_output) => object_output.complete,
		}
	}
}
