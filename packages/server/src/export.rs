use crate::Server;
use async_compression::tokio::bufread::ZstdEncoder;
use dashmap::DashMap;
use futures::{
	FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream::FuturesUnordered,
};
use indoc::formatdoc;
use itertools::Itertools as _;
use num::ToPrimitive as _;
use rusqlite as sqlite;
use smallvec::SmallVec;
use std::{
	panic::AssertUnwindSafe,
	path::PathBuf,
	pin::{Pin, pin},
	sync::{Arc, RwLock, Weak, atomic::AtomicBool},
};
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{
	Body,
	header::{accept_encoding::AcceptEncoding, content_encoding::ContentEncoding},
	request::Ext as _,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

#[derive(Clone)]
struct Graph {
	nodes: Arc<DashMap<Either<tg::process::Id, tg::object::Id>, Arc<Node>, fnv::FnvBuildHasher>>,
}

struct Node {
	complete: Either<RwLock<ProcessComplete>, AtomicBool>,
	parents: RwLock<SmallVec<[Weak<Self>; 1]>>,
	children: RwLock<Vec<Arc<Self>>>,
}

#[derive(Clone)]
struct ProcessComplete {
	pub commands_complete: bool,
	pub complete: bool,
	pub outputs_complete: bool,
}

struct InnerObjectOutput {
	pub count: u64,
	pub weight: u64,
}

struct InnerProcessOutput {
	pub process_count: u64,
	pub object_count: u64,
	pub object_weight: u64,
}

struct ProcessStats {
	process_count: Option<u64>,
	object_count: Option<u64>,
	object_weight: Option<u64>,
}

struct StateSync {
	database: sqlite::Connection,
	index: sqlite::Connection,
	file: Option<(tg::artifact::Id, Option<PathBuf>, std::fs::File)>,
	graph: Graph,
	import_complete_receiver: tokio::sync::mpsc::Receiver<tg::import::Complete>,
	event_sender: tokio::sync::mpsc::Sender<tg::Result<tg::export::Event>>,
}

impl Server {
	pub async fn export(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Complete>> + Send + 'static>>,
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

		// Create the task.
		let (event_sender, event_receiver) = tokio::sync::mpsc::channel(4096);
		let task = tokio::spawn({
			let server = self.clone();
			async move {
				let result = AssertUnwindSafe(server.export_inner(arg, stream, &event_sender))
					.catch_unwind()
					.await;
				match result {
					Ok(Ok(())) => {
						event_sender
							.send(Ok(tg::export::Event::End))
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to send the export end event");
							})
							.ok();
					},
					Ok(Err(error)) => {
						event_sender
							.send(Err(error))
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to send the export error");
							})
							.ok();
					},
					Err(payload) => {
						let message = payload
							.downcast_ref::<String>()
							.map(String::as_str)
							.or(payload.downcast_ref::<&str>().copied());
						event_sender
							.send(Err(tg::error!(?message, "the task panicked")))
							.await
							.inspect_err(|error| {
								tracing::error!(?error, "failed to send the export panic");
							})
							.ok();
					},
				}
			}
		});

		// Create the stream.
		let stream = ReceiverStream::new(event_receiver).attach(AbortOnDropHandle::new(task));

		Ok(stream.right_stream())
	}

	async fn export_inner(
		&self,
		arg: tg::export::Arg,
		stream: Pin<Box<dyn Stream<Item = tg::Result<tg::import::Complete>> + Send + 'static>>,
		event_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::export::Event>>,
	) -> tg::Result<()> {
		if self.database.is_left()
			&& self.index.is_left()
			&& matches!(self.store, crate::Store::Lmdb(_) | crate::Store::Memory(_))
		{
			let (import_complete_sender, import_complete_receiver) =
				tokio::sync::mpsc::channel(4096);
			let import_task = tokio::spawn(async move {
				let mut stream = std::pin::pin!(stream);
				while let Some(event) = stream.try_next().await? {
					import_complete_sender.send(event).await.ok();
				}
				Ok::<_, tg::Error>(())
			});
			scopeguard::defer! {
				import_task.abort();
			}
			tokio::task::spawn_blocking({
				let server = self.clone();
				let arg = arg.clone();
				let event_sender = event_sender.clone();
				move || server.export_sync_task(&arg, import_complete_receiver, event_sender)
			})
			.await
			.unwrap()?;
			return Ok(());
		}

		// Create the graph.
		let graph = Graph::new();

		// Spawn a task to receive import completion events and update the graph.
		let import_complete_task = tokio::spawn({
			let graph = graph.clone();
			async move {
				let mut stream = pin!(stream);
				while let Some(complete) = stream.try_next().await? {
					match &complete {
						tg::import::Complete::Process(process_complete) => {
							graph.update_complete(&Either::Left(process_complete));
						},
						tg::import::Complete::Object(object_complete) => {
							graph.update_complete(&Either::Right(object_complete));
						},
					}
				}
				Ok::<_, tg::Error>(())
			}
		});
		scopeguard::defer! {
			import_complete_task.abort();
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
								.export_inner_process(None, process, &graph, event_sender, arg)
								.boxed()
								.await;
							if let Err(error) = result {
								event_sender.send(Err(error)).await.ok();
							}
						},
						Either::Right(object) => {
							let result = server
								.export_inner_object(None, object, &graph, event_sender)
								.boxed()
								.await;
							if let Err(error) = result {
								event_sender.send(Err(error)).await.ok();
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

	fn export_sync_task(
		&self,
		arg: &tg::export::Arg,
		import_complete_receiver: tokio::sync::mpsc::Receiver<tg::import::Complete>,
		event_sender: tokio::sync::mpsc::Sender<tg::Result<tg::export::Event>>,
	) -> tg::Result<()> {
		// Create a database connection.
		let database = self
			.database
			.as_ref()
			.left()
			.ok_or_else(|| tg::error!("expected the database to be sqlite"))?
			.create_connection(true)
			.map_err(|source| tg::error!(!source, "failed to create a connection"))?;

		// Create an index connection.
		let index = self
			.index
			.as_ref()
			.left()
			.ok_or_else(|| tg::error!("expected the index to be sqlite"))?
			.create_connection(true)
			.map_err(|source| tg::error!(!source, "failed to create a connection"))?;

		// Create an empty graph.
		let graph = Graph::new();

		// Create the export state.
		let mut state = StateSync {
			database,
			index,
			file: None,
			graph,
			import_complete_receiver,
			event_sender,
		};

		// Export each item.
		for item in &arg.items {
			let result = match item {
				Either::Left(process) => self
					.export_sync_inner_process(&mut state, arg, None, process)
					.map(|_| ()),
				Either::Right(object) => self
					.export_sync_inner_object(&mut state, None, object)
					.map(|_| ()),
			};
			if let Err(error) = result {
				state.event_sender.blocking_send(Err(error)).ok();
			}
		}

		Ok(())
	}

	async fn export_inner_process(
		&self,
		parent: Option<&tg::process::Id>,
		process: &tg::process::Id,
		graph: &Graph,
		event_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::export::Event>>,
		arg: tg::export::Arg,
	) -> tg::Result<InnerProcessOutput> {
		// Get the process
		let tg::process::get::Output { data, .. } = self
			.get_process(process)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?;
		let process_complete =
			self.export_get_process_complete(process)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to get export process complete status")
				})?;

		// Return an error if the process is not finished.
		if !data.status.is_finished() {
			return Err(tg::error!(%process, "process is not finished"));
		}

		// Get the stats.
		let stats =
			Self::export_get_process_stats(&self.clone(), &arg, &data, &process_complete).await?;

		if !graph.insert(parent.map(Either::Left).as_ref(), &Either::Left(process)) {
			return Ok(InnerProcessOutput {
				process_count: stats.process_count.unwrap_or(1),
				object_count: stats.object_count.unwrap_or(0),
				object_weight: stats.object_weight.unwrap_or(0),
			});
		}

		// Send the process if it is not complete.
		if graph.is_complete(&Either::Left(process)) {
			event_sender
				.send(Ok(tg::export::Event::Complete(
					tg::export::Complete::Process(process_complete.clone()),
				)))
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to send export process complete event")
				})?;
		} else {
			let item = tg::export::Item::Process(tg::export::ProcessItem {
				id: process.clone(),
				data: data.clone(),
			});
			event_sender
				.send(Ok(tg::export::Event::Item(item)))
				.await
				.map_err(|source| tg::error!(!source, "failed to send"))?;
		}

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

		// Handle the command and output.
		let current_output = graph.get_complete(&Either::Left(process));

		let mut objects: Vec<tg::object::Id> = Vec::new();
		if arg.commands {
			let commands_complete = current_output.as_ref().is_some_and(|either| {
				either
					.as_ref()
					.left()
					.is_some_and(|output| output.commands_complete)
			});
			if !commands_complete {
				objects.push(data.command.clone().into());
			}
		}
		if arg.outputs {
			let outputs_complete = current_output.as_ref().is_some_and(|either| {
				either
					.as_ref()
					.left()
					.is_some_and(|output| output.outputs_complete)
			});
			if !outputs_complete {
				if let Some(output_objects) = data.output.as_ref().map(tg::value::Data::children) {
					objects.extend(output_objects);
				}
			}
		}
		let self_object_count_and_weight = objects
			.iter()
			.map(|object| async {
				let output = self
					.export_inner_object(Some(Either::Left(process)), object, graph, event_sender)
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
						event_sender,
						arg.clone(),
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

		let process_count = stats.process_count.map_or_else(
			|| 1 + children_process_count,
			|process_count| process_count - 1 - children_process_count,
		);
		let object_count = stats.object_count.map_or_else(
			|| self_object_count + children_object_count,
			|object_count| object_count - self_object_count - children_object_count,
		);
		let object_weight = stats.object_weight.map_or_else(
			|| self_object_weight + children_object_weight,
			|object_weight| object_weight - self_object_weight - children_object_weight,
		);

		Ok(InnerProcessOutput {
			process_count,
			object_count,
			object_weight,
		})
	}

	fn export_sync_inner_process(
		&self,
		state: &mut StateSync,
		arg: &tg::export::Arg,
		parent: Option<&Either<&tg::process::Id, &tg::object::Id>>,
		process: &tg::process::Id,
	) -> tg::Result<InnerProcessOutput> {
		// Update the complete graph.
		let len = state.import_complete_receiver.len();
		let mut buffer = Vec::with_capacity(len);
		state
			.import_complete_receiver
			.blocking_recv_many(&mut buffer, len);
		for complete in buffer {
			match &complete {
				tg::import::Complete::Process(process_complete) => {
					state.graph.update_complete(&Either::Left(process_complete));
				},
				tg::import::Complete::Object(object_complete) => {
					state.graph.update_complete(&Either::Right(object_complete));
				},
			}
		}

		// Get the process
		let data = Self::try_get_process_local_sync(&state.database, process)?
			.ok_or_else(|| tg::error!("failed to find the process"))?;
		let process_complete = Self::export_sync_get_process_complete(state, process)?;

		// Return an error if the process is not finished.
		if !data.status.is_finished() {
			return Err(tg::error!(%process, "process is not finished"));
		}

		// Get the stats.
		let stats = Self::export_sync_get_process_stats(state, arg, &data, &process_complete)?;

		if !state.graph.insert(parent, &Either::Left(process)) {
			return Ok(InnerProcessOutput {
				process_count: stats.process_count.unwrap_or(1),
				object_count: stats.object_count.unwrap_or(0),
				object_weight: stats.object_weight.unwrap_or(0),
			});
		}

		// Send the process if it is not complete.
		if state.graph.is_complete(&Either::Left(process)) {
			state
				.event_sender
				.blocking_send(Ok(tg::export::Event::Complete(
					tg::export::Complete::Process(process_complete.clone()),
				)))
				.map_err(|source| {
					tg::error!(!source, "failed to send export process complete event")
				})?;
		} else {
			let item = tg::export::Item::Process(tg::export::ProcessItem {
				id: process.clone(),
				data: data.clone(),
			});
			state
				.event_sender
				.blocking_send(Ok(tg::export::Event::Item(item)))
				.map_err(|source| tg::error!(!source, "failed to send"))?;
		}

		// Get the children.
		let children = data.children.clone().unwrap_or_default();

		// Handle the command, log, and output.
		let current_output = state.graph.get_complete(&Either::Left(process));

		let mut objects: Vec<tg::object::Id> = Vec::new();
		if arg.commands {
			let commands_complete = current_output.as_ref().is_some_and(|either| {
				either
					.as_ref()
					.left()
					.is_some_and(|output| output.commands_complete)
			});
			if !commands_complete {
				objects.push(data.command.clone().into());
			}
		}
		if arg.outputs {
			let outputs_complete = current_output.as_ref().is_some_and(|either| {
				either
					.as_ref()
					.left()
					.is_some_and(|output| output.outputs_complete)
			});
			if !outputs_complete {
				if let Some(output_objects) = data.output.as_ref().map(tg::value::Data::children) {
					objects.extend(output_objects);
				}
			}
		}

		let self_object_count_and_weight: Vec<_> = objects
			.iter()
			.map(|object| {
				let output =
					self.export_sync_inner_object(state, Some(&Either::Left(process)), object)?;
				Ok::<_, tg::Error>((output.count, output.weight))
			})
			.try_collect()?;
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
			let outputs: Vec<_> = children
				.iter()
				.map(|child| {
					self.export_sync_inner_process(state, arg, Some(&Either::Left(process)), child)
				})
				.try_collect()?;
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

		let process_count = stats.process_count.map_or_else(
			|| 1 + children_process_count,
			|process_count| process_count - 1 - children_process_count,
		);
		let object_count = stats.object_count.map_or_else(
			|| self_object_count + children_object_count,
			|object_count| object_count - self_object_count - children_object_count,
		);
		let object_weight = stats.object_weight.map_or_else(
			|| self_object_weight + children_object_weight,
			|object_weight| object_weight - self_object_weight - children_object_weight,
		);

		Ok(InnerProcessOutput {
			process_count,
			object_count,
			object_weight,
		})
	}

	async fn export_inner_object(
		&self,
		parent: Option<Either<&tg::process::Id, &tg::object::Id>>,
		object: &tg::object::Id,
		graph: &Graph,
		event_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::export::Event>>,
	) -> tg::Result<InnerObjectOutput> {
		// Get the object.
		let tg::object::get::Output { bytes, .. } = self
			.get_object(object)
			.await
			.map_err(|source| tg::error!(!source, %object, "failed to get the object"))?;
		let object_complete = self
			.export_get_object_complete(object)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					?object,
					"failed to locate the object to check count and weight"
				)
			})?;
		let data = tg::object::Data::deserialize(object.kind(), bytes.clone())?;
		let size = bytes.len().to_u64().unwrap();

		// If the object has already been sent or is complete, then update the progress and return.
		let inserted = graph.insert(parent.as_ref(), &Either::Right(object));
		let is_complete = graph.is_complete(&Either::Right(object));
		if is_complete {
			event_sender
				.send(Ok(tg::export::Event::Complete(
					tg::export::Complete::Object(object_complete.clone()),
				)))
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to send export object complete event")
				})?;
		}

		if !inserted || is_complete {
			let count = object_complete.count.unwrap_or(1);
			let weight = object_complete.weight.unwrap_or(size);
			let output = InnerObjectOutput { count, weight };
			return Ok(output);
		}

		// Send the object.
		let item = tg::export::Item::Object(tg::export::ObjectItem {
			id: object.clone(),
			bytes: bytes.clone(),
		});
		event_sender
			.send(Ok(tg::export::Event::Item(item)))
			.await
			.map_err(|source| tg::error!(!source, "failed to send"))?;

		// Recurse into the children.
		let (children_count, children_weight) = data
			.children()
			.iter()
			.map(|child| {
				let server = self.clone();
				async move {
					let output = server
						.export_inner_object(
							Some(Either::Right(object)),
							child,
							graph,
							event_sender,
						)
						.await?;
					Ok::<_, tg::Error>(output)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?
			.into_iter()
			.fold((0, 0), |a, b| (a.0 + b.count, a.1 + b.weight));

		// Create the output.
		let count = object_complete.count.unwrap_or_else(|| 1 + children_count);
		let weight = object_complete
			.weight
			.unwrap_or_else(|| size + children_weight);
		let output = InnerObjectOutput { count, weight };

		Ok(output)
	}

	fn export_sync_inner_object(
		&self,
		state: &mut StateSync,
		parent: Option<&Either<&tg::process::Id, &tg::object::Id>>,
		object: &tg::object::Id,
	) -> tg::Result<InnerObjectOutput> {
		// Update the complete graph.
		let len = state.import_complete_receiver.len();
		let mut buffer = Vec::with_capacity(len);
		state
			.import_complete_receiver
			.blocking_recv_many(&mut buffer, len);
		for complete in buffer {
			match &complete {
				tg::import::Complete::Process(process_complete) => {
					state.graph.update_complete(&Either::Left(process_complete));
				},
				tg::import::Complete::Object(object_complete) => {
					state.graph.update_complete(&Either::Right(object_complete));
				},
			}
		}

		// Get the object.
		let bytes = self
			.try_get_object_sync(object, &mut state.file)?
			.ok_or_else(|| tg::error!("failed to find the object"))?
			.bytes;

		// Get the count/weight.
		let object_complete = Self::export_sync_get_object_complete(state, object)?;

		// Get the data.
		let data = tg::object::Data::deserialize(object.kind(), bytes.clone())?;

		// Get the size.
		let size = bytes.len().to_u64().unwrap();

		// If the object has already been sent or is complete, then update the progress and return.
		let inserted = state.graph.insert(parent, &Either::Right(object));
		let is_complete = state.graph.is_complete(&Either::Right(object));
		if is_complete {
			state
				.event_sender
				.blocking_send(Ok(tg::export::Event::Complete(
					tg::export::Complete::Object(object_complete.clone()),
				)))
				.map_err(|source| tg::error!(!source, "failed to send"))?;
		}

		if !inserted || is_complete {
			let count = object_complete.count.unwrap_or(1);
			let weight = object_complete.weight.unwrap_or(size);
			let output = InnerObjectOutput { count, weight };
			return Ok(output);
		}

		// Send the object.
		let item = tg::export::Item::Object(tg::export::ObjectItem {
			id: object.clone(),
			bytes: bytes.clone(),
		});
		state
			.event_sender
			.blocking_send(Ok(tg::export::Event::Item(item)))
			.map_err(|source| tg::error!(!source, "failed to send"))?;

		// Recurse into the children.
		let children: Vec<_> = data
			.children()
			.iter()
			.map(move |child| {
				self.export_sync_inner_object(state, Some(&Either::Right(object)), child)
			})
			.try_collect()?;

		let (children_count, children_weight) = children
			.into_iter()
			.fold((0, 0), |a, b| (a.0 + b.count, a.1 + b.weight));

		// Create the output.
		let count = object_complete.count.unwrap_or_else(|| 1 + children_count);
		let weight = object_complete
			.weight
			.unwrap_or_else(|| size + children_weight);
		let output = InnerObjectOutput { count, weight };

		Ok(output)
	}

	async fn export_get_process_complete(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<tg::export::ProcessComplete> {
		// Get an index connection.
		let connection = self
			.index
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the object metadata.
		#[derive(serde::Deserialize)]
		struct Row {
			commands_count: Option<u64>,
			commands_weight: Option<u64>,
			count: Option<u64>,
			outputs_count: Option<u64>,
			outputs_weight: Option<u64>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					commands_count,
					commands_weight,
					count,
					outputs_count,
					outputs_weight
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Create the output.
		let output = tg::export::ProcessComplete {
			commands_count: row.as_ref().and_then(|row| row.commands_count),
			commands_weight: row.as_ref().and_then(|row| row.commands_weight),
			count: row.as_ref().and_then(|row| row.count),
			id: id.clone(),
			outputs_count: row.as_ref().and_then(|row| row.outputs_count),
			outputs_weight: row.as_ref().and_then(|row| row.outputs_weight),
		};

		Ok(output)
	}

	fn export_sync_get_process_complete(
		state: &mut StateSync,
		id: &tg::process::Id,
	) -> tg::Result<tg::export::ProcessComplete> {
		let metadata = Self::try_get_process_metadata_local_sync(&state.index, id)?
			.ok_or_else(|| tg::error!("failed to find the process"))?;
		let complete = tg::export::ProcessComplete {
			commands_count: metadata.commands_count,
			commands_weight: metadata.commands_weight,
			count: metadata.count,
			id: id.clone(),
			outputs_count: metadata.outputs_count,
			outputs_weight: metadata.outputs_weight,
		};
		Ok(complete)
	}

	async fn export_get_object_complete(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<tg::export::ObjectComplete> {
		let metadata = self.try_get_object_metadata(id).await?;
		let output = tg::export::ObjectComplete {
			id: id.clone(),
			count: metadata.as_ref().and_then(|metadata| metadata.count),
			weight: metadata.as_ref().and_then(|metadata| metadata.weight),
		};
		Ok(output)
	}

	fn export_sync_get_object_complete(
		state: &mut StateSync,
		id: &tg::object::Id,
	) -> tg::Result<tg::export::ObjectComplete> {
		let metadata = Self::try_get_object_metadata_local_sync(&state.index, id)?;
		let complete = tg::export::ObjectComplete {
			count: metadata.as_ref().and_then(|metadata| metadata.count),
			id: id.clone(),
			weight: metadata.as_ref().and_then(|metadata| metadata.weight),
		};
		Ok(complete)
	}

	async fn export_get_process_stats(
		src: &impl tg::Handle,
		arg: &tg::export::Arg,
		data: &tg::process::Data,
		process_complete: &tg::export::ProcessComplete,
	) -> tg::Result<ProcessStats> {
		let process_count = if arg.recursive {
			process_complete.count
		} else {
			Some(1)
		};
		let (object_count, object_weight) = if arg.recursive {
			let commands_count = if arg.commands {
				process_complete.commands_count
			} else {
				Some(0)
			};
			let outputs_count = if arg.outputs {
				process_complete.outputs_count
			} else {
				Some(0)
			};
			let count = std::iter::empty()
				.chain(Some(commands_count))
				.chain(Some(outputs_count))
				.sum::<Option<u64>>();
			let commands_weight = if arg.commands {
				process_complete.commands_weight
			} else {
				Some(0)
			};
			let outputs_weight = if arg.outputs {
				process_complete.outputs_weight
			} else {
				Some(0)
			};
			let weight = std::iter::empty()
				.chain(Some(commands_weight))
				.chain(Some(outputs_weight))
				.sum::<Option<u64>>();
			(count, weight)
		} else {
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
			let (output_count, output_weight) = if arg.outputs {
				if data.status.is_finished() {
					let metadata: Vec<Option<tangram_client::object::Metadata>> = data
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
			let count = std::iter::empty()
				.chain(Some(command_count))
				.chain(Some(output_count))
				.sum::<Option<u64>>();
			let weight = std::iter::empty()
				.chain(Some(command_weight))
				.chain(Some(output_weight))
				.sum::<Option<u64>>();
			(count, weight)
		};
		Ok(ProcessStats {
			process_count,
			object_count,
			object_weight,
		})
	}

	fn export_sync_get_process_stats(
		state: &mut StateSync,
		arg: &tg::export::Arg,
		data: &tg::process::Data,
		process_complete: &tg::export::ProcessComplete,
	) -> tg::Result<ProcessStats> {
		let process_count = if arg.recursive {
			process_complete.count
		} else {
			Some(1)
		};
		let (object_count, object_weight) = if arg.recursive {
			// If the push is recursive, then use the command's and outputs' counts and weights.
			let commands_count = if arg.commands {
				process_complete.commands_count
			} else {
				Some(0)
			};
			let outputs_count = if arg.outputs {
				process_complete.outputs_count
			} else {
				Some(0)
			};
			let count = std::iter::empty()
				.chain(Some(commands_count))
				.chain(Some(outputs_count))
				.sum::<Option<u64>>();
			let commands_weight = if arg.commands {
				process_complete.commands_weight
			} else {
				Some(0)
			};
			let outputs_weight = if arg.outputs {
				process_complete.outputs_weight
			} else {
				Some(0)
			};
			let weight = std::iter::empty()
				.chain(Some(commands_weight))
				.chain(Some(outputs_weight))
				.sum::<Option<u64>>();
			(count, weight)
		} else {
			// If the push is not recursive, then use the count and weight of the command and output.
			let (command_count, command_weight) = {
				let metadata = Self::get_object_metadata_local_sync(
					&state.index,
					&data.command.clone().into(),
				)?;
				(metadata.count, metadata.weight)
			};
			let (output_count, output_weight) = if arg.outputs {
				if data.status.is_finished() {
					let metadata: Vec<tangram_client::object::Metadata> = data
						.output
						.as_ref()
						.map(tg::value::Data::children)
						.iter()
						.flatten()
						.map(|child| Self::get_object_metadata_local_sync(&state.index, child))
						.try_collect()?;
					let count = metadata
						.iter()
						.map(|metadata| metadata.count)
						.sum::<Option<u64>>();
					let weight = metadata
						.iter()
						.map(|metadata| metadata.weight)
						.sum::<Option<u64>>();
					(count, weight)
				} else {
					(Some(0), Some(0))
				}
			} else {
				(Some(0), Some(0))
			};
			let count = std::iter::empty()
				.chain(Some(command_count))
				.chain(Some(output_count))
				.sum::<Option<u64>>();
			let weight = std::iter::empty()
				.chain(Some(command_weight))
				.chain(Some(output_weight))
				.sum::<Option<u64>>();
			(count, weight)
		};
		Ok(ProcessStats {
			process_count,
			object_count,
			object_weight,
		})
	}

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

		// Get the accept encoding header.
		let accept_encoding = request
			.parse_header::<AcceptEncoding, _>(http::header::ACCEPT_ENCODING)
			.transpose()?;

		// Get the stop signal.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		// Create the request stream.
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

		// Create the response stream.
		let stream = handle.export(arg, stream).await?;

		// Stop the outgoing stream when the server stops.
		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);

		// Create the response body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::APPLICATION, mime::OCTET_STREAM)) => {
				let content_type = Some(mime::APPLICATION_OCTET_STREAM);
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
				let body = Body::with_stream(stream);
				(content_type, body)
			},
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Compress the response body if accepted.
		let (content_encoding, body) = if accept_encoding.is_some_and(|accept_encoding| {
			accept_encoding
				.preferences
				.iter()
				.any(|preference| preference.encoding == ContentEncoding::Zstd)
		}) {
			let body = Body::with_reader(ZstdEncoder::new(body.into_reader()));
			(Some(ContentEncoding::Zstd), body)
		} else {
			(None, body)
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_encoding) = content_encoding {
			response =
				response.header(http::header::CONTENT_ENCODING, content_encoding.to_string());
		}
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
		parent: Option<&Either<&tg::process::Id, &tg::object::Id>>,
		item: &Either<&tg::process::Id, &tg::object::Id>,
	) -> bool {
		let item = item.cloned();

		// Check if the node already exists.
		if self.nodes.contains_key(&item) {
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
			let parent = parent.cloned();
			// Add the new node as a child of the parent.
			if let Some(parent_node) = self.nodes.get(&parent) {
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
				tracing::error!("parent not found");
			}
		}

		// Insert the new node.
		self.nodes.insert(item, new_node);

		true
	}

	fn is_complete(&self, item: &Either<&tg::process::Id, &tg::object::Id>) -> bool {
		let item = item.cloned();
		let Some(node) = self.nodes.get(&item) else {
			return false;
		};

		// If this node is complete, return true.
		if node.is_complete() {
			return true;
		}

		// Track a list of nodes to a specific ancestor.
		let mut completion_path = Vec::new();
		let found_complete_ancestor = Self::check_ancestor_completion(&node, &mut completion_path);

		// If we found a path to complete, mark intermediate nodes as complete.
		if found_complete_ancestor {
			for node in completion_path {
				match &node.complete {
					Either::Left(process_complete) => {
						process_complete.write().unwrap().complete = true;
					},
					Either::Right(object_complete) => {
						object_complete.store(true, std::sync::atomic::Ordering::SeqCst);
					},
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
				tracing::error!("failed to upgrade weak pointer");
			}
		}

		false
	}

	fn get_complete(
		&self,
		item: &Either<&tg::process::Id, &tg::object::Id>,
	) -> Option<Either<ProcessComplete, bool>> {
		let item = item.cloned();
		let node = self.nodes.get(&item)?;
		let output = match &node.complete {
			Either::Left(process_complete) => {
				Either::Left(process_complete.read().unwrap().clone())
			},
			Either::Right(object_complete) => {
				Either::Right(object_complete.load(std::sync::atomic::Ordering::SeqCst))
			},
		};
		Some(output)
	}

	fn update_complete(
		&self,
		output: &Either<&tg::import::ProcessComplete, &tg::import::ObjectComplete>,
	) {
		let (item, new_complete) = match output {
			Either::Left(process) => (
				Either::Left(process.id.clone()),
				Either::Left(ProcessComplete::from((*process).clone())),
			),
			Either::Right(object) => (Either::Right(object.id.clone()), Either::Right(true)),
		};
		if let Some(node) = self.nodes.get(&item) {
			match (&node.complete, new_complete) {
				(Either::Left(old_complete), Either::Left(new_complete)) => {
					let mut w = old_complete.write().unwrap();
					*w = new_complete.clone();
				},
				(Either::Right(old_complete), Either::Right(value)) => {
					old_complete.store(value, std::sync::atomic::Ordering::SeqCst);
				},
				_ => {
					tracing::error!("attempted to update complete with mismatched type");
				},
			}
		} else {
			self.insert(None, &item.as_ref());
			let Some(node) = self.nodes.get(&item) else {
				tracing::error!(?item, "failed to get node after insertion");
				return;
			};
			match (&node.complete, new_complete) {
				(Either::Left(old), Either::Left(new)) => {
					*old.write().unwrap() = new.clone();
				},
				(Either::Right(old), Either::Right(value)) => {
					old.store(value, std::sync::atomic::Ordering::SeqCst);
				},
				_ => {
					tracing::error!("attempted to update complete with mismatched type");
				},
			}
		}
	}
}

impl Node {
	fn object() -> Self {
		Self {
			parents: RwLock::new(SmallVec::new()),
			children: RwLock::new(Vec::new()),
			complete: Either::Right(AtomicBool::new(false)),
		}
	}

	fn process() -> Self {
		Self {
			parents: RwLock::new(SmallVec::new()),
			children: RwLock::new(Vec::new()),
			complete: Either::Left(RwLock::new(ProcessComplete::new())),
		}
	}

	fn is_complete(&self) -> bool {
		match &self.complete {
			Either::Left(process_complete) => process_complete.read().unwrap().complete,
			Either::Right(object_complete) => {
				object_complete.load(std::sync::atomic::Ordering::SeqCst)
			},
		}
	}
}

impl ProcessComplete {
	fn new() -> Self {
		Self {
			commands_complete: false,
			complete: false,
			outputs_complete: false,
		}
	}
}

impl From<tg::import::ProcessComplete> for ProcessComplete {
	fn from(value: tg::import::ProcessComplete) -> Self {
		let tg::import::ProcessComplete {
			commands_complete,
			complete,
			outputs_complete,
			..
		} = value;
		Self {
			commands_complete,
			complete,
			outputs_complete,
		}
	}
}
