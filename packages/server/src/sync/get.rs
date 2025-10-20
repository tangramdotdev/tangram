use {
	self::{
		graph::{Graph, NodeInner},
		progress::Progress,
	},
	crate::Server,
	futures::{prelude::*, stream::BoxStream},
	std::{
		collections::{BTreeSet, VecDeque},
		pin::pin,
		sync::{Arc, Mutex},
		time::Duration,
	},
	tangram_client as tg,
	tangram_either::Either,
	tangram_futures::task::Stop,
	tokio_util::task::AbortOnDropHandle,
};

mod graph;
mod index;
mod progress;
mod store;

impl Server {
	pub(super) async fn sync_get(
		&self,
		arg: tg::sync::Arg,
		mut stream: BoxStream<'static, tg::sync::Message>,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		// Create the progress.
		let progress = Arc::new(Progress::new());

		// Create the channels.
		let (process_index_sender, process_index_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::ProcessPutMessage>(256);
		let (object_index_sender, object_index_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::ObjectPutMessage>(256);
		let (process_store_sender, process_store_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::ProcessPutMessage>(256);
		let (object_store_sender, object_store_receiver) =
			tokio::sync::mpsc::channel::<tg::sync::ObjectPutMessage>(256);

		// Create the graph.
		let graph = Arc::new(Mutex::new(Graph::new()));

		// Spawn the incomplete task.
		let incomplete_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let arg = arg.clone();
			let sender = sender.clone();
			async move { server.sync_get_incomplete_task(arg, sender).await }
		}));

		// Spawn the index task.
		self.tasks.spawn(|_| {
			let server = self.clone();
			let graph = graph.clone();
			let sender = sender.clone();
			async move {
				let result = server
					.sync_get_index(graph, process_index_receiver, object_index_receiver, sender)
					.await;
				if let Err(error) = result {
					tracing::error!(?error, "the index task failed");
				}
			}
		});

		// Spawn the store task.
		let store_task = AbortOnDropHandle::new(tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				server
					.sync_get_store(
						graph,
						process_store_receiver,
						object_store_receiver,
						progress,
					)
					.await
			}
		}));

		// Spawn the progress task.
		let progress_stop = Stop::new();
		let progress_task = AbortOnDropHandle::new(tokio::spawn({
			let progress = progress.clone();
			let progress_stop = progress_stop.clone();
			let sender = sender.clone();
			async move {
				loop {
					let stop = progress_stop.wait();
					let stop = pin!(stop);
					let sleep = tokio::time::sleep(Duration::from_millis(100));
					let sleep = pin!(sleep);
					let result = future::select(sleep, stop).await;
					let message = progress.get();
					let message = tg::sync::Message::Progress(message);
					sender.send(Ok(message)).await.ok();
					if matches!(result, future::Either::Right(_)) {
						break;
					}
				}
			}
		}));

		// Read the messages from the stream and send them to the tasks.
		while let Some(message) = stream.next().await {
			let message = match message {
				tg::sync::Message::Put(Some(message)) => message,
				tg::sync::Message::Put(None) => {
					break;
				},
				_ => {
					unreachable!()
				},
			};
			match message {
				tg::sync::PutMessage::Process(message) => {
					future::try_join(
						process_index_sender.send(message.clone()),
						process_store_sender.send(message.clone()),
					)
					.await
					.ok();
				},
				tg::sync::PutMessage::Object(message) => {
					future::try_join(
						object_index_sender.send(message.clone()),
						object_store_sender.send(message.clone()),
					)
					.await
					.ok();
				},
			}
		}

		// Drop the store senders.
		drop(process_store_sender);
		drop(object_store_sender);

		// Await the incomplete and store.
		let (incomplete_result, store_result) =
			future::try_join(incomplete_task, store_task).await.unwrap();
		incomplete_result.and(store_result)?;

		// Drop the index senders.
		drop(process_index_sender);
		drop(object_index_sender);

		// Stop and await the progress task.
		progress_stop.stop();
		progress_task.await.unwrap();

		Ok(())
	}

	async fn sync_get_incomplete_task(
		&self,
		arg: tg::sync::Arg,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sync::Message>>,
	) -> tg::Result<()> {
		let mut queue = VecDeque::from(arg.get.unwrap_or_default());

		while let Some(item) = queue.pop_front() {
			let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
			match item {
				Either::Left(id) => {
					let output = self
						.try_touch_process_and_get_complete_and_metadata(&id, touched_at)
						.await?;
					if let Some((complete, _)) = output {
						let data = self
							.try_get_process_local(&id)
							.await?
							.ok_or_else(|| tg::error!("expected the process to exist"))?
							.data;
						if arg.commands && !complete.command {
							queue.push_back(Either::Right(data.command.clone().into()));
						}
						if (arg.outputs && !complete.output)
							&& let Some(output) = data.output
						{
							let mut children = BTreeSet::new();
							output.children(&mut children);
							queue.extend(children.into_iter().map(Either::Right));
						}
						if ((arg.recursive && !complete.children)
							|| (arg.commands && !complete.children_commands)
							|| (arg.outputs && !complete.children_outputs))
							&& let Some(children) = data.children
						{
							for referent in children {
								queue.push_back(Either::Left(referent.item));
							}
						}
					} else {
						let message = tg::sync::Message::Get(Some(tg::sync::GetMessage::Process(
							tg::sync::ProcessGetMessage { id: id.clone() },
						)));
						sender.send(Ok(message)).await.map_err(|source| {
							tg::error!(!source, "failed to send the get message")
						})?;
					}
				},

				Either::Right(id) => {
					let output = self
						.try_touch_object_and_get_complete_and_metadata(&id, touched_at)
						.await?;
					if let Some((complete, _)) = output {
						if !complete {
							let bytes = self
								.try_get_object_local(&id)
								.await?
								.ok_or_else(|| tg::error!("expected the object to exist"))?
								.bytes;
							let object = tg::object::Data::deserialize(id.kind(), bytes)?;
							let mut children = BTreeSet::new();
							object.children(&mut children);
							queue.extend(children.into_iter().map(Either::Right));
						}
					} else {
						let message = tg::sync::Message::Get(Some(tg::sync::GetMessage::Object(
							tg::sync::ObjectGetMessage { id: id.clone() },
						)));
						sender.send(Ok(message)).await.map_err(|source| {
							tg::error!(!source, "failed to send the get message")
						})?;
					}
				},
			}
		}

		sender
			.send(Ok(tg::sync::Message::Get(None)))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the last get message"))?;

		Ok(())
	}
}
