use {
	crate::Server,
	bytes::Bytes,
	futures::stream::{StreamExt as _, TryStreamExt as _},
	std::pin::pin,
	tangram_client::{self as tg, Handle as _, handle::Process},
	tangram_messenger::{Consumer as _, Messenger as _, Stream as _},
};

pub mod get;
pub mod post;
pub mod reader;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Message {
	Compact(tg::process::Id),
}

impl Server {
	pub(crate) async fn log_compaction_task(&self) -> tg::Result<()> {
		// Get or create the stream.
		let config = tangram_messenger::StreamConfig::default();
		let stream = self
			.messenger
			.get_or_create_stream("process.log_compaction".into(), config)
			.await
			.map_err(|source| tg::error!(!source, "failed to get stream"))?;

		// Create the consumer.
		let config = tangram_messenger::ConsumerConfig::default();
		let consumer = stream
			.create_consumer(format!("logs"), config)
			.await
			.map_err(|source| tg::error!(!source, "failed to create consumer"))?;

		// Subscribe to the stream.
		let messages = consumer
			.subscribe()
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the stream"))?;

		// Handle messages.
		let mut messages = pin!(messages);
		while let Some(message) = messages
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to receive the message"))?
		{
			// Deserialize the message.
			let (message, acker) = message.split();
			let message = serde_json::from_slice(&message)
				.map_err(|source| tg::error!(!source, "failed to deserialize the message"))?;
			match message {
				Message::Compact(process) => {
					// Get or spawn the task.
					let server = self.clone();
					let task = self.log_compaction_tasks.get_or_spawn(
						process.clone(),
						async move |_stop| {
							server
								.compact_logs(&process)
								.await
								.inspect_err(
									|error| tracing::error!(?error, process = %process, "failed to compact the process logs"),
								)
								.ok();
						},
					);

					// Spawn a task to ack the message when the compaction task completes.
					tokio::spawn(async move {
						task.wait().await.ok();
						acker
							.ack()
							.await
							.inspect_err(|error| tracing::error!(?error, "failed to ack message"))
							.ok();
					});
				},
			}
		}
		Ok(())
	}

	pub(crate) async fn wait_for_log_compaction(&self, id: &tg::process::Id) -> tg::Result<()> {
		// Get the process data.
		let data = self
			.try_get_process_local(id)
			.await?
			.ok_or_else(|| tg::error!(process = %id, "process is not local"))?
			.data;

		// Log is a blob, exit early.
		if data.log.is_some() {
			return Ok(());
		};

		// Process isn't finished, error.
		if !data.status.is_finished() {
			return Err(tg::error!(process = %id, "expected the process to be finished"));
		}

		// Publish a notification that we want the log to be compacted.
		let message = Message::Compact(id.clone());
		let message = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		let _published = self
			.messenger
			.stream_publish(format!("processes.log_compaction"), message.into())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		// Wait for the notification that the log has been compacted.
		let _message = self
			.messenger
			.subscribe(format!("processes.{id}.log_compacted"), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the stream"))?
			.next()
			.await;

		Ok(())
	}

	pub(crate) async fn compact_logs(&self, id: &tg::process::Id) -> tg::Result<()> {
		// Get the process data.
		let mut data = self
			.try_get_process_local(id)
			.await?
			.ok_or_else(|| tg::error!(process = %id, "process is not local"))?
			.data;
		if !data.status.is_finished() {
			return Err(tg::error!(process = %id, "cannot compact logs of an unfinished process"));
		}

		// Bail out if the log is already a blob.
		if data.log.is_some() {
			return Ok(());
		}

		// Open the log file.
		let path = self.logs_path().join(id.to_string());
		let file = match tokio::fs::File::open(&path).await {
			Ok(file) => file,
			Err(error) if matches!(error.kind(), std::io::ErrorKind::NotFound) => {
				tracing::warn!(path = %path.display(), "could not find log file for compaction");
				return Ok(());
			},
			Err(source) => {
				return Err(
					tg::error!(!source, process = %id, path = %path.display(), "failed to open log file"),
				);
			},
		};

		// Create the blob.
		let blob = self
			.write(file)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the blob"))?
			.blob;

		// Update the process data.
		data.log.replace(blob);

		// Update the process.
		self.put_process(id, tg::process::put::Arg { data })
			.await
			.map_err(|source| tg::error!(!source, process = %id, "failed to put the process"))?;

		// Publish a message to notify waiters that the log has been compacted.
		self.messenger
			.publish(format!("processes.{id}.log_compacted"), Bytes::new())
			.await
			.map_err(|source| tg::error!(!source, "published message"))?;

		Ok(())
	}
}
