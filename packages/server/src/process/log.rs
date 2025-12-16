use {
	crate::Server,
	futures::stream::{StreamExt as _, TryStreamExt as _},
	indoc::formatdoc,
	std::pin::pin,
	tangram_client::{self as tg, Handle as _},
	tangram_database::{self as db, Database, Query},
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
			.get_or_create_stream("log_compaction".into(), config)
			.await
			.map_err(|source| tg::error!(!source, "failed to get stream"))?;

		// Create the consumer.
		let config = tangram_messenger::ConsumerConfig::default();
		let consumer = stream
			.create_consumer("logs".into(), config)
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
					// Compact the log.
					self.compact_log(&process)
						.await
						.inspect_err(
							|error| tracing::error!(?error, process = %process, "failed to compact the logs"),
						)
						.ok();

					// Spawn a task to ack the message when the compaction task completes.
					acker
						.ack()
						.await
						.inspect_err(|error| tracing::error!(?error, "failed to ack message"))
						.ok();
				},
			}
		}
		Ok(())
	}

	pub(crate) async fn wait_for_log_compaction(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<tg::blob::Id> {
		// If running in shared process mode, we need to make sure the log compaction task is configured else there's no guarantee that the log will ever be compacted!
		if self.config.advanced.shared_process && self.config.log_compaction.is_none() {
			return Err(tg::error!("expected log compaction to be configured"));
		}

		// Create a future to wait for the notification.
		let stream = self
			.messenger
			.subscribe(format!("process.{id}.log.compacted"), None)
			.await
			.map_err(|source| tg::error!(!source, "acknowledgement failed"))?;
		let mut stream = pin!(stream);
		let compacted = stream.next();

		// Notify the compaction task.
		let message = Message::Compact(id.clone());
		let message = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		self.messenger
			.stream_publish("log_compaction".to_owned(), message.into())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		// Wait for the reply.
		let response = compacted
			.await
			.ok_or_else(|| tg::error!("failed to wait for the log to be compacted"))?;
		serde_json::from_slice(&response.payload)
			.map_err(|source| tg::error!(!source, "failed to deserialize the response"))
	}

	pub(crate) async fn compact_log(&self, id: &tg::process::Id) -> tg::Result<()> {
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

		// Update the process.
		self.put_process_log(id, &blob).await?;

		// Publish a message to notify waiters that the log has been compacted.
		let payload = blob.to_bytes();
		self.messenger
			.publish(format!("processes.{id}.log_compacted"), payload)
			.await
			.map_err(|source| tg::error!(!source, "published message"))?;

		Ok(())
	}

	async fn put_process_log(&self, id: &tg::process::Id, log: &tg::blob::Id) -> tg::Result<()> {
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update processes
				set log = {p}2
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string(), log.to_string()];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to set the process log"))?;
		Ok(())
	}
}
