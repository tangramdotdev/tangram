use {
	crate::{Context, Server},
	bytes::Bytes,
	futures::stream::StreamExt as _,
	num::ToPrimitive,
	std::io::Write,
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

#[derive(tangram_serialize::Serialize, tangram_serialize::Deserialize)]
enum LogQueueMessage {
	#[tangram_serialize(id = 0)]
	AppendChunk(AppendChunk),

	#[tangram_serialize(id = 1)]
	End,
}

#[derive(tangram_serialize::Serialize, tangram_serialize::Deserialize)]
struct AppendChunk {
	#[tangram_serialize(id = 0)]
	stream: tg::process::log::Stream,

	#[tangram_serialize(id = 1)]
	bytes: Bytes,
}

impl Server {
	pub(crate) async fn post_process_log_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::log::post::Arg,
	) -> tg::Result<()> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self.get_remote_client(remote).await?;
			let arg = tg::process::log::post::Arg {
				bytes: arg.bytes,
				local: None,
				remotes: None,
				stream: arg.stream,
			};
			client.post_process_log(id, arg).await?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Get the process data.
		let data = self
			.try_get_process_local(id)
			.await?
			.ok_or_else(|| tg::error!("not found"))?
			.data;

		// Verify the process is local and started.
		if data.status != tg::process::Status::Started {
			return Err(tg::error!("failed to find the process"));
		}

		// Send the message.
		let message = LogQueueMessage::AppendChunk(AppendChunk {
			stream: arg.stream,
			bytes: arg.bytes,
		});
		let payload = tangram_serialize::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?
			.into();
		self.messenger
			.publish(format!("processes.{id}.log.queue"), payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
		Ok(())
	}

	pub(crate) async fn finish_process_log(&self, id: &tg::process::Id) -> tg::Result<()> {
		let message = LogQueueMessage::End;
		let payload = tangram_serialize::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?
			.into();
		self.messenger
			.publish(format!("processes.{id}.log.queue"), payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
		Ok(())
	}

	pub(crate) async fn process_log_task(
		&self,
		id: &tg::process::Id,
		started_at: u64,
	) -> tg::Result<()> {
		// Subscribe to the stream.
		let mut stream = self
			.messenger
			.subscribe(format!("process.{id}.log.queue"), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to stream"))?
			.boxed();

		// Keep track of the log position.
		let mut position = 0;
		while let Some(message) = stream.next().await {
			let Ok(message) = tangram_serialize::from_slice(&message.payload)
				.inspect_err(|error| tracing::error!(?error, "failed to deserialize the message"))
			else {
				continue;
			};
			match message {
				LogQueueMessage::AppendChunk(message) => {
					// Write to the file.
					self.post_process_log_to_file(
						id,
						message.bytes,
						&mut position,
						started_at,
						message.stream,
					)
					.await
					.inspect_err(|error| tracing::error!(?error, "failed to write the log message"))
					.ok();

					// Send a notification now that the log has been committed.
					tokio::spawn({
						let server = self.clone();
						let id = id.clone();
						async move {
							server
								.messenger
								.publish(format!("processes.{id}.log"), Bytes::new())
								.await
								.inspect_err(|error| {
									tracing::error!(?error, "failed to publish the message");
								})
								.ok();
						}
					});
				},
				LogQueueMessage::End => break,
			}
		}

		let message = super::Message::Compact(id.clone());
		let message = serde_json::to_vec(&message)
			.map_err(|source| tg::error!(!source, "failed to serialize the message"))?;
		let _published = self
			.messenger
			.stream_publish("log_compaction".into(), message.into())
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	async fn post_process_log_to_file(
		&self,
		id: &tg::process::Id,
		bytes: Bytes,
		position: &mut u64,
		started_at: u64,
		stream: tg::process::log::Stream,
	) -> tg::Result<()> {
		// Open the file.
		let path = self.logs_path().join(id.to_string());
		let mut file = std::fs::File::options()
			.create(true)
			.append(true)
			.open(&path)
			.map_err(
				|source| tg::error!(!source, path = %path.display(), "failed to open the log file"),
			)?;

		// Compute the timestamp.
		let now = time::OffsetDateTime::now_utc()
			.unix_timestamp()
			.to_u64()
			.unwrap();
		let timestamp = now - started_at;

		// Create the chunk.
		let chunk = tg::process::log::get::Chunk {
			bytes,
			position: *position,
			stream,
			timestamp,
		};

		// Serialize the chunk.
		let mut json = serde_json::to_vec(&chunk)
			.map_err(|source| tg::error!(!source, "failed to serialize log chunk"))?;
		json.push(b'\n');

		tokio::task::spawn_blocking(move || {
			file.lock()
				.map_err(|source| tg::error!(!source, "failed to lock the file"))?;
			file.write_all(&json)
				.map_err(|source| tg::error!(!source, "failed to write to the file"))?;
			Ok::<_, tg::Error>(())
		})
		.await
		.map_err(|source| tg::error!(!source, "the task panicked"))?
		.map_err(|source| tg::error!(!source, "failed to write the log"))?;

		// Update the position.
		*position += chunk.bytes.len().to_u64().unwrap();

		Ok(())
	}

	pub(crate) async fn handle_post_process_log_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.json().await?;
		self.post_process_log_with_context(context, &id, arg)
			.await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
