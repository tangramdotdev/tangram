use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, mem, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_index::prelude::*,
	tangram_store::{Store as _, log},
	tokio_stream::wrappers::ReceiverStream,
};

const LOG_BATCH_DELAY: Duration = Duration::from_millis(5);
const LOG_BATCH_MAX_CHUNKS: usize = 64;
const LOG_BATCH_SIZE: usize = 32 * 1024;

pub(super) struct Request {
	pub(super) arg: tg::process::control::WriteClientRequestArg,
	pub(super) id: String,
}

pub(super) struct RunProcessControlWriteTaskArg {
	pub(super) compacted: bool,
	pub(super) id: tg::process::Id,
	pub(super) receiver: tokio::sync::mpsc::Receiver<Request>,
	pub(super) sender: super::ProcessControlSender,
	pub(super) streams: BTreeSet<tg::process::stdio::Stream>,
}

struct Pending {
	entry: log::put::Arg,
	id: String,
}

impl Session {
	pub(super) fn spawn_process_control_write_task(
		&self,
		arg: RunProcessControlWriteTaskArg,
	) -> Task<tg::Result<()>> {
		let session = self.clone();
		Task::spawn(move |_| async move {
			let result = session.run_process_control_write_task(arg).boxed().await;
			if let Err(error) = &result {
				tracing::error!(error = %error.trace(), "the process control write task failed");
			}

			result
		})
	}

	async fn run_process_control_write_task(
		&self,
		arg: RunProcessControlWriteTaskArg,
	) -> tg::Result<()> {
		let RunProcessControlWriteTaskArg {
			compacted,
			id,
			receiver,
			sender,
			streams,
		} = arg;
		let requests = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(receiver),
			LOG_BATCH_MAX_CHUNKS,
			LOG_BATCH_DELAY,
		);
		let mut requests = pin!(requests);
		let mut ended = compacted
			|| streams.is_empty()
			|| self.server.store.try_get_log_end(&id).await?.is_some();
		while let Some(requests) = requests.next().await {
			self.write_process_control_request_batch(&id, requests, &sender, &mut ended, &streams)
				.boxed()
				.await?;
		}

		Ok(())
	}

	async fn write_process_control_request_batch(
		&self,
		id: &tg::process::Id,
		requests: Vec<Request>,
		sender: &super::ProcessControlSender,
		ended: &mut bool,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<()> {
		let mut batch = Vec::new();
		let mut batch_length = 0_usize;
		for request in requests {
			let Request {
				arg,
				id: request_id,
			} = request;
			match arg {
				tg::process::control::WriteClientRequestArg::Chunk(chunk) => {
					if *ended {
						let error = tg::error!("the process log has ended");
						Self::send_process_control_write_response(sender, request_id, Err(error))
							.await?;
						continue;
					}
					let length = chunk.bytes.len();
					if !batch.is_empty() && batch_length.saturating_add(length) > LOG_BATCH_SIZE {
						self.flush_process_control_write_batch(id, &mut batch, sender)
							.await?;
						batch_length = 0;
					}
					let result = Self::create_process_control_log_entry(id, streams, chunk);
					match result {
						Ok(entry) => {
							batch.push(Pending {
								entry,
								id: request_id,
							});
							batch_length += length;
						},
						Err(error) => {
							self.flush_process_control_write_batch(id, &mut batch, sender)
								.await?;
							batch_length = 0;
							Self::send_process_control_write_response(
								sender,
								request_id,
								Err(error),
							)
							.await?;
						},
					}
				},
				tg::process::control::WriteClientRequestArg::End(end) => {
					self.flush_process_control_write_batch(id, &mut batch, sender)
						.await?;
					batch_length = 0;
					let result = self.end_process_control_log(id, streams, end).await;
					if result.is_ok() {
						*ended = true;
					}
					Self::send_process_control_write_response(sender, request_id, result).await?;
				},
			}
		}
		self.flush_process_control_write_batch(id, &mut batch, sender)
			.await?;

		Ok(())
	}

	async fn flush_process_control_write_batch(
		&self,
		id: &tg::process::Id,
		batch: &mut Vec<Pending>,
		sender: &super::ProcessControlSender,
	) -> tg::Result<()> {
		if batch.is_empty() {
			return Ok(());
		}
		let (entries, responses): (Vec<_>, Vec<_>) = mem::take(batch)
			.into_iter()
			.map(|pending| {
				let length = pending.entry.bytes.len().to_u64().unwrap();
				(pending.entry, (pending.id, length))
			})
			.unzip();
		let result = self.put_process_log_batch_local(id, entries).await;
		for (request_id, length) in responses {
			let result = match &result {
				Ok(()) => Ok(tg::process::control::WriteServerResponseOutput {
					closed: false,
					length,
				}),
				Err(error) => Err(error.clone()),
			};
			Self::send_process_control_write_response(sender, request_id, result).await?;
		}

		Ok(())
	}

	async fn put_process_log_batch_local(
		&self,
		id: &tg::process::Id,
		args: Vec<log::put::Arg>,
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		self.server
			.store
			.put_log_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the log"))?;
		self.server.log_notifications.notify(id);

		Ok(())
	}

	async fn end_process_control_log(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
		end: tg::process::log::End,
	) -> tg::Result<tg::process::control::WriteServerResponseOutput> {
		if streams.is_empty() {
			return Err(tg::error!("the process does not have a log"));
		}
		if end.stderr_position.checked_add(end.stdout_position) != Some(end.position)
			|| (!streams.contains(&tg::process::stdio::Stream::Stderr) && end.stderr_position != 0)
			|| (!streams.contains(&tg::process::stdio::Stream::Stdout) && end.stdout_position != 0)
		{
			return Err(tg::error!("invalid log end positions"));
		}

		// Consult the index only for completion, never for a write batch.
		let data = self
			.get_process_from_index(id)
			.await?
			.data
			.ok_or_else(|| tg::error!(%id, "missing the process data"))?;
		if !data.status.is_finished() {
			return Err(tg::error!("the process is not finished"));
		}
		if data.log.is_some() {
			return Ok(tg::process::control::WriteServerResponseOutput {
				closed: true,
				length: 0,
			});
		}

		// Persist the writer's final positions before scheduling compaction or reporting success.
		if let Some(stored) = self.server.store.try_get_log_end(id).await? {
			if stored != end {
				return Err(tg::error!("the log end positions do not match"));
			}
		} else {
			let arg = log::end::Arg {
				end,
				process: id.clone(),
			};
			self.server
				.store
				.put_log_end(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to store the log end"))?;
		}
		self.server.index.enqueue_log_compaction(id).await.map_err(
			|error| tg::error!(!error, %id, "failed to enqueue the process log compaction"),
		)?;
		self.server.spawn_publish_log_compaction_notification_task();
		self.server.log_notifications.notify(id);
		for &stream in streams {
			self.server
				.spawn_publish_process_stdio_close_message_task(id, stream);
		}

		Ok(tg::process::control::WriteServerResponseOutput {
			closed: true,
			length: 0,
		})
	}

	fn create_process_control_log_entry(
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
		chunk: tg::process::stdio::Chunk,
	) -> tg::Result<log::put::Arg> {
		if !streams.contains(&chunk.stream) {
			return Err(tg::error!(stream = %chunk.stream, "the process stream is not a log"));
		}
		if chunk.bytes.is_empty() {
			return Err(tg::error!("the process log chunk is empty"));
		}
		let length = chunk.bytes.len().to_u64().unwrap();
		chunk
			.combined_position
			.checked_add(length)
			.ok_or_else(|| tg::error!("the log position is too large"))?;
		chunk
			.stream_position
			.checked_add(length)
			.ok_or_else(|| tg::error!("the stream log position is too large"))?;
		let timestamp = chunk
			.timestamp
			.ok_or_else(|| tg::error!("missing the log timestamp"))?;
		let entry = log::put::Arg {
			bytes: chunk.bytes,
			position: chunk.combined_position,
			process: id.clone(),
			stream: chunk.stream,
			stream_position: chunk.stream_position,
			timestamp,
		};

		Ok(entry)
	}

	async fn send_process_control_write_response(
		sender: &super::ProcessControlSender,
		id: String,
		result: tg::Result<tg::process::control::WriteServerResponseOutput>,
	) -> tg::Result<()> {
		let result = result.map(tg::process::control::ServerResponseOutput::Write);
		let response = Self::process_control_server_response(id, result);
		sender.send_low(response).await?;

		Ok(())
	}
}
