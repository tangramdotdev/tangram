use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _},
	num::ToPrimitive as _,
	std::{mem, pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
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
	pub(super) id: tg::process::Id,
	pub(super) receiver: tokio::sync::mpsc::Receiver<Request>,
	pub(super) sender: super::ProcessControlSender,
}

struct Pending {
	entry: log::put::Arg,
	id: String,
	position: u64,
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
			id,
			receiver,
			sender,
		} = arg;
		let requests = tokio_stream::StreamExt::chunks_timeout(
			ReceiverStream::new(receiver),
			LOG_BATCH_MAX_CHUNKS,
			LOG_BATCH_DELAY,
		);
		let mut requests = pin!(requests);
		while let Some(requests) = requests.next().await {
			self.write_process_control_request_batch(&id, requests, &sender)
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
	) -> tg::Result<()> {
		let indexed = self
			.get_process_from_index(id)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process"));
		let mut data = match indexed.and_then(|indexed| {
			indexed
				.data
				.ok_or_else(|| tg::error!(%id, "missing the process data"))
		}) {
			Ok(data) => data,
			Err(error) => {
				for request in requests {
					Self::send_process_control_write_response(
						sender,
						request.id,
						Err(error.clone()),
					)
					.await?;
				}

				return Ok(());
			},
		};
		let mut batch = Vec::new();
		let mut batch_length = 0_usize;
		for request in requests {
			let Request {
				arg,
				id: request_id,
			} = request;
			if arg.chunk.bytes.is_empty() {
				self.flush_process_control_write_batch(id, &mut batch, sender)
					.await?;
				batch_length = 0;
				let result = self
					.finish_process_control_log_stream(id, &mut data, arg)
					.await;
				Self::send_process_control_write_response(sender, request_id, result).await?;
				continue;
			}
			let length = arg.chunk.bytes.len();
			if !batch.is_empty() && batch_length.saturating_add(length) > LOG_BATCH_SIZE {
				self.flush_process_control_write_batch(id, &mut batch, sender)
					.await?;
				batch_length = 0;
			}
			let result = Self::create_process_control_log_entry(id, &data, arg);
			match result {
				Ok((entry, position)) => {
					batch.push(Pending {
						entry,
						id: request_id,
						position,
					});
					batch_length += length;
				},
				Err(error) => {
					self.flush_process_control_write_batch(id, &mut batch, sender)
						.await?;
					batch_length = 0;
					Self::send_process_control_write_response(sender, request_id, Err(error))
						.await?;
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
			.map(|pending| (pending.entry, (pending.id, pending.position)))
			.unzip();
		let result = self.put_process_log_batch_local(id, entries).await;
		for (request_id, position) in responses {
			let result = match &result {
				Ok(()) => Ok(tg::process::control::WriteServerResponseOutput { position }),
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

	async fn finish_process_control_log_stream(
		&self,
		id: &tg::process::Id,
		data: &mut tg::process::Data,
		arg: tg::process::control::WriteClientRequestArg,
	) -> tg::Result<tg::process::control::WriteServerResponseOutput> {
		let failed = arg.failed;
		let chunk = arg.chunk;
		let (finished, stdio) = match chunk.stream {
			tg::process::stdio::Stream::Stderr => (data.stderr_finished, &data.stderr),
			tg::process::stdio::Stream::Stdin => {
				return Err(tg::error!(
					"cannot write process stdin over process control"
				));
			},
			tg::process::stdio::Stream::Stdout => (data.stdout_finished, &data.stdout),
		};
		if !stdio.is_log() {
			return Err(tg::error!(stream = %chunk.stream, "the process stream is not a log"));
		}
		if !data.status.is_finished() {
			return Err(tg::error!("the process is not finished"));
		}
		let position = chunk.combined_position;
		if finished && (!failed || data.log_failed) {
			let output = tg::process::control::WriteServerResponseOutput { position };

			return Ok(output);
		}

		let mut next = data.clone();
		if failed {
			next.log_failed = true;
			next.stderr_finished |= next.stderr.is_log();
			next.stdout_finished |= next.stdout.is_log();
		} else {
			match chunk.stream {
				tg::process::stdio::Stream::Stderr => next.stderr_finished = true,
				tg::process::stdio::Stream::Stdin => unreachable!(),
				tg::process::stdio::Stream::Stdout => next.stdout_finished = true,
			}
		}
		let streams = if failed {
			[
				next.stderr
					.is_log()
					.then_some(tg::process::stdio::Stream::Stderr),
				next.stdout
					.is_log()
					.then_some(tg::process::stdio::Stream::Stdout),
			]
		} else {
			[Some(chunk.stream), None]
		};
		self.put_process_log_finished_local(id, next.clone())
			.await?;
		*data = next;
		self.server.log_notifications.notify(id);
		for stream in streams.into_iter().flatten() {
			self.server
				.spawn_publish_process_stdio_close_message_task(id, stream);
		}
		let output = tg::process::control::WriteServerResponseOutput { position };

		Ok(output)
	}

	fn create_process_control_log_entry(
		id: &tg::process::Id,
		data: &tg::process::Data,
		arg: tg::process::control::WriteClientRequestArg,
	) -> tg::Result<(log::put::Arg, u64)> {
		let failed = arg.failed;
		let chunk = arg.chunk;
		let (finished, stdio) = match chunk.stream {
			tg::process::stdio::Stream::Stderr => (data.stderr_finished, &data.stderr),
			tg::process::stdio::Stream::Stdin => {
				return Err(tg::error!(
					"cannot write process stdin over process control"
				));
			},
			tg::process::stdio::Stream::Stdout => (data.stdout_finished, &data.stdout),
		};
		if !stdio.is_log() {
			return Err(tg::error!(stream = %chunk.stream, "the process stream is not a log"));
		}
		if failed {
			return Err(tg::error!("a log failure requires an empty chunk"));
		}
		if finished {
			return Err(tg::error!(stream = %chunk.stream, "the process log stream is finished"));
		}
		let length = chunk.bytes.len().to_u64().unwrap();
		let position = chunk
			.combined_position
			.checked_add(length)
			.ok_or_else(|| tg::error!("the log position is too large"))?;
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

		Ok((entry, position))
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
