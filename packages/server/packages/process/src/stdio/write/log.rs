use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream::BoxStream},
	num::ToPrimitive as _,
	std::{collections::BTreeSet, mem},
	tangram_cache::log,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_index::prelude::*,
	tangram_server_core::Session,
	tg::process::stdio::write::{Data, Output},
	tokio_stream::wrappers::ReceiverStream,
};

type Sender = tokio::sync::mpsc::Sender<tg::Result<Output>>;

impl Component {
	pub(super) async fn try_write_process_stdio_log_local(
		&self,
		session: &Session,
		id: &tg::process::Id,
		streams: &[tg::process::stdio::Stream],
		input: BoxStream<'static, tg::Result<Data>>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<Output>>>> {
		// Only the process itself may write its logged output.
		match &session.context.principal {
			tg::Principal::Process(process) if process == id => (),
			_ => return Err(tg::error!("unauthorized")),
		}
		// The index can lag the process control start, so trust the authenticated process's streams until it catches up.
		let data = session
			.server
			.index
			.try_get_process_with_data(id)
			.await?
			.and_then(|(_, data)| data);
		let log_streams = match &data {
			Some(data) => [
				data.stderr
					.is_log()
					.then_some(tg::process::stdio::Stream::Stderr),
				data.stdout
					.is_log()
					.then_some(tg::process::stdio::Stream::Stdout),
			]
			.into_iter()
			.flatten()
			.collect::<BTreeSet<_>>(),
			None => streams.iter().copied().collect(),
		};
		if streams.iter().any(|stream| !log_streams.contains(stream)) {
			return Err(tg::error!("the process stream is not a log"));
		}
		// A log that has ended accepts only replays of the chunks before its end. A stored log ends after every chunk.
		let end = if let Some(end) = session.server.cache.try_get_log_end(id).await? {
			Some(end.position)
		} else if data.is_some_and(|data| data.log.is_some()) {
			Some(u64::MAX)
		} else {
			None
		};

		let capacity = session
			.server
			.config
			.process
			.stdio_log_batch
			.channel_capacity;
		let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
		let task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			let id = id.clone();
			move |_| async move {
				let result = component
					.write_process_stdio_log_local_task(
						&session,
						&id,
						log_streams,
						end,
						input,
						&sender,
					)
					.boxed()
					.await;
				if let Err(error) = result {
					sender.send(Err(error)).await.ok();
				}
			}
		});
		let stream = ReceiverStream::new(receiver).attach(task).boxed();

		Ok(Some(stream))
	}

	async fn write_process_stdio_log_local_task(
		&self,
		session: &Session,
		id: &tg::process::Id,
		streams: BTreeSet<tg::process::stdio::Stream>,
		end: Option<u64>,
		mut input: BoxStream<'static, tg::Result<Data>>,
		sender: &Sender,
	) -> tg::Result<()> {
		let mut batch = Vec::new();
		let mut batch_length = 0_usize;
		loop {
			// Store the batch once no more data is immediately available.
			let data = if let Some(data) = input.try_next().now_or_never() {
				data?
			} else {
				self.flush_process_stdio_log_batch(session, id, &mut batch, sender)
					.await?;
				batch_length = 0;
				input.try_next().await?
			};
			let Some(data) = data else {
				self.flush_process_stdio_log_batch(session, id, &mut batch, sender)
					.await?;
				return Ok(());
			};
			match data {
				Data::Chunk(chunk) => {
					if let Some(end) = end {
						// A writer that reconnects after the log ended replays chunks that were already stored.
						let position = chunk
							.combined_position
							.checked_add(chunk.bytes.len().to_u64().unwrap())
							.ok_or_else(|| tg::error!("the log position is too large"))?;
						if position > end {
							return Err(tg::error!("the process log has ended"));
						}
						let output = Output {
							closed: false,
							position,
						};
						sender
							.send(Ok(output))
							.await
							.map_err(|_| tg::error!("the stdio write output closed"))?;
						continue;
					}
					let length = chunk.bytes.len();
					let config = &session.server.config.process.stdio_log_batch;
					if batch.len() >= config.max_chunks
						|| (!batch.is_empty()
							&& batch_length.saturating_add(length) > config.max_size)
					{
						self.flush_process_stdio_log_batch(session, id, &mut batch, sender)
							.await?;
						batch_length = 0;
					}
					let entry = Self::create_process_stdio_log_entry(id, &streams, chunk)?;
					batch.push(entry);
					batch_length += length;
				},
				Data::End(end) => {
					self.flush_process_stdio_log_batch(session, id, &mut batch, sender)
						.await?;
					let output = self
						.end_process_stdio_log(session, id, &streams, end)
						.await?;
					sender
						.send(Ok(output))
						.await
						.map_err(|_| tg::error!("the stdio write output closed"))?;
					return Ok(());
				},
			}
		}
	}

	async fn flush_process_stdio_log_batch(
		&self,
		session: &Session,
		id: &tg::process::Id,
		batch: &mut Vec<log::put::Arg>,
		sender: &Sender,
	) -> tg::Result<()> {
		let Some(last) = batch.last() else {
			return Ok(());
		};
		let position = last.position + last.bytes.len().to_u64().unwrap();
		let entries = mem::take(batch);
		self.put_process_stdio_log_batch(session, id, entries)
			.await?;
		let output = Output {
			closed: false,
			position,
		};
		sender
			.send(Ok(output))
			.await
			.map_err(|_| tg::error!("the stdio write output closed"))?;

		Ok(())
	}

	async fn put_process_stdio_log_batch(
		&self,
		session: &Session,
		id: &tg::process::Id,
		args: Vec<log::put::Arg>,
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		session
			.server
			.cache
			.put_log_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the log"))?;
		Self::notify_process_log(&session.server, id);

		Ok(())
	}

	async fn end_process_stdio_log(
		&self,
		session: &Session,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
		end: tg::process::stdio::End,
	) -> tg::Result<Output> {
		if end
			.stream_positions
			.contains_key(&tg::process::stdio::Stream::Stdin)
		{
			return Err(tg::error!("invalid log end streams"));
		}
		let end = tg::process::log::End {
			position: end.combined_position,
			stderr_position: end
				.stream_positions
				.get(&tg::process::stdio::Stream::Stderr)
				.copied()
				.unwrap_or_default(),
			stdout_position: end
				.stream_positions
				.get(&tg::process::stdio::Stream::Stdout)
				.copied()
				.unwrap_or_default(),
		};
		if streams.is_empty() {
			return Err(tg::error!("the process does not have a log"));
		}
		if end.stderr_position.checked_add(end.stdout_position) != Some(end.position)
			|| (!streams.contains(&tg::process::stdio::Stream::Stderr) && end.stderr_position != 0)
			|| (!streams.contains(&tg::process::stdio::Stream::Stdout) && end.stdout_position != 0)
		{
			return Err(tg::error!("invalid log end positions"));
		}

		let data = session
			.server
			.index
			.try_get_process_with_data(id)
			.await?
			.and_then(|(_, data)| data);
		if data.is_some_and(|data| data.log.is_some()) {
			return Ok(Output {
				closed: true,
				position: end.position,
			});
		}

		tangram_server_core::checkpoint!(session.server, "process.stdio.log.end", process = %id)
			.await;

		// Persist the writer's final positions before reporting success.
		if let Some(stored) = session.server.cache.try_get_log_end(id).await? {
			if stored != end {
				return Err(tg::error!("the log end positions do not match"));
			}
		} else {
			let arg = log::end::Arg {
				end,
				process: id.clone(),
			};
			session
				.server
				.cache
				.put_log_end(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to store the log end"))?;
		}
		Self::notify_process_log(&session.server, id);
		for &stream in streams {
			Self::spawn_publish_process_stdio_close_message_task(&session.server, id, stream);
		}

		Ok(Output {
			closed: true,
			position: end.position,
		})
	}

	fn create_process_stdio_log_entry(
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
		if chunk.bytes.len() > tg::process::stdio::MAX_CHUNK_SIZE {
			return Err(tg::error!("the process log chunk is too large"));
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
}
