use {
	super::{Ack, ClientMessage, Data, Input, Request, Response, ServerMessage, stream::Arg},
	crate::{prelude::*, process::stdio::flow},
	futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream},
	std::collections::{BTreeMap, VecDeque},
};

struct Pending {
	completion: Option<tokio::sync::oneshot::Sender<()>>,
	request: Request,
	sent: bool,
}

pub(crate) async fn all<H: tg::Handle>(
	handle: &H,
	id: &tg::process::Id,
	arg: Arg,
	mut input: BoxStream<'static, tg::Result<Input>>,
) -> tg::Result<()> {
	let mut combined_position = 0;
	let mut stream_positions = arg
		.streams
		.iter()
		.map(|stream| (*stream, 0))
		.collect::<BTreeMap<_, _>>();
	let mut input_ended = false;
	let mut next_id = 0;
	let mut output = None::<BoxStream<'static, tg::Result<ServerMessage>>>;
	let mut pending = VecDeque::<Pending>::new();
	let mut remaining = None::<Input>;
	let mut sender = None::<async_channel::Sender<tg::Result<ClientMessage>>>;
	let mut retries = None::<BoxStream<'static, ()>>;
	loop {
		// Reconnect with only the writes whose completed outcomes are still unknown.
		if output.is_none() {
			if let Some(retries) = &mut retries {
				retries.next().await;
			}
			let (new_sender, receiver) = async_channel::bounded(flow::CHANNEL_CAPACITY);
			match handle
				.try_write_process_stdio(id, arg.clone(), receiver.boxed())
				.await
			{
				Ok(Some(stream)) => {
					output = Some(stream.boxed());
					sender = Some(new_sender);
				},
				Ok(None) => return Err(tg::error!("failed to find the process")),
				Err(error) if retries.is_some() => {
					tracing::error!(error = %error.trace(), "failed to reconnect the stdio write stream");
					continue;
				},
				Err(error) => return Err(error),
			}
			for pending in &mut pending {
				pending.sent = false;
			}
		}

		// Keep both bytes and request metadata bounded until responses arrive.
		if pending.len() < flow::MAX_CHUNKS {
			if let Some(mut value) = remaining.take() {
				let mut chunk = value.chunk.clone();
				if !arg.streams.contains(&chunk.stream) {
					return Err(tg::error!("invalid process stdio stream"));
				}
				let length = chunk.bytes.len().min(flow::CHUNK_SIZE);
				if length == 0 {
					if let Some(completion) = value.completion.take() {
						completion.send(()).ok();
					}
					continue;
				}
				chunk.bytes = chunk.bytes.slice(..length);
				combined_position = chunk
					.combined_position
					.checked_add(length as u64)
					.ok_or_else(|| tg::error!("the stdio position is too large"))?;
				stream_positions.insert(
					chunk.stream,
					chunk
						.stream_position
						.checked_add(length as u64)
						.ok_or_else(|| tg::error!("the stdio position is too large"))?,
				);
				let completion = if length == value.chunk.bytes.len() {
					value.completion.take()
				} else {
					value.chunk.bytes = value.chunk.bytes.slice(length..);
					value.chunk.combined_position += length as u64;
					value.chunk.stream_position += length as u64;
					remaining = Some(value);
					None
				};
				let request = Request {
					arg: Data::Chunk(chunk),
					id: next_id,
				};
				next_id += 1;
				pending.push_back(Pending {
					completion,
					request,
					sent: false,
				});
			} else if input_ended && pending.is_empty() {
				let end = tg::process::stdio::End {
					combined_position,
					stream_positions: stream_positions.clone(),
				};
				let request = Request {
					arg: Data::End(end),
					id: next_id,
				};
				next_id += 1;
				pending.push_back(Pending {
					completion: None,
					request,
					sent: false,
				});
			}
		}
		for pending in &mut pending {
			if !pending.sent {
				sender
					.as_ref()
					.unwrap()
					.send(Ok(ClientMessage::Request(pending.request.clone())))
					.await
					.ok();
				pending.sent = true;
			}
		}

		// Poll responses even while the input is idle or the write window is full.
		tokio::select! {
			biased;
			message = output.as_mut().unwrap().next() => {
				let Some(message) = message else {
					output = None;
					sender = None;
					let options = tangram_futures::retry::Options {
						max_retries: u64::MAX,
						..Default::default()
					};
					retries.get_or_insert_with(|| tangram_futures::retry::stream(options).boxed());
					continue;
				};
				let ServerMessage::Response(response) = message? else { continue; };
				retries.take();
				sender.as_ref().unwrap().send(Ok(ClientMessage::Ack(Ack { id: response.id }))).await.ok();
				let value = pending.pop_front().ok_or_else(|| tg::error!("received an unexpected write response"))?;
				if value.complete(response)? {
					return Ok(());
				}
			},
			value = input.try_next(), if !input_ended && remaining.is_none() && pending.len() < flow::MAX_CHUNKS => {
				remaining = value?;
				input_ended = remaining.is_none();
			},
			() = tokio::task::yield_now(), if remaining.is_some() && pending.len() < flow::MAX_CHUNKS => {},
		}
	}
}

impl Pending {
	fn complete(self, response: Response) -> tg::Result<bool> {
		if self.request.id != response.id {
			return Err(tg::error!("received an out-of-order write response"));
		}
		if let Some(error) = response.error {
			return Err(tg::Error::try_from(error)?);
		}
		let output = response
			.output
			.ok_or_else(|| tg::error!("missing the process write response"))?;
		let length = match &self.request.arg {
			Data::Chunk(chunk) => chunk.bytes.len() as u64,
			Data::End(_) => 0,
		};
		if output.length > length || (!output.closed && output.length != length) {
			return Err(tg::error!("invalid process stdio write length"));
		}
		if matches!(self.request.arg, Data::End(_)) && !output.closed {
			return Err(tg::error!("the stdio end was not confirmed"));
		}
		if output.length == length
			&& let Some(completion) = self.completion
		{
			completion.send(()).ok();
		}

		Ok(output.closed)
	}
}
