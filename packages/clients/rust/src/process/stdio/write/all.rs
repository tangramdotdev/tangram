use {
	super::{Data, Input, Output, stream::Arg},
	crate::prelude::*,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future::BoxFuture, stream::BoxStream,
	},
	std::collections::{BTreeMap, VecDeque},
	tokio_stream::wrappers::ReceiverStream,
};

type Connecting = BoxFuture<'static, tg::Result<Option<BoxStream<'static, tg::Result<Output>>>>>;

struct Pending {
	completion: Option<tokio::sync::oneshot::Sender<()>>,
	data: Data,
	end: u64,
	sent: bool,
}

pub(crate) async fn all<I: tg::Instance>(
	instance: &I,
	arg: Arg,
	mut input: BoxStream<'static, tg::Result<Input>>,
) -> tg::Result<()> {
	let mut combined_position = 0;
	let mut stream_positions = arg
		.streams
		.iter()
		.map(|stream| (*stream, 0))
		.collect::<BTreeMap<_, _>>();
	let mut connecting = None::<Connecting>;
	let mut error = None::<tg::Error>;
	let mut input_ended = false;
	let mut output = None::<BoxStream<'static, tg::Result<Output>>>;
	let mut pending = VecDeque::<Pending>::new();
	let mut retries = None::<BoxStream<'static, ()>>;
	let mut sender = None::<tokio::sync::mpsc::Sender<tg::Result<Data>>>;
	let stream_retry = instance.arg().stream_retry;
	let mut written = 0;
	loop {
		// Reconnect and resend the data the server has not reported written, writing the request body without waiting for the response headers.
		if output.is_none() && connecting.is_none() {
			// Fail with the last error once the retries are exhausted.
			if let Some(retries) = &mut retries
				&& retries.next().await.is_none()
			{
				let error = error.take().unwrap_or_else(|| {
					tg::error!(
						unavailable,
						"the process stdio stream ended before completion"
					)
				});
				return Err(error);
			}
			let (new_sender, receiver) = tokio::sync::mpsc::channel(1);
			sender = Some(new_sender);
			for pending in &mut pending {
				pending.sent = false;
			}
			let instance = instance.clone();
			let arg = arg.clone();
			connecting = Some(
				async move {
					let output = instance
						.try_write_process_stdio(arg, ReceiverStream::new(receiver).boxed())
						.await?
						.map(futures::StreamExt::boxed);
					Ok(output)
				}
				.boxed(),
			);
		}

		// Write the data, holding it until the server reports it written.
		let unsent = pending.iter().position(|pending| !pending.sent);
		let channel = sender.clone();
		tokio::select! {
			biased;
			result = async { connecting.as_mut().unwrap().await }, if connecting.is_some() => {
				connecting = None;
				match result {
					Ok(Some(stream)) => output = Some(stream),
					Ok(None) => return Err(tg::error!("failed to find the process")),
					// Retry the attempt if it failed with an unavailable error.
					Err(attempt_error) if attempt_error.kind() == Some(tg::error::Kind::Unavailable) => {
						tracing::error!(error = %attempt_error.trace(), "failed to reconnect the stdio write stream");
						error = Some(attempt_error);
						sender = None;
						retries.get_or_insert_with(|| tangram_futures::retry::stream(stream_retry.clone()).boxed());
					},
					Err(error) => return Err(error),
				}
			},
			message = async { output.as_mut().unwrap().next().await }, if output.is_some() => {
				let Some(message) = message else {
					output = None;
					sender = None;
					retries.get_or_insert_with(|| tangram_futures::retry::stream(stream_retry.clone()).boxed());
					continue;
				};
				let Output { closed, position } = message?;
				error = None;
				retries.take();
				written = written.max(position);
				while pending.front().is_some_and(|pending| {
					matches!(pending.data, Data::Chunk(_)) && pending.end <= written
				}) {
					let pending = pending.pop_front().unwrap();
					if let Some(completion) = pending.completion {
						completion.send(()).ok();
					}
				}
				if closed {
					return Ok(());
				}
			},
			permit = async { channel.as_ref().unwrap().reserve().await }, if unsent.is_some() && channel.is_some() => {
				let Ok(permit) = permit else {
					sender = None;
					continue;
				};
				let pending = &mut pending[unsent.unwrap()];
				let mut data = pending.data.clone();

				// Skip the bytes of a resent chunk that the server already wrote.
				if let Data::Chunk(chunk) = &mut data
					&& chunk.combined_position < written
				{
					let offset = usize::try_from(written - chunk.combined_position).unwrap();
					chunk.bytes = chunk.bytes.slice(offset..);
					chunk.combined_position += offset as u64;
					chunk.stream_position += offset as u64;
				}
				permit.send(Ok(data));
				pending.sent = true;
			},
			value = input.try_next(), if !input_ended && unsent.is_none() => {
				let Some(value) = value? else {
					input_ended = true;
					let end = tg::process::stdio::End {
						combined_position,
						stream_positions: stream_positions.clone(),
					};
					pending.push_back(Pending {
						completion: None,
						data: Data::End(end),
						end: combined_position,
						sent: false,
					});
					continue;
				};
				let Input { chunk, mut completion } = value;
				if !arg.streams.contains(&chunk.stream) {
					return Err(tg::error!("invalid process stdio stream"));
				}

				// Split the chunk, completing the input once its last part is written.
				let mut offset = 0;
				while offset < chunk.bytes.len() {
					let length = (chunk.bytes.len() - offset).min(tg::process::stdio::MAX_CHUNK_SIZE);
					let part = tg::process::stdio::Chunk {
						bytes: chunk.bytes.slice(offset..offset + length),
						combined_position: chunk.combined_position + offset as u64,
						stream: chunk.stream,
						stream_position: chunk.stream_position + offset as u64,
						timestamp: chunk.timestamp,
					};
					offset += length;
					combined_position = part
						.combined_position
						.checked_add(length as u64)
						.ok_or_else(|| tg::error!("the stdio position is too large"))?;
					stream_positions.insert(
						part.stream,
						part.stream_position
							.checked_add(length as u64)
							.ok_or_else(|| tg::error!("the stdio position is too large"))?,
					);
					let completion = if offset == chunk.bytes.len() {
						completion.take()
					} else {
						None
					};
					pending.push_back(Pending {
						completion,
						data: Data::Chunk(part),
						end: combined_position,
						sent: false,
					});
				}
				if let Some(completion) = completion {
					completion.send(()).ok();
				}
			},
		}
	}
}
