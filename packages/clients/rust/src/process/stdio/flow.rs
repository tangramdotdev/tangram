use {
	super::read,
	crate::prelude::*,
	futures::{
		StreamExt as _, TryStreamExt as _,
		stream::{self, BoxStream},
	},
	std::collections::VecDeque,
};

#[cfg(test)]
mod tests;

pub const CHUNK_SIZE: usize = 32 * 1024;
pub const MAX_CHUNKS: usize = 64;
pub const CHANNEL_CAPACITY: usize = MAX_CHUNKS * 2 + 4;
pub const WINDOW: u64 = (CHUNK_SIZE * MAX_CHUNKS) as u64;

/// Bound outstanding bytes and chunk metadata independently of the source position.
#[derive(Default)]
pub struct Sender {
	chunks: VecDeque<u64>,
	consumed: u64,
	sent: u64,
}

#[derive(Default)]
pub struct Receiver {
	chunks: usize,
	consumed: u64,
	reported: u64,
}

struct State {
	ended: bool,
	input: BoxStream<'static, tg::Result<read::ClientMessage>>,
	output: BoxStream<'static, tg::Result<read::ServerMessage>>,
	pending: Option<read::ServerMessage>,
	window: Sender,
}

#[must_use]
pub fn read(
	input: BoxStream<'static, tg::Result<read::ClientMessage>>,
	output: BoxStream<'static, tg::Result<read::ServerMessage>>,
) -> BoxStream<'static, tg::Result<read::ServerMessage>> {
	let state = State {
		ended: false,
		input,
		output,
		pending: None,
		window: Sender::default(),
	};
	stream::try_unfold(state, |mut state| async move {
		loop {
			if let Some(message) = state.pending.take() {
				match &message {
					read::ServerMessage::Notification(read::Event::Chunk(chunk)) => {
						if chunk.bytes.len() > CHUNK_SIZE || chunk.bytes.is_empty() {
							return Err(tg::error!("invalid stdio chunk size"));
						}
						if state.window.available(chunk.bytes.len()) {
							state.window.send(chunk.bytes.len())?;
							return Ok(Some((message, state)));
						}
						state.pending = Some(message);
					},
					read::ServerMessage::Notification(read::Event::Position { .. }) => return Ok(Some((message, state))),
					read::ServerMessage::Response(_) => {
						state.ended = true;
						return Ok(Some((message, state)));
					},
				}
			}
			tokio::select! {
				biased;
				message = state.input.try_next() => match message?.ok_or_else(|| tg::error!("the stdio read input closed before completion"))? {
					read::ClientMessage::Ack if state.ended => return Ok(None),
					read::ClientMessage::Ack => return Err(tg::error!("received an unexpected stdio read acknowledgment")),
					read::ClientMessage::Notification(progress) => state.window.update(progress)?,
				},
				message = state.output.try_next(), if !state.ended && state.pending.is_none() => {
					state.pending = Some(message?.ok_or_else(|| tg::error!("the stdio read source ended before its response"))?);
				},
			}
		}
	}).boxed()
}

impl Sender {
	#[must_use]
	pub fn available(&self, length: usize) -> bool {
		self.chunks.len() < MAX_CHUNKS
			&& length as u64 <= WINDOW.saturating_sub(self.sent - self.consumed)
	}

	pub fn send(&mut self, length: usize) -> tg::Result<()> {
		if length == 0 || length > CHUNK_SIZE || !self.available(length) {
			return Err(tg::error!("the stdio flow window was exceeded"));
		}
		self.sent = self
			.sent
			.checked_add(length as u64)
			.ok_or_else(|| tg::error!("the stdio byte count is too large"))?;
		self.chunks.push_back(self.sent);
		Ok(())
	}

	pub fn update(&mut self, progress: super::read::Progress) -> tg::Result<()> {
		if progress.consumed < self.consumed || progress.consumed > self.sent {
			return Err(tg::error!("invalid stdio consumption progress"));
		}
		self.consumed = progress.consumed;
		while self.chunks.front().is_some_and(|end| *end <= self.consumed) {
			self.chunks.pop_front();
		}
		Ok(())
	}
}

impl Receiver {
	pub fn consume(&mut self, length: usize) -> tg::Result<Option<super::read::Progress>> {
		if length == 0 {
			return Ok(None);
		}
		self.consumed = self
			.consumed
			.checked_add(length as u64)
			.ok_or_else(|| tg::error!("the stdio byte count is too large"))?;
		self.chunks += 1;
		if self.consumed - self.reported < WINDOW / 2 && self.chunks < MAX_CHUNKS / 2 {
			return Ok(None);
		}
		self.chunks = 0;
		self.reported = self.consumed;
		let progress = super::read::Progress {
			consumed: self.consumed,
		};
		Ok(Some(progress))
	}
}
