use {
	num::ToPrimitive as _,
	std::{borrow::Cow, collections::BTreeSet},
	tangram_client::prelude::*,
};

#[cfg(test)]
mod tests;

/// Read at most `length` contiguous bytes from `position` in the selected streams, stopping at the first gap.
#[derive(Clone, Debug)]
pub struct Arg {
	pub length: u64,
	pub position: u64,
	pub process: tg::process::Id,
	pub streams: BTreeSet<tg::process::stdio::Stream>,
}

pub(crate) struct Builder<'a> {
	arg: &'a Arg,
	entries: Vec<Entry<'static>>,
	position: u64,
	remaining: u64,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Entry<'a> {
	#[tangram_serialize(id = 0)]
	pub bytes: Cow<'a, [u8]>,

	#[tangram_serialize(id = 1)]
	pub position: u64,

	#[tangram_serialize(id = 2)]
	pub stream: tg::process::stdio::Stream,

	#[tangram_serialize(id = 3)]
	pub stream_position: u64,

	#[tangram_serialize(id = 4)]
	pub timestamp: i64,
}

impl<'a> Builder<'a> {
	#[must_use]
	pub(crate) fn new(arg: &'a Arg) -> Self {
		Self {
			arg,
			entries: Vec::new(),
			position: arg.position,
			remaining: arg.length,
		}
	}

	// Return only a contiguous prefix so a reader can resume at a gap when a delayed write arrives.
	pub(crate) fn push(&mut self, entry: &Entry<'_>) -> bool {
		if self.remaining == 0 {
			return false;
		}
		if !self.arg.streams.contains(&entry.stream) {
			return true;
		}
		let position = if self.arg.streams.len() > 1 {
			entry.position
		} else {
			entry.stream_position
		};
		if position > self.position {
			return false;
		}
		let offset = self.position - position;
		let available = entry.bytes.len().to_u64().unwrap().saturating_sub(offset);
		let length = self.remaining.min(available);
		if length == 0 {
			return true;
		}
		let bytes = &entry.bytes[offset.to_usize().unwrap()..(offset + length).to_usize().unwrap()];
		let position = entry.position + offset;
		let stream_position = entry.stream_position + offset;

		// Preserve both positions when coalescing entries, including reads of a single stream.
		if let Some(previous) = self.entries.last_mut()
			&& previous.stream == entry.stream
			&& previous.position + previous.bytes.len().to_u64().unwrap() == position
			&& previous.stream_position + previous.bytes.len().to_u64().unwrap() == stream_position
		{
			previous.bytes.to_mut().extend_from_slice(bytes);
		} else {
			let entry = Entry {
				bytes: Cow::Owned(bytes.to_vec()),
				position,
				stream: entry.stream,
				stream_position,
				timestamp: entry.timestamp,
			};
			self.entries.push(entry);
		}
		self.position += length;
		self.remaining -= length;

		self.remaining > 0
	}

	#[must_use]
	pub(crate) fn finish(self) -> Vec<Entry<'static>> {
		self.entries
	}
}

impl Entry<'_> {
	#[must_use]
	pub fn into_static(self) -> Entry<'static> {
		Entry {
			bytes: Cow::Owned(self.bytes.into_owned()),
			position: self.position,
			stream: self.stream,
			stream_position: self.stream_position,
			timestamp: self.timestamp,
		}
	}
}
