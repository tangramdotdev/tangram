use {super::Store, crate::log, num::ToPrimitive as _, std::borrow::Cow};

impl Store {
	#[expect(clippy::needless_pass_by_value)]
	pub fn delete_log(&self, arg: log::delete::Arg) {
		self.state().logs.remove(&arg.process);
	}

	pub fn put_log(&self, arg: log::put::Arg) {
		self.put_log_batch(vec![arg]);
	}

	pub fn put_log_batch(&self, args: Vec<log::put::Arg>) {
		let mut state = self.state();
		for arg in args {
			if arg.bytes.is_empty() {
				continue;
			}
			let log::put::Arg {
				bytes,
				position,
				process,
				stream,
				stream_position,
				timestamp,
			} = arg;
			let log = state.logs.entry(process).or_default();
			let entry = log::read::Entry {
				bytes: Cow::Owned(bytes.to_vec()),
				position,
				stream,
				stream_position,
				timestamp,
			};
			log.entries.insert(position, entry);
			log.stream_positions
				.insert((stream, stream_position), position);
		}
	}

	#[must_use]
	pub fn try_get_log_length(&self, arg: &log::length::Arg) -> Option<u64> {
		if arg.streams.is_empty() {
			return None;
		}
		let state = self.state();
		let log = state.logs.get(&arg.process)?;
		if arg.streams.len() == 1 {
			let stream = arg.streams.iter().next().copied()?;
			let position = log
				.stream_positions
				.range((stream, 0)..(stream, u64::MAX))
				.next_back()
				.map(|(_, &position)| position)?;
			let entry = log.entries.get(&position)?;
			Some(entry.stream_position + entry.bytes.len().to_u64().unwrap())
		} else {
			let entry = log.entries.values().next_back()?;
			Some(entry.position + entry.bytes.len().to_u64().unwrap())
		}
	}

	#[must_use]
	#[expect(clippy::needless_pass_by_value)]
	pub fn try_read_log(&self, arg: log::read::Arg) -> Vec<log::read::Entry<'static>> {
		let state = self.state();
		let Some(log) = state.logs.get(&arg.process) else {
			return Vec::new();
		};
		if arg.streams.is_empty() {
			return Vec::new();
		}
		let combined = arg.streams.len() > 1;
		let start_position = if combined {
			arg.position
		} else {
			let Some(stream) = arg.streams.iter().next().copied() else {
				return Vec::new();
			};
			let position = log
				.stream_positions
				.range(..=(stream, arg.position))
				.next_back()
				.filter(|((current, _), _)| *current == stream)
				.map(|(_, &position)| position);
			let Some(position) = position else {
				return Vec::new();
			};
			position
		};
		let mut current: Option<log::read::Entry<'static>> = None;
		let mut output = Vec::new();
		let mut remaining = arg.length;
		for chunk in log.entries.range(start_position..).map(|(_, entry)| entry) {
			if remaining == 0 {
				break;
			}
			if !arg.streams.contains(&chunk.stream) {
				continue;
			}
			let position = if combined {
				chunk.position
			} else {
				chunk.stream_position
			};
			let offset = arg.position.saturating_sub(position);
			let available = chunk.bytes.len().to_u64().unwrap().saturating_sub(offset);
			let take = remaining.min(available);
			if take == 0 {
				continue;
			}
			let start = offset.to_usize().unwrap();
			let end = (offset + take).to_usize().unwrap();
			let bytes = chunk.bytes[start..end].to_vec();
			if let Some(entry) = &mut current {
				if entry.stream == chunk.stream {
					entry.bytes.to_mut().extend_from_slice(&bytes);
				} else {
					output.push(current.take().unwrap());
					current = Some(log::read::Entry {
						bytes: Cow::Owned(bytes),
						position: chunk.position + offset,
						stream: chunk.stream,
						stream_position: chunk.stream_position + offset,
						timestamp: chunk.timestamp,
					});
				}
			} else {
				current = Some(log::read::Entry {
					bytes: Cow::Owned(bytes),
					position: chunk.position + offset,
					stream: chunk.stream,
					stream_position: chunk.stream_position + offset,
					timestamp: chunk.timestamp,
				});
			}
			remaining -= take;
		}
		if let Some(entry) = current {
			output.push(entry);
		}

		output
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, std::collections::BTreeSet, tangram_client::prelude::*};

	fn collect_bytes(entries: Vec<log::read::Entry<'_>>) -> Bytes {
		entries
			.into_iter()
			.flat_map(|entry| entry.bytes.to_vec())
			.collect::<Vec<_>>()
			.into()
	}

	#[test]
	fn put_retry_is_idempotent() {
		let store = Store::new();
		let process = tg::process::Id::new();
		let arg = log::put::Arg {
			bytes: Bytes::from_static(b"hello"),
			position: 0,
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			stream_position: 0,
			timestamp: 1,
		};
		store.put_log(arg.clone());
		store.put_log(arg);
		let entries = store.try_read_log(log::read::Arg {
			length: u64::MAX,
			position: 0,
			process: process.clone(),
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		});
		assert_eq!(collect_bytes(entries), Bytes::from_static(b"hello"));
		assert_eq!(
			store.try_get_log_length(&log::length::Arg {
				process,
				streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
			}),
			Some(5)
		);
	}
}
