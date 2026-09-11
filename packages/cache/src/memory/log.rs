use {
	super::Cache, crate::log, num::ToPrimitive as _, std::borrow::Cow, tangram_client::prelude::*,
};

impl Cache {
	#[expect(clippy::needless_pass_by_value)]
	pub fn delete_log(&self, arg: log::delete::Arg) {
		self.state().logs.remove(&arg.process);
	}

	pub fn put_log_end(&self, arg: log::end::Arg) {
		self.state().logs.entry(arg.process).or_default().end = Some(arg.end);
	}

	#[must_use]
	pub fn try_get_log_end(&self, process: &tg::process::Id) -> Option<tg::process::log::End> {
		self.state().logs.get(process)?.end
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
			let Some((&position, _)) = log.entries.range(..=arg.position).next_back() else {
				return Vec::new();
			};
			position
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
		let mut builder = log::read::Builder::new(&arg);
		for entry in log.entries.range(start_position..).map(|(_, entry)| entry) {
			if !builder.push(entry) {
				break;
			}
		}

		builder.finish()
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, std::collections::BTreeSet};

	fn collect_bytes(entries: Vec<log::read::Entry<'_>>) -> Bytes {
		entries
			.into_iter()
			.flat_map(|entry| entry.bytes.to_vec())
			.collect::<Vec<_>>()
			.into()
	}

	#[test]
	fn put_retry_is_idempotent() {
		let cache = Cache::new();
		let process = tg::process::Id::new();
		let arg = log::put::Arg {
			bytes: Bytes::from_static(b"hello"),
			position: 0,
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			stream_position: 0,
			timestamp: 1,
		};
		cache.put_log(arg.clone());
		cache.put_log(arg);
		let entries = cache.try_read_log(log::read::Arg {
			length: u64::MAX,
			position: 0,
			process: process.clone(),
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		});
		assert_eq!(collect_bytes(entries), Bytes::from_static(b"hello"));
		assert_eq!(
			cache.try_get_log_length(&log::length::Arg {
				process,
				streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
			}),
			Some(5)
		);
	}
}
