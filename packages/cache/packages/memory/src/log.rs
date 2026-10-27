use {
	crate::Cache, num::ToPrimitive as _, std::borrow::Cow, tangram_cache::log,
	tangram_client::prelude::*,
};
#[cfg(test)]
mod tests;

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

	pub async fn delete_log_cache_entry(&self, arg: log::cache::delete::Arg) -> tg::Result<()> {
		let mut state = self.state();
		let entry = arg.entry;
		state.logs.remove(&entry.process);
		state
			.log_cache
			.remove(&(entry.partition, entry.expires_at, entry.process));
		Ok(())
	}

	pub async fn get_log_cache_entries(
		&self,
		arg: log::cache::get::Arg,
	) -> tg::Result<Vec<log::cache::Entry>> {
		let state = self.state();
		let output = state
			.log_cache
			.iter()
			.filter(|(partition, expires_at, _)| {
				*partition == arg.partition && *expires_at <= arg.now
			})
			.take(arg.batch_size)
			.map(|(partition, expires_at, process)| log::cache::Entry {
				expires_at: *expires_at,
				partition: *partition,
				process: process.clone(),
			})
			.collect();

		Ok(output)
	}

	pub async fn put_log_cache_entry(&self, arg: log::cache::put::Arg) -> tg::Result<()> {
		let entry = arg.entry;
		self.state()
			.log_cache
			.insert((entry.partition, entry.expires_at, entry.process));
		Ok(())
	}
}

impl tangram_cache::log::Cache for Cache {
	async fn delete_log_cache_entry(
		&self,
		arg: tangram_cache::log::cache::delete::Arg,
	) -> tg::Result<()> {
		self.delete_log_cache_entry(arg).await
	}

	async fn get_log_cache_entries(
		&self,
		arg: tangram_cache::log::cache::get::Arg,
	) -> tg::Result<Vec<tangram_cache::log::cache::Entry>> {
		self.get_log_cache_entries(arg).await
	}

	async fn put_log_cache_entry(
		&self,
		arg: tangram_cache::log::cache::put::Arg,
	) -> tg::Result<()> {
		self.put_log_cache_entry(arg).await
	}

	async fn delete_log(&self, arg: tangram_cache::log::delete::Arg) -> tg::Result<()> {
		self.delete_log(arg);
		Ok(())
	}

	async fn put_log(&self, arg: tangram_cache::log::put::Arg) -> tg::Result<()> {
		self.put_log(arg);
		Ok(())
	}

	async fn put_log_batch(&self, args: Vec<tangram_cache::log::put::Arg>) -> tg::Result<()> {
		self.put_log_batch(args);
		Ok(())
	}

	async fn put_log_end(&self, arg: tangram_cache::log::end::Arg) -> tg::Result<()> {
		self.put_log_end(arg);
		Ok(())
	}

	async fn try_get_log_end(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::log::End>> {
		Ok(self.try_get_log_end(process))
	}

	async fn try_get_log_length(
		&self,
		arg: tangram_cache::log::length::Arg,
	) -> tg::Result<Option<u64>> {
		Ok(self.try_get_log_length(&arg))
	}

	async fn try_read_log(
		&self,
		arg: tangram_cache::log::read::Arg,
	) -> tg::Result<Vec<tangram_cache::log::read::Entry<'static>>> {
		Ok(self.try_read_log(arg))
	}
}
