use {std::path::Path, tangram_client::prelude::*};

pub fn detect(path: &Path) -> tg::Result<(u64, u64, u64)> {
	let contents = std::fs::read_to_string("/proc/meminfo")
		.map_err(|error| tg::error!(!error, "failed to read the host memory capacity"))?;
	let mut memory = field(&contents, "MemAvailable:")?;
	let mut swap = field(&contents, "SwapFree:")?;
	let current = std::fs::read_to_string("/proc/self/cgroup")
		.map_err(|error| tg::error!(!error, "failed to read the runner cgroup"))?;
	let current = current
		.lines()
		.find_map(|line| line.strip_prefix("0::"))
		.ok_or_else(|| tg::error!("the runner requires cgroup v2"))?;
	let current = std::path::PathBuf::from("/sys/fs/cgroup").join(current.trim_start_matches('/'));
	for ancestor in current
		.ancestors()
		.take_while(|ancestor| ancestor.starts_with("/sys/fs/cgroup"))
	{
		for (available, limit, used) in [
			(&mut memory, "memory.max", "memory.current"),
			(&mut swap, "memory.swap.max", "memory.swap.current"),
		] {
			let Ok(limit) = std::fs::read_to_string(ancestor.join(limit)) else {
				continue;
			};
			if limit.trim() == "max" {
				continue;
			}
			let limit = limit
				.trim()
				.parse::<u64>()
				.map_err(|error| tg::error!(!error, "invalid ancestor resource limit"))?;
			let used = std::fs::read_to_string(ancestor.join(used)).map_err(|error| {
				tg::error!(!error, "failed to read the ancestor resource usage")
			})?;
			let used = used
				.trim()
				.parse::<u64>()
				.map_err(|error| tg::error!(!error, "invalid ancestor resource usage"))?;
			*available = (*available).min(limit.saturating_sub(used));
		}
	}
	let statistics = rustix::fs::statvfs(path)
		.map_err(|error| tg::error!(!error, "failed to read the host disk capacity"))?;
	let storage = statistics
		.f_bavail
		.checked_mul(statistics.f_frsize)
		.ok_or_else(|| tg::error!("the disk capacity overflowed"))?;
	Ok((memory, storage, swap))
}

fn field(contents: &str, name: &str) -> tg::Result<u64> {
	let value = contents
		.lines()
		.find_map(|line| line.strip_prefix(name))
		.and_then(|value| value.split_ascii_whitespace().next())
		.ok_or_else(|| tg::error!("the host memory capacity is missing"))?;
	let value = value
		.parse::<u64>()
		.map_err(|error| tg::error!(!error, "invalid host memory capacity"))?;
	value
		.checked_mul(1024)
		.ok_or_else(|| tg::error!("the memory capacity overflowed"))
}
