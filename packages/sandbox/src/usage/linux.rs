use {
	crate::{cgroup::Handle, usage::Snapshot},
	tangram_client::prelude::*,
};

pub(crate) struct Source {
	cgroup: Handle,
	directory: Option<std::fs::File>,
	project: Option<u32>,
}

impl Source {
	pub(crate) fn new(
		cgroup: Handle,
		path: &std::path::Path,
		project: Option<u32>,
	) -> tg::Result<Self> {
		let directory = project
			.map(|_| std::fs::File::open(path.parent().unwrap_or(path)))
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to open the sandbox storage meter"))?;
		Ok(Self {
			cgroup,
			directory,
			project,
		})
	}

	pub(crate) fn snapshot(&self) -> tg::Result<Snapshot> {
		let cpu = parse_cpu(&self.cgroup.read(c"cpu.stat")?)?
			.checked_mul(1000)
			.ok_or_else(|| tg::error!("the CPU usage overflowed"))?;
		let memory = self
			.cgroup
			.read(c"memory.current")?
			.trim()
			.parse()
			.map_err(|error| tg::error!(!error, "invalid sandbox memory usage"))?;
		let swap = self
			.cgroup
			.read(c"memory.swap.current")?
			.trim()
			.parse()
			.map_err(|error| tg::error!(!error, "invalid sandbox swap usage"))?;

		// Read the storage usage from the filesystem's project quota counter, and report none without a project quota.
		let storage = if let (Some(directory), Some(project)) = (&self.directory, self.project) {
			crate::filesystem::usage(directory, project)?
		} else {
			0
		};

		Ok(Snapshot {
			cpu,
			memory,
			storage,
			swap,
		})
	}
}

fn parse_cpu(contents: &str) -> tg::Result<u64> {
	let value = contents
		.lines()
		.find_map(|line| line.strip_prefix("usage_usec "))
		.ok_or_else(|| tg::error!("cpu.stat does not contain usage_usec"))?;
	let cpu = value
		.trim()
		.parse()
		.map_err(|error| tg::error!(!error, "invalid sandbox CPU usage"))?;
	Ok(cpu)
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn reads_total_cpu_time() {
		assert_eq!(
			parse_cpu("user_usec 10\nusage_usec 30\nsystem_usec 20\n").unwrap(),
			30
		);
		assert!(parse_cpu("user_usec 10\n").is_err());
		assert!(parse_cpu("usage_usec nope\n").is_err());
	}
}
