use {
	crate::{container::cgroup::Handle, usage::Snapshot},
	std::{
		collections::BTreeSet,
		fs::File,
		io::Read as _,
		os::fd::{AsRawFd as _, FromRawFd as _},
	},
	tangram_client::prelude::*,
};

pub(crate) struct Source {
	cgroup: Handle,
	counters: Option<Vec<File>>,
}

/// The original 64-byte `perf_event_attr` ABI, with unused sampling fields zeroed.
#[repr(C)]
#[derive(Default)]
struct Attr {
	type_: u32,
	size: u32,
	config: u64,
	sample_period: u64,
	sample_type: u64,
	read_format: u64,
	flags: u64,
	wakeup_events: u32,
	bp_type: u32,
	config1: u64,
}

impl Source {
	pub(crate) fn new(
		cgroup: Handle,
		shared: Option<&BTreeSet<u32>>,
		mixed: bool,
	) -> tg::Result<Self> {
		// Monitor every potential shared CPU so moving shared workloads preserves the counters.
		let counters = if mixed {
			let shared = shared
				.ok_or_else(|| tg::error!("mixed CPU accounting requires a sandbox CPU pool"))?;
			let directory = cgroup.open_fd()?;
			let counters = shared
				.iter()
				.map(|cpu| open_counter(directory.as_raw_fd(), *cpu))
				.collect::<tg::Result<Vec<_>>>()?;
			Some(counters)
		} else {
			None
		};
		Ok(Self { cgroup, counters })
	}

	pub(crate) fn snapshot(&self) -> tg::Result<Snapshot> {
		let cpu = if let Some(counters) = &self.counters {
			let mut cpu = 0_u64;
			for mut counter in counters {
				let mut bytes = [0; 8];
				counter
					.read_exact(&mut bytes)
					.map_err(|error| tg::error!(!error, "failed to read the shared CPU counter"))?;
				cpu = cpu
					.checked_add(u64::from_ne_bytes(bytes))
					.ok_or_else(|| tg::error!("the shared CPU usage overflowed"))?;
			}
			cpu
		} else {
			let contents = self.cgroup.read(c"cpu.stat")?;
			let usage = parse_cpu(&contents)?;
			usage
				.checked_mul(1000)
				.ok_or_else(|| tg::error!("the sandbox CPU usage overflowed"))?
		};
		let memory = self
			.cgroup
			.read(c"memory.current")?
			.trim()
			.parse()
			.map_err(|error| tg::error!(!error, "invalid sandbox memory usage"))?;
		Ok(Snapshot { cpu, memory })
	}
}

fn open_counter(cgroup: i32, cpu: u32) -> tg::Result<File> {
	let attr = Attr {
		type_: 1,
		size: std::mem::size_of::<Attr>().try_into().unwrap(),
		..Default::default()
	};
	let cpu = i32::try_from(cpu).map_err(|_| tg::error!("the CPU ID is too large"))?;
	// SAFETY: The attribute pointer describes a valid perf_event_attr ABI; the cgroup descriptor remains open for this call.
	let fd = unsafe {
		libc::syscall(
			libc::SYS_perf_event_open,
			&raw const attr,
			cgroup,
			cpu,
			-1_i32,
			12_u64,
		)
	};
	if fd < 0 {
		let error = std::io::Error::last_os_error();
		return Err(tg::error!(
			!error,
			"failed to open shared CPU accounting; mixed CPU requests require cgroup perf events and CAP_PERFMON"
		));
	}
	let fd = i32::try_from(fd)
		.map_err(|_| tg::error!("the performance counter descriptor is too large"))?;
	// SAFETY: perf_event_open returned a new owned descriptor.
	let counter = unsafe { File::from_raw_fd(fd) };
	Ok(counter)
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
