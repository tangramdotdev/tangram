use {crate::usage::Snapshot, std::collections::BTreeSet, tangram_client::prelude::*};

pub(crate) struct Source {
	pid: libc::pid_t,
}

impl Source {
	#[must_use]
	pub(crate) fn new(pid: libc::pid_t) -> Self {
		Self { pid }
	}

	pub(crate) fn snapshot(&self) -> tg::Result<Snapshot> {
		// Include living descendants and the CPU time their parents have already collected.
		let mut pending = vec![self.pid];
		let mut visited = BTreeSet::new();
		let mut cpu = 0_u64;
		let mut memory = 0_u64;
		while let Some(pid) = pending.pop() {
			if !visited.insert(pid) {
				continue;
			}
			// SAFETY: All fields of the rusage output structure accept zero initialization.
			let mut usage: libc::rusage_info_v2 = unsafe { std::mem::zeroed() };
			// SAFETY: The buffer is sized and aligned for the requested rusage flavor.
			let result = unsafe {
				libc::proc_pid_rusage(pid, libc::RUSAGE_INFO_V2, (&raw mut usage).cast())
			};
			if result != 0 {
				let error = std::io::Error::last_os_error();
				if pid != self.pid && error.raw_os_error() == Some(libc::ESRCH) {
					continue;
				}
				return Err(tg::error!(
					!error,
					"failed to read the sandbox process usage"
				));
			}
			for value in [
				usage.ri_user_time,
				usage.ri_system_time,
				usage.ri_child_user_time,
				usage.ri_child_system_time,
			] {
				cpu = cpu
					.checked_add(value)
					.ok_or_else(|| tg::error!("the sandbox CPU usage overflowed"))?;
			}
			memory = memory
				.checked_add(usage.ri_resident_size)
				.ok_or_else(|| tg::error!("the sandbox memory usage overflowed"))?;
			let mut children = vec![0_i32; 1024];
			loop {
				let bytes = i32::try_from(children.len() * std::mem::size_of::<libc::pid_t>())
					.map_err(|_| tg::error!("the sandbox process list is too large"))?;
				// SAFETY: The writable buffer is valid for the specified byte count.
				let count =
					unsafe { libc::proc_listchildpids(pid, children.as_mut_ptr().cast(), bytes) };
				if count < 0 {
					return Err(tg::error!("failed to list the sandbox child processes"));
				}
				let count = usize::try_from(count).unwrap() / std::mem::size_of::<libc::pid_t>();
				if count < children.len() {
					children.truncate(count);
					pending.extend(children.into_iter().filter(|pid| *pid > 0));
					break;
				}
				children.resize(children.len() * 2, 0);
			}
		}
		Ok(Snapshot { cpu, memory })
	}
}
