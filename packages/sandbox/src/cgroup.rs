use {
	rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, mkdirat, openat, unlinkat},
	std::{
		ffi::{CStr, CString},
		io::{Read as _, Write as _},
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
		path::{Path, PathBuf},
		sync::Arc,
		time::{Duration, Instant},
	},
	tangram_client::prelude::*,
};

pub(crate) const GUEST: &CStr = c"guest";

pub(crate) const SIDECARS: &CStr = c"sidecars";

const INIT: &CStr = c"init";

pub struct Cgroup {
	cleanup: Arc<Cleanup>,
	cleanup_interval: Duration,
	cleanup_timeout: Duration,
	path: PathBuf,
	pin: Option<crate::cpu::Pin>,
	share: Option<crate::cpu::Share>,
	stopped: bool,
}

#[derive(Debug)]
pub struct Handle {
	directory: OwnedFd,
}

struct Cleanup {
	directory: OwnedFd,
	name: String,
	parent: OwnedFd,
}

#[derive(Clone, Debug)]
pub struct Options {
	pub cleanup_interval: Duration,
	pub cleanup_timeout: Duration,
	pub cpu: Option<u64>,
	pub cpu_affinity: bool,
	pub cpu_request: Option<tg::sandbox::Cpu>,
	pub cpu_shares: Option<crate::cpu::Shares>,
	pub cpu_threads: Option<crate::cpu::Threads>,
	pub memory: Option<tg::sandbox::Resource<u64>>,
	pub memory_oom_group: bool,
	pub pids: Option<u64>,
}

pub(crate) fn create(arg: &crate::Arg) -> tg::Result<Cgroup> {
	let resources = &arg.resources;
	let options = Options {
		cleanup_interval: arg.cgroup_cleanup_interval,
		cleanup_timeout: arg.cgroup_cleanup_timeout,
		cpu: arg
			.cpu
			.map(tg::sandbox::Cpu::threads)
			.transpose()?
			.flatten(),
		cpu_affinity: arg.cpu_affinity,
		cpu_request: arg.cpu,
		cpu_shares: arg.cpu_shares.clone(),
		cpu_threads: arg.cpu_threads.clone(),
		memory: arg.memory,
		memory_oom_group: true,
		pids: resources.max_pids,
	};
	let name = arg
		.path
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("sandbox");
	let cgroup = Cgroup::new(name, &options)?;
	Ok(cgroup)
}

pub(crate) fn set_affinity_before_exec(command: &mut std::process::Command, cpus: &[usize]) {
	use std::os::unix::process::CommandExt as _;

	// Build the CPU set before forking, so that the child only makes a system call.
	// SAFETY: A zeroed cpu_set_t is an empty CPU set.
	let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
	for cpu in cpus {
		// SAFETY: CPU_SET ignores CPUs beyond the set's size.
		unsafe { libc::CPU_SET(*cpu, &mut set) };
	}

	// SAFETY: The pre_exec closure only invokes the async-signal-safe sched_setaffinity system call.
	unsafe {
		command.pre_exec(move || {
			if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &raw const set)
				!= 0
			{
				return Err(std::io::Error::last_os_error());
			}
			Ok(())
		});
	}
}

pub(crate) fn enter_before_exec(command: &mut std::process::Command, cgroup: libc::c_int) {
	use std::os::unix::process::CommandExt as _;

	// SAFETY: The pre_exec closure only invokes async-signal-safe system calls.
	unsafe {
		command.pre_exec(move || {
			let fd = libc::openat(
				cgroup,
				c"cgroup.procs".as_ptr(),
				libc::O_WRONLY | libc::O_CLOEXEC,
			);
			if fd < 0 {
				return Err(std::io::Error::last_os_error());
			}
			let bytes = b"0\n";
			let result = libc::write(fd, bytes.as_ptr().cast(), bytes.len());
			let error = std::io::Error::last_os_error();
			libc::close(fd);
			if result != isize::try_from(bytes.len()).unwrap() {
				return Err(error);
			}
			Ok(())
		});
	}
}

impl Cgroup {
	pub fn new(name: &str, options: &Options) -> tg::Result<Self> {
		use std::os::unix::fs::OpenOptionsExt as _;

		// Resolve the current cgroup.
		let root = Path::new("/sys/fs/cgroup");
		let current = current(root)?;

		// Select the required resource controllers.
		let mut controllers = Vec::new();
		if options.cpu.is_some() || options.cpu_request.is_some() {
			controllers.push("cpu");
		}
		if options.memory.is_some() || options.memory_oom_group {
			controllers.push("memory");
		}
		if options.pids.is_some() {
			controllers.push("pids");
		}

		// Select the parent cgroup.
		let current = resolve_parent(root, &current, &controllers)?;

		// Open the parent cgroup.
		let name = sanitize_name(name);

		// Hold the parent directory open so the cgroup can be removed even after the sandbox replaces the cgroup mount it was resolved through.
		let parent = std::fs::OpenOptions::new()
			.read(true)
			.custom_flags(libc::O_DIRECTORY | libc::O_PATH)
			.open(&current)
			.map_err(|error| {
				tg::error!(
					!error,
					path = %current.display(),
					"failed to open the cgroup directory",
				)
			})?;
		let parent = OwnedFd::from(parent);
		let path = current.join(&name);

		// Create the sandbox cgroup and cleanup state.
		std::fs::create_dir(&path).map_err(|error| {
			tg::error!(
				!error,
				path = %path.display(),
				"failed to create the cgroup"
			)
		})?;
		let directory = std::fs::OpenOptions::new()
			.read(true)
			.custom_flags(libc::O_DIRECTORY | libc::O_PATH)
			.open(&path)
			.map_err(|error| {
				tg::error!(
					!error,
					path = %path.display(),
					"failed to open the cgroup directory",
				)
			})?;
		let directory = OwnedFd::from(directory);

		let cleanup = Cleanup {
			directory,
			name: name.clone(),
			parent,
		};
		let cleanup = Arc::new(cleanup);

		// Pin the sandbox to the least used hardware threads, so that it sees as many as its CPU maximum.
		let pin = if options.cpu_affinity
			&& let Some(max) = options.cpu_request.and_then(|cpu| cpu.max)
		{
			let cpus = available_cpus()?;
			let width = usize::try_from(max.threads())
				.map_err(|_| tg::error!("the CPU affinity is too wide"))?;
			let threads = options.cpu_threads.clone().unwrap_or_default();
			Some(threads.pin(&cpus, width)?)
		} else {
			None
		};

		let mut cgroup = Self {
			cleanup,
			cleanup_interval: options.cleanup_interval,
			cleanup_timeout: options.cleanup_timeout,
			path: path.clone(),
			pin,
			share: None,
			stopped: false,
		};

		// Configure the CPU limit.
		if let Some(cpu) = options.cpu {
			let quota = match options.cpu_request.and_then(|request| request.max) {
				Some(max) => max.millis().checked_mul(1000),
				None => cpu.checked_mul(1_000_000),
			}
			.ok_or_else(|| tg::error!("the sandbox CPU is too large"))?;
			let cpu_max = path.join("cpu.max");
			write_file(&cpu_max, format!("{quota} 1000000\n").as_bytes()).map_err(|error| {
				tg::error!(
					!error,
					path = %cpu_max.display(),
					"failed to set cpu.max"
				)
			})?;
		}

		// Add the sandbox to the CPU shares, which weight each sandbox by its minimum and divide the rest of the CPU in proportion to the capacity above each minimum.
		if let Some(shares) = &options.cpu_shares
			&& path.join("cpu.weight").exists()
		{
			let cpu = options.cpu_request.unwrap_or_default();
			cgroup.share = Some(shares.insert(cpu, cgroup.handle()?)?);
		}

		// Configure the memory limits and disable swap.
		if let Some(max) = options.memory.and_then(|memory| memory.max) {
			let memory_max = path.join("memory.max");
			write_file(&memory_max, format!("{max}\n").as_bytes()).map_err(|error| {
				tg::error!(
					!error,
					path = %memory_max.display(),
					"failed to set memory.max"
				)
			})?;
		}

		if let Some(memory) = options.memory {
			write_file(&path.join("memory.min"), memory.min.to_string().as_bytes())
				.map_err(|error| tg::error!(!error, "failed to set memory.min"))?;
		}

		if options.memory_oom_group {
			let oom_group = path.join("memory.oom.group");
			if oom_group.exists() {
				write_file(&oom_group, b"1\n").map_err(|error| {
					tg::error!(
						!error,
						path = %oom_group.display(),
						"failed to set memory.oom.group"
					)
				})?;
			}
		}

		if controllers.contains(&"memory") {
			let memory_swap_max = path.join("memory.swap.max");
			if memory_swap_max.exists() {
				write_file(&memory_swap_max, b"0\n").map_err(|error| {
					tg::error!(
						!error,
						path = %memory_swap_max.display(),
						"failed to set memory.swap.max"
					)
				})?;
			}
		}

		// Configure the process limit.
		if let Some(pids) = options.pids {
			let pids_max = path.join("pids.max");
			write_file(&pids_max, format!("{pids}\n").as_bytes()).map_err(|error| {
				tg::error!(
					!error,
					path = %pids_max.display(),
					"failed to set pids.max"
				)
			})?;
		}

		Ok(cgroup)
	}

	/// Get the hardware threads that the sandbox's processes are pinned to.
	#[must_use]
	pub(crate) fn affinity(&self) -> Option<&[usize]> {
		self.pin.as_ref().map(crate::cpu::Pin::cpus)
	}

	#[must_use]
	pub(crate) fn cpu_share(&self) -> Option<crate::cpu::ShareId> {
		self.share.as_ref().map(crate::cpu::Share::id)
	}

	pub(crate) fn set_cpu_lender(&self, lender: Option<crate::cpu::ShareId>) -> tg::Result<()> {
		if let Some(share) = &self.share {
			share.set_lender(lender)?;
		}
		Ok(())
	}

	pub fn handle(&self) -> tg::Result<Handle> {
		let directory = self.cleanup.directory.try_clone().map_err(|error| {
			tg::error!(
				!error,
				path = %self.path.display(),
				"failed to clone the cgroup directory descriptor",
			)
		})?;
		Ok(Handle { directory })
	}

	pub fn oom_kill(&self) -> tg::Result<u64> {
		let contents = read_file_at(&self.cleanup.directory, c"memory.events")
			.map_err(|error| tg::error!(!error, "failed to read the cgroup memory events"))?;
		let count = parse_oom_kill(&contents)?;
		Ok(count)
	}

	pub(crate) fn child(&self, name: &CStr) -> tg::Result<Handle> {
		mkdirat(&self.cleanup.directory, name, Mode::from_raw_mode(0o755)).map_err(|error| {
			tg::error!(
				!error,
				path = %self.path.display(),
				"failed to create the child cgroup"
			)
		})?;
		let flags = OFlags::CLOEXEC | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::PATH;
		let directory = openat(&self.cleanup.directory, name, flags, Mode::empty())
			.map_err(|error| tg::error!(!error, "failed to open the child cgroup"))?;
		let handle = Handle { directory };
		Ok(handle)
	}

	pub fn cleanup(mut self) -> tg::Result<()> {
		drop(self.share.take());
		self.stop()?;
		self.stopped = true;

		Ok(())
	}

	pub(crate) fn stop(&self) -> tg::Result<()> {
		write_file_at(&self.cleanup.directory, c"cgroup.kill", b"1\n")
			.map_err(|error| tg::error!(!error, "failed to stop the sandbox cgroup"))?;
		self.wait_until_empty()?;
		Ok(())
	}

	fn wait_until_empty(&self) -> tg::Result<()> {
		let deadline = Instant::now() + self.cleanup_timeout;
		loop {
			let contents =
				read_file_at(&self.cleanup.directory, c"cgroup.events").map_err(|error| {
					tg::error!(
						!error,
						path = %self.path.display(),
						"failed to read the cgroup events"
					)
				})?;
			if !parse_populated(&contents)? {
				return Ok(());
			}
			if Instant::now() >= deadline {
				return Err(tg::error!(
					path = %self.path.display(),
					"timed out waiting for the cgroup to become empty"
				));
			}
			std::thread::sleep(self.cleanup_interval);
		}
	}
}

impl Handle {
	pub(crate) fn current() -> tg::Result<Self> {
		use std::os::unix::fs::OpenOptionsExt as _;

		let path = current(Path::new("/sys/fs/cgroup"))?;
		let directory = std::fs::OpenOptions::new()
			.read(true)
			.custom_flags(libc::O_DIRECTORY | libc::O_PATH)
			.open(&path)
			.map_err(|error| {
				tg::error!(
					!error,
					path = %path.display(),
					"failed to open the cgroup directory",
				)
			})?;
		let directory = OwnedFd::from(directory);
		let handle = Self { directory };
		Ok(handle)
	}

	#[cfg(test)]
	pub(crate) unsafe fn from_raw_fd(fd: libc::c_int) -> Self {
		// SAFETY: The caller transfers ownership of a valid directory descriptor.
		let directory = unsafe { OwnedFd::from_raw_fd(fd) };
		Self { directory }
	}

	pub fn open_fd(&self) -> tg::Result<OwnedFd> {
		self.directory
			.try_clone()
			.map_err(|error| tg::error!(!error, "failed to clone the cgroup directory descriptor"))
	}

	pub(crate) fn read(&self, name: &CStr) -> tg::Result<String> {
		let contents = read_file_at(&self.directory, name)
			.map_err(|error| tg::error!(!error, "failed to read the sandbox usage"))?;
		Ok(contents)
	}

	pub(crate) fn write(&self, name: &CStr, bytes: &[u8]) -> tg::Result<()> {
		write_file_at(&self.directory, name, bytes)
			.map_err(|error| tg::error!(!error, "failed to update the sandbox cgroup"))?;
		Ok(())
	}

	pub fn move_self(&self) -> tg::Result<()> {
		write_file_at(&self.directory, c"cgroup.procs", b"0\n")
			.map_err(|error| tg::error!(!error, "failed to move the process into the cgroup"))
	}

	pub(crate) fn delegate(&self) -> tg::Result<()> {
		// Move the process into a leaf so that the cgroup can enable controllers for its children.
		mkdirat(&self.directory, INIT, Mode::from_raw_mode(0o755))
			.map_err(|error| tg::error!(!error, "failed to create the init cgroup"))?;
		write_file_at(&self.directory, c"init/cgroup.procs", b"0\n").map_err(|error| {
			tg::error!(!error, "failed to move the process into the init cgroup")
		})?;
		self.enable_controllers()?;
		Ok(())
	}

	pub(crate) fn enable_controllers(&self) -> tg::Result<()> {
		let controllers =
			read_file_at(&self.directory, c"cgroup.controllers").map_err(|error| {
				tg::error!(!error, "failed to read the available cgroup controllers")
			})?;
		let controllers = controllers
			.split_ascii_whitespace()
			.map(|controller| format!("+{controller}"))
			.collect::<Vec<_>>()
			.join(" ");
		if !controllers.is_empty() {
			write_file_at(
				&self.directory,
				c"cgroup.subtree_control",
				controllers.as_bytes(),
			)
			.map_err(|error| tg::error!(!error, "failed to enable the cgroup controllers"))?;
		}
		Ok(())
	}
}

impl Default for Options {
	fn default() -> Self {
		Self {
			cleanup_interval: Duration::from_millis(10),
			cleanup_timeout: Duration::from_secs(5),
			cpu: None,
			cpu_affinity: false,
			cpu_request: None,
			cpu_shares: None,
			cpu_threads: None,
			memory: None,
			memory_oom_group: false,
			pids: None,
		}
	}
}

impl Drop for Cgroup {
	fn drop(&mut self) {
		drop(self.share.take());
		if self.stopped {
			return;
		}
		if let Err(error) = self.stop() {
			tracing::error!(%error, path = %self.path.display(), "failed to stop the cgroup");
		}
	}
}

impl Drop for Cleanup {
	fn drop(&mut self) {
		if let Err(error) = remove_descendants(&self.directory) {
			tracing::error!(%error, name = %self.name, "failed to remove the descendant cgroups");
		}
		if let Err(error) = unlinkat(&self.parent, self.name.as_str(), AtFlags::REMOVEDIR) {
			tracing::error!(%error, name = %self.name, "failed to remove the cgroup");
		}
	}
}

/// Get the hardware threads that the current process may run on.
fn available_cpus() -> tg::Result<Vec<usize>> {
	// SAFETY: A zeroed cpu_set_t is an empty CPU set, which sched_getaffinity fills.
	let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
	// SAFETY: The set is a valid cpu_set_t of the given size.
	let result =
		unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &raw mut set) };
	if result != 0 {
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to get the available CPUs"
		));
	}
	let cpus = (0..usize::try_from(libc::CPU_SETSIZE).unwrap())
		// SAFETY: Each CPU is within the set's size.
		.filter(|cpu| unsafe { libc::CPU_ISSET(*cpu, &set) })
		.collect();
	Ok(cpus)
}

fn current(root: &Path) -> tg::Result<PathBuf> {
	if !root.join("cgroup.controllers").exists() {
		return Err(tg::error!("cgroup v2 is not available"));
	}
	let contents = std::fs::read_to_string("/proc/self/cgroup")
		.map_err(|error| tg::error!(!error, "failed to read the current cgroup"))?;
	let current = contents
		.lines()
		.find_map(|line| line.strip_prefix("0::"))
		.ok_or_else(|| tg::error!("the process is not running in a unified cgroup v2 hierarchy"))?;
	let current = root.join(current.trim_start_matches('/'));
	Ok(current)
}

fn remove_descendants(directory: &OwnedFd) -> tg::Result<()> {
	// Open the cgroup through its descriptor because the sandbox can replace its cgroup mount.
	let flags = OFlags::CLOEXEC | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::RDONLY;
	let directory = openat(directory, c".", flags, Mode::empty())
		.map_err(|error| tg::error!(!error, "failed to open the cgroup directory"))?;
	let entries = Dir::read_from(&directory)
		.map_err(|error| tg::error!(!error, "failed to read the cgroup directory"))?;

	// Remove the descendants depth first, removing each cgroup after its children.
	let mut current = (directory, entries, CString::default());
	let mut ancestors = Vec::<(OwnedFd, Dir, CString)>::new();
	loop {
		if let Some(entry) = current.1.next() {
			let entry = entry
				.map_err(|error| tg::error!(!error, "failed to read a cgroup directory entry"))?;
			let name = entry.file_name();
			if entry.file_type() != FileType::Directory || name == c"." || name == c".." {
				continue;
			}
			let directory = openat(&current.0, name, flags, Mode::empty())
				.map_err(|error| tg::error!(!error, "failed to open a descendant cgroup"))?;
			let entries = Dir::read_from(&directory)
				.map_err(|error| tg::error!(!error, "failed to read a descendant cgroup"))?;
			let name = name.to_owned();
			ancestors.push(current);
			current = (directory, entries, name);
		} else {
			let Some(parent) = ancestors.pop() else {
				break;
			};
			unlinkat(&parent.0, current.2.as_c_str(), AtFlags::REMOVEDIR)
				.map_err(|error| tg::error!(!error, "failed to remove a descendant cgroup"))?;
			current = parent;
		}
	}

	Ok(())
}

fn parse_oom_kill(contents: &str) -> tg::Result<u64> {
	let count = contents
		.lines()
		.filter_map(|line| line.split_once(' '))
		.find(|(key, _)| *key == "oom_kill")
		.ok_or_else(|| tg::error!("the cgroup memory events are missing the OOM kill count"))?
		.1
		.parse()
		.map_err(|error| tg::error!(!error, "invalid cgroup OOM kill count"))?;
	Ok(count)
}

fn parse_populated(contents: &str) -> tg::Result<bool> {
	contents
		.lines()
		.filter_map(|line| line.split_once(' '))
		.find(|(key, _)| *key == "populated")
		.map(|(_, value)| match value {
			"0" => Ok(false),
			"1" => Ok(true),
			_ => Err(tg::error!(
				value = %value,
				"invalid populated value in cgroup.events"
			)),
		})
		.transpose()?
		.ok_or_else(|| tg::error!("cgroup.events does not contain populated"))
}

fn sanitize_name(name: &str) -> String {
	let mut output = String::new();
	for char in name.chars() {
		if char.is_ascii_alphanumeric() || matches!(char, '-' | '_') {
			output.push(char);
		} else {
			output.push('-');
		}
	}
	if output.is_empty() {
		output.push_str("sandbox");
	}
	output
}

fn resolve_parent(root: &Path, current: &Path, controllers: &[&str]) -> tg::Result<PathBuf> {
	// A populated leaf cannot distribute memory to children. Use its delegated parent for sibling sandbox cgroups.
	let parent = current.parent().filter(|parent| parent.starts_with(root));
	for path in std::iter::once(current).chain(parent) {
		let enabled = std::fs::read_to_string(path.join("cgroup.subtree_control"))
			.map_err(|error| tg::error!(!error, path = %path.display(), "failed to read the enabled cgroup controllers"))?;
		if !controllers.iter().all(|controller| {
			enabled
				.split_ascii_whitespace()
				.any(|value| value == *controller)
		}) {
			continue;
		}
		let directory = std::fs::File::open(path)
			.map_err(|error| tg::error!(!error, "failed to open the cgroup parent"))?;
		let access = rustix::fs::Access::WRITE_OK | rustix::fs::Access::EXEC_OK;
		let flags = AtFlags::EACCESS;
		if rustix::fs::accessat(&directory, ".", access, flags).is_err()
			|| rustix::fs::accessat(
				&directory,
				"cgroup.procs",
				rustix::fs::Access::WRITE_OK,
				flags,
			)
			.is_err()
		{
			continue;
		}
		return Ok(path.to_owned());
	}
	Err(tg::error!(
		controllers = %controllers.join(", "),
		path = %current.display(),
		"no writable cgroup parent has the required controllers enabled; launch tangram in a leaf of a delegated cgroup with the required controllers enabled"
	))
}

fn write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
	let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
	file.write_all(bytes)
}

fn open_file_at(
	directory: &OwnedFd,
	name: &CStr,
	flags: libc::c_int,
) -> std::io::Result<std::fs::File> {
	let fd = unsafe {
		libc::openat(
			directory.as_raw_fd(),
			name.as_ptr(),
			flags | libc::O_CLOEXEC,
		)
	};
	if fd < 0 {
		return Err(std::io::Error::last_os_error());
	}

	// SAFETY: The successful openat call returned a new owned file descriptor.
	let file = unsafe { std::fs::File::from_raw_fd(fd) };

	Ok(file)
}

fn read_file_at(directory: &OwnedFd, name: &CStr) -> std::io::Result<String> {
	let mut file = open_file_at(directory, name, libc::O_RDONLY)?;
	let mut contents = String::new();
	file.read_to_string(&mut contents)?;

	Ok(contents)
}

fn write_file_at(directory: &OwnedFd, name: &CStr, bytes: &[u8]) -> std::io::Result<()> {
	let mut file = open_file_at(directory, name, libc::O_WRONLY)?;
	file.write_all(bytes)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn select_delegated_parent_for_populated_leaf() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		let leaf = temp.path().join("runner");
		std::fs::create_dir_all(&leaf).unwrap();
		for path in [temp.path(), leaf.as_path()] {
			std::fs::write(path.join("cgroup.procs"), "").unwrap();
		}
		std::fs::write(leaf.join("cgroup.subtree_control"), "").unwrap();
		std::fs::write(
			temp.path().join("cgroup.subtree_control"),
			"cpu memory pids",
		)
		.unwrap();
		assert_eq!(
			resolve_parent(temp.path(), &leaf, &["cpu", "memory", "pids"]).unwrap(),
			temp.path()
		);
		assert_eq!(resolve_parent(temp.path(), &leaf, &[]).unwrap(), leaf);
		std::fs::write(temp.path().join("cgroup.subtree_control"), "cpu pids").unwrap();
		assert!(resolve_parent(temp.path(), &leaf, &["memory"]).is_err());
	}

	#[test]
	fn parse_oom_kill_value() {
		assert_eq!(parse_oom_kill("oom 3\noom_kill 0\n").unwrap(), 0);
		assert_eq!(parse_oom_kill("oom 3\noom_kill 4\n").unwrap(), 4);
		assert!(parse_oom_kill("oom 3\n").is_err());
		assert!(parse_oom_kill("oom_kill invalid\n").is_err());
	}

	#[test]
	fn parse_populated_value() {
		assert!(!parse_populated("populated 0\nfrozen 0\n").unwrap());
		assert!(parse_populated("populated 1\nfrozen 0\n").unwrap());
		assert!(parse_populated("frozen 0\n").is_err());
		assert!(parse_populated("populated 2\n").is_err());
	}
}
