use {
	rustix::fs::{AtFlags, unlinkat},
	std::{
		ffi::CStr,
		io::{Read as _, Write as _},
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
		path::{Path, PathBuf},
		sync::{Arc, Mutex},
		time::{Duration, Instant},
	},
	tangram_client::prelude::*,
};

static MEMORY: Mutex<()> = Mutex::new(());

pub struct Cgroup {
	allocation: Option<crate::cpu::Allocation>,
	cleanup: Arc<Cleanup>,
	cleanup_interval: Duration,
	cleanup_timeout: Duration,
	name: String,
	path: PathBuf,
	stopped: bool,
	swap_allocation: Option<crate::budget::Allocation>,
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
	pub cpu_parent: Option<crate::cpu::Lease>,
	pub cpu_pool: Option<crate::cpu::Pool>,
	pub cpu_request: Option<tg::sandbox::Cpu>,
	pub memory: Option<tg::sandbox::Resource<u64>>,
	pub memory_oom_group: bool,
	pub memory_swap: Option<u64>,
	pub pids: Option<u64>,
	pub swap: Option<tg::sandbox::Resource<u64>>,
	pub swap_pool: Option<crate::budget::Pool>,
}

pub(crate) fn create(arg: &crate::Arg) -> tg::Result<Cgroup> {
	let resources = &arg.resources;
	let options = Options {
		cleanup_interval: arg.cgroup_cleanup_interval,
		cleanup_timeout: arg.cgroup_cleanup_timeout,
		cpu: arg
			.cpu
			.map(|cpu| crate::cpu::threads(cpu, arg.cpu_pool.as_ref()))
			.transpose()?,
		cpu_affinity: arg.cpu_affinity,
		cpu_parent: arg.cpu_parent.clone(),
		cpu_pool: arg.cpu_pool.clone(),
		cpu_request: arg.cpu,
		memory: arg.memory,
		memory_oom_group: true,
		memory_swap: arg.swap.map(|swap| swap.max).or(resources.memory_swap),
		pids: resources.max_pids,
		swap: arg.swap,
		swap_pool: arg.swap_pool.clone(),
	};
	let name = arg
		.path
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("sandbox");
	let cgroup = Cgroup::new(name, &options)?;
	Ok(cgroup)
}

pub(crate) fn enter_before_exec(command: &mut tokio::process::Command, cgroup: libc::c_int) {
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
		let _memory = MEMORY
			.lock()
			.map_err(|error| tg::error!(%error, "the memory admission lock is poisoned"))?;
		let root = Path::new("/sys/fs/cgroup");
		if !root.join("cgroup.controllers").exists() {
			return Err(tg::error!("cgroup v2 is not available"));
		}
		let contents = std::fs::read_to_string("/proc/self/cgroup")
			.map_err(|error| tg::error!(!error, "failed to read the current cgroup"))?;
		let current = contents
			.lines()
			.find_map(|line| line.strip_prefix("0::"))
			.ok_or_else(|| {
				tg::error!("the process is not running in a unified cgroup v2 hierarchy")
			})?;
		let current = root.join(current.trim_start_matches('/'));
		let mut controllers = Vec::new();
		if options.cpu.is_some() {
			controllers.push("cpu");
		}
		if options.memory.is_some() || options.memory_oom_group || options.memory_swap.is_some() {
			controllers.push("memory");
		}
		if options.pids.is_some() {
			controllers.push("pids");
		}
		let allocation = options
			.cpu_pool
			.as_ref()
			.map(|pool| {
				let cpu = options.cpu_request.unwrap_or(1.into());
				match &options.cpu_parent {
					Some(parent) => pool.borrow(parent, cpu),
					None => pool.allocate(cpu),
				}
			})
			.transpose()?;
		if allocation.is_some() || (options.cpu_affinity && options.cpu_request.is_some()) {
			controllers.push("cpuset");
		}
		let current = if let Some(allocation) = &allocation {
			allocation.parent().to_owned()
		} else {
			resolve_parent(root, &current, &controllers)?
		};
		if let Some(memory) = options.memory
			&& memory.min > 0
		{
			validate_memory(root, &current, memory.min)?;
		}

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
		let swap = match (options.swap, &options.swap_pool) {
			(Some(resource), Some(pool)) => Some(pool.allocate(resource)?),
			(Some(resource), None) if resource.max > 0 => {
				return Err(tg::error!("swap limits require a runner swap pool"));
			},
			_ => None,
		};
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
		let cgroup = Self {
			allocation,
			cleanup,
			cleanup_interval: options.cleanup_interval,
			cleanup_timeout: options.cleanup_timeout,
			name,
			path: path.clone(),
			stopped: false,
			swap_allocation: swap,
		};

		if let Some(allocation) = &cgroup.allocation {
			let mems = std::fs::read_to_string(current.join("cpuset.mems.effective"))
				.map_err(|error| tg::error!(!error, "failed to read the CPU pool memory nodes"))?;
			write_file(&path.join("cpuset.mems"), mems.as_bytes())
				.map_err(|error| tg::error!(!error, "failed to set the sandbox memory nodes"))?;
			allocation.bind(cgroup.handle()?)?;
		}

		if cgroup.allocation.is_none()
			&& options.cpu_affinity
			&& let Some(cpu) = options.cpu_request
		{
			let cpus = crate::cpu::parse_list(
				&std::fs::read_to_string(current.join("cpuset.cpus.effective"))
					.map_err(|error| tg::error!(!error, "failed to read the available CPUs"))?,
			)?;
			let width = usize::try_from(cpu.max.threads())
				.map_err(|_| tg::error!("the CPU affinity is too wide"))?;
			if width > cpus.len() {
				return Err(tg::error!(
					"the CPU maximum exceeds the available hardware threads"
				));
			}
			let offset = cgroup.name.bytes().map(usize::from).sum::<usize>() % cpus.len();
			let cpus = cpus
				.iter()
				.copied()
				.cycle()
				.skip(offset)
				.take(width)
				.collect();
			cgroup
				.handle()?
				.write(c"cpuset.cpus", crate::cpu::format_list(&cpus).as_bytes())?;
		}

		if let Some(cpu) = options.cpu {
			let quota = match options.cpu_request {
				Some(request) => request.max.millis().checked_mul(1000),
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

		if let Some(memory) = options.memory {
			let memory_max = path.join("memory.max");
			write_file(&memory_max, format!("{}\n", memory.max).as_bytes()).map_err(|error| {
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

		if let Some(memory_swap) = cgroup
			.swap_allocation
			.as_ref()
			.map(crate::budget::Allocation::limit)
			.or(options.memory_swap)
		{
			let memory_swap_max = path.join("memory.swap.max");
			write_file(&memory_swap_max, format!("{memory_swap}\n").as_bytes()).map_err(
				|error| {
					tg::error!(
						!error,
						path = %memory_swap_max.display(),
						"failed to set memory.swap.max"
					)
				},
			)?;
		}

		if let Some(swap) = &cgroup.swap_allocation {
			let cleanup = cgroup.cleanup.clone();
			let limit_handle = cgroup.handle()?;
			let usage = Box::new(move || {
				read_file_at(&cleanup.directory, c"memory.swap.current")
					.map_err(|error| tg::error!(!error, "failed to read the sandbox swap usage"))?
					.trim()
					.parse()
					.map_err(|error| tg::error!(!error, "invalid swap usage"))
			});
			let limit = Box::new(move |limit: u64| {
				limit_handle.write(c"memory.swap.max", limit.to_string().as_bytes())
			});
			let control = crate::budget::Control { limit, usage };
			swap.bind(control)?;
		}

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

	#[must_use]
	pub(crate) fn cpu_lease(&self) -> Option<crate::cpu::Lease> {
		self.allocation.as_ref().map(crate::cpu::Allocation::lease)
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

	pub fn cleanup(mut self) -> tg::Result<()> {
		self.stop()?;
		self.stopped = true;
		// Retained swap allocations keep the directory alive until their data is released.
		drop(self.allocation.take());
		drop(self.swap_allocation.take());
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
	/// Takes ownership of a cgroup directory descriptor inherited from the parent.
	pub unsafe fn from_raw_fd(fd: libc::c_int) -> Self {
		// SAFETY: The caller guarantees ownership of the inherited descriptor.
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
}

impl Default for Options {
	fn default() -> Self {
		Self {
			cleanup_interval: Duration::from_millis(10),
			cleanup_timeout: Duration::from_secs(5),
			cpu: None,
			cpu_affinity: false,
			cpu_parent: None,
			cpu_pool: None,
			cpu_request: None,
			memory: None,
			memory_oom_group: false,
			memory_swap: None,
			pids: None,
			swap: None,
			swap_pool: None,
		}
	}
}

impl Drop for Cgroup {
	fn drop(&mut self) {
		if self.stopped {
			return;
		}
		if let Err(error) = self.stop() {
			tracing::error!(%error, path = %self.path.display(), "failed to stop the cgroup");
			// Retain the core reservation when processes could still be executing on it.
			if let Some(allocation) = self.allocation.take() {
				allocation.retain();
			}
			return;
		}
		drop(self.allocation.take());
		drop(self.swap_allocation.take());
	}
}

impl Drop for Cleanup {
	fn drop(&mut self) {
		if let Err(error) = unlinkat(&self.parent, self.name.as_str(), AtFlags::REMOVEDIR) {
			tracing::error!(%error, name = %self.name, "failed to remove the cgroup");
		}
	}
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

fn validate_memory(root: &Path, parent: &Path, additional: u64) -> tg::Result<()> {
	for ancestor in parent
		.ancestors()
		.take_while(|ancestor| *ancestor != root && ancestor.starts_with(root))
	{
		let minimum = read_minimum(ancestor)?;
		let children = std::fs::read_dir(ancestor).map_err(|error| {
			tg::error!(!error, "failed to read the memory protection hierarchy")
		})?;
		let mut required = if ancestor == parent { additional } else { 0 };
		for child in children {
			let child = child
				.map_err(|error| tg::error!(!error, "failed to read a memory protection entry"))?;
			if !child
				.file_type()
				.map_err(|error| tg::error!(!error, "failed to inspect a memory protection entry"))?
				.is_dir()
			{
				continue;
			}
			required = required
				.checked_add(read_minimum(&child.path())?)
				.ok_or_else(|| tg::error!("the memory reservation overflowed"))?;
		}
		if required > minimum {
			return Err(tg::error!(
				"memory minima exceed the runner ancestor protection"
			));
		}
	}
	Ok(())
}

fn read_minimum(path: &Path) -> tg::Result<u64> {
	std::fs::read_to_string(path.join("memory.min"))
		.map_err(|error| tg::error!(!error, "failed to read the runner memory protection"))?
		.trim()
		.parse()
		.map_err(|error| tg::error!(!error, "invalid runner memory protection"))
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
	fn retained_swap_keeps_its_directory_until_usage_is_zero() {
		use std::sync::atomic::{AtomicU64, Ordering};
		let temp = tangram_util::fs::Temp::new().unwrap();
		let path = temp.path().join("sandbox");
		std::fs::create_dir_all(&path).unwrap();
		let directory = std::fs::File::open(&path).unwrap().into();
		let parent = std::fs::File::open(temp.path()).unwrap().into();
		let cleanup = Cleanup {
			directory,
			name: "sandbox".to_owned(),
			parent,
		};
		let cleanup = Arc::new(cleanup);
		let pool = crate::budget::Pool::new(100, 0, 1);
		let allocation = pool.allocate(100.into()).unwrap();
		let usage = Arc::new(AtomicU64::new(90));
		let retained = cleanup.clone();
		let current = usage.clone();
		let control = crate::budget::Control {
			limit: Box::new(|_| Ok(())),
			usage: Box::new(move || {
				rustix::fs::fstat(&retained.directory)
					.map_err(|error| tg::error!(!error, "failed to inspect the directory"))?;
				Ok(current.load(Ordering::SeqCst))
			}),
		};
		allocation.bind(control).unwrap();
		drop(cleanup);
		drop(allocation);
		assert!(path.exists());
		assert!(pool.allocate(tg::sandbox::Resource::new(20, 20)).is_err());
		usage.store(0, Ordering::SeqCst);
		assert!(pool.allocate(tg::sandbox::Resource::new(20, 20)).is_ok());
		assert!(!path.exists());
	}

	#[test]
	fn memory_admission_accounts_for_sibling_minima() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		let parent = temp.path().join("runner");
		let child = parent.join("sandbox");
		std::fs::create_dir_all(&child).unwrap();
		std::fs::write(parent.join("memory.min"), "100").unwrap();
		std::fs::write(child.join("memory.min"), "60").unwrap();
		assert!(validate_memory(temp.path(), &parent, 40).is_ok());
		assert!(validate_memory(temp.path(), &parent, 41).is_err());
		let leaf = child.join("leaf");
		std::fs::create_dir(&leaf).unwrap();
		std::fs::write(child.join("memory.min"), "120").unwrap();
		std::fs::write(leaf.join("memory.min"), "0").unwrap();
		assert!(validate_memory(temp.path(), &child, 1).is_err());
	}

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
	fn parse_populated_value() {
		assert!(!parse_populated("populated 0\nfrozen 0\n").unwrap());
		assert!(parse_populated("populated 1\nfrozen 0\n").unwrap());
		assert!(parse_populated("frozen 0\n").is_err());
		assert!(parse_populated("populated 2\n").is_err());
	}
}
