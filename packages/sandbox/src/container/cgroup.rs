use {
	rustix::fs::{AtFlags, unlinkat},
	std::{
		ffi::CStr,
		io::{Read as _, Write as _},
		os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd},
		path::{Path, PathBuf},
		time::{Duration, Instant},
	},
	tangram_client::prelude::*,
};

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const CLEANUP_WAIT_INTERVAL: Duration = Duration::from_millis(10);

pub struct Cgroup {
	directory: OwnedFd,
	name: String,
	parent: OwnedFd,
	path: PathBuf,
	removed: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Options {
	pub cpu: Option<u64>,
	pub memory: Option<u64>,
	pub memory_oom_group: bool,
	pub memory_swap: Option<u64>,
	pub pids: Option<u64>,
}

impl Cgroup {
	pub fn new(name: &str, options: Options) -> tg::Result<Self> {
		use std::os::unix::fs::OpenOptionsExt as _;
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
		let current = resolve_parent(root, &current, &controllers)?;
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
		let cgroup = Self {
			directory,
			name,
			parent,
			path: path.clone(),
			removed: false,
		};

		if let Some(cpu) = options.cpu {
			let quota = cpu
				.checked_mul(100_000)
				.ok_or_else(|| tg::error!("sandbox cpu is too large"))?;
			let cpu_max = path.join("cpu.max");
			write_file(&cpu_max, format!("{quota} 100000\n").as_bytes()).map_err(|error| {
				tg::error!(
					!error,
					path = %cpu_max.display(),
					"failed to set cpu.max"
				)
			})?;
		}

		if let Some(memory) = options.memory {
			let memory_max = path.join("memory.max");
			write_file(&memory_max, format!("{memory}\n").as_bytes()).map_err(|error| {
				tg::error!(
					!error,
					path = %memory_max.display(),
					"failed to set memory.max"
				)
			})?;
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

		if let Some(memory_swap) = options.memory_swap {
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

	pub fn open_fd(&self) -> tg::Result<OwnedFd> {
		self.directory.try_clone().map_err(|error| {
			tg::error!(
				!error,
				path = %self.path.display(),
				"failed to clone the cgroup directory descriptor",
			)
		})
	}

	pub fn move_self(&self) -> tg::Result<()> {
		write_file_at(&self.directory, c"cgroup.procs", b"0\n").map_err(|error| {
			tg::error!(
				!error,
				path = %self.path.display(),
				"failed to move the process into the cgroup"
			)
		})
	}

	pub fn cleanup(mut self) -> tg::Result<()> {
		match write_file_at(&self.directory, c"cgroup.kill", b"1\n") {
			Ok(()) => (),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
			Err(error) => {
				return Err(tg::error!(
					!error,
					path = %self.path.display(),
					"failed to kill the processes in the cgroup"
				));
			},
		}
		self.wait_until_empty(CLEANUP_TIMEOUT)?;
		unlinkat(&self.parent, self.name.as_str(), AtFlags::REMOVEDIR).map_err(|error| {
			tg::error!(
				!error,
				path = %self.path.display(),
				"failed to remove the cgroup"
			)
		})?;
		self.removed = true;

		Ok(())
	}

	fn wait_until_empty(&self, timeout: Duration) -> tg::Result<()> {
		let deadline = Instant::now() + timeout;
		loop {
			let contents = read_file_at(&self.directory, c"cgroup.events").map_err(|error| {
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
			std::thread::sleep(CLEANUP_WAIT_INTERVAL);
		}
	}
}

impl Drop for Cgroup {
	fn drop(&mut self) {
		if self.removed {
			return;
		}
		if let Err(error) = unlinkat(&self.parent, self.name.as_str(), AtFlags::REMOVEDIR) {
			tracing::error!(%error, path = %self.path.display(), "failed to remove cgroup");
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
	fn parse_populated_value() {
		assert!(!parse_populated("populated 0\nfrozen 0\n").unwrap());
		assert!(parse_populated("populated 1\nfrozen 0\n").unwrap());
		assert!(parse_populated("frozen 0\n").is_err());
		assert!(parse_populated("populated 2\n").is_err());
	}
}
