use {
	rustix::fs::{AtFlags, unlinkat},
	std::{
		os::fd::OwnedFd,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

pub struct Cgroup {
	name: String,
	parent: OwnedFd,
	path: PathBuf,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Options {
	pub cpu: Option<u64>,
	pub memory: Option<u64>,
	pub memory_oom_group: bool,
	pub pids: Option<u64>,
}

impl Cgroup {
	pub fn new(name: &str, options: Options) -> tg::Result<Self> {
		use std::os::unix::fs::OpenOptionsExt as _;
		let root = Path::new("/sys/fs/cgroup");
		if !root.join("cgroup.controllers").exists() {
			return Err(tg::error!("cgroup v2 is not available"));
		}
		let current = std::fs::read_to_string("/proc/self/cgroup")
			.map_err(|error| tg::error!(!error, "failed to read the current cgroup"))?;
		let current = current
			.lines()
			.find_map(|line| {
				let (_, path) = line.split_once("::")?;
				Some(path.trim().to_owned())
			})
			.unwrap_or_else(|| "/".to_owned());
		let current = root.join(current.trim_start_matches('/'));
		if options.pids.is_some() {
			validate_controller_enabled(&current, "pids")?;
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
		std::fs::create_dir(&path).map_err(|error| {
			tg::error!(
				!error,
				path = %path.display(),
				"failed to create the cgroup"
			)
		})?;
		let cgroup = Self {
			name,
			parent,
			path: path.clone(),
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
		use std::os::unix::fs::OpenOptionsExt as _;
		let file = std::fs::OpenOptions::new()
			.read(true)
			.custom_flags(libc::O_PATH | libc::O_DIRECTORY)
			.open(&self.path)
			.map_err(|error| {
				tg::error!(
					!error,
					path = %self.path.display(),
					"failed to open the cgroup directory",
				)
			})?;
		Ok(OwnedFd::from(file))
	}

	pub fn move_self(&self) -> tg::Result<()> {
		let path = self.path.join("cgroup.procs");
		write_file(&path, b"0\n").map_err(|error| {
			tg::error!(
				!error,
				path = %path.display(),
				"failed to move the process into the cgroup"
			)
		})
	}
}

impl Drop for Cgroup {
	fn drop(&mut self) {
		if let Err(error) = unlinkat(&self.parent, self.name.as_str(), AtFlags::REMOVEDIR) {
			tracing::error!(%error, path = %self.path.display(), "failed to remove cgroup");
		}
	}
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

fn validate_controller_enabled(path: &Path, controller: &str) -> tg::Result<()> {
	let path = path.join("cgroup.subtree_control");
	let controllers = std::fs::read_to_string(&path).map_err(|error| {
		tg::error!(
			!error,
			path = %path.display(),
			"failed to read the enabled cgroup controllers"
		)
	})?;
	if !controllers
		.split_ascii_whitespace()
		.any(|value| value == controller)
	{
		return Err(tg::error!(
			path = %path.display(),
			"the {controller} cgroup controller is not enabled for child cgroups; launch tangram in a cgroup with the controller delegated and enabled"
		));
	}

	Ok(())
}

fn write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
	let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
	std::io::Write::write_all(&mut file, bytes)
}
