use {
	std::{
		os::fd::OwnedFd,
		path::{Path, PathBuf},
	},
	tangram_client::prelude::*,
};

pub struct Cgroup {
	path: PathBuf,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct Options {
	pub cpu: Option<u64>,
	pub memory: Option<u64>,
	pub memory_oom_group: bool,
}

impl Cgroup {
	pub fn new(name: &str, options: Options) -> tg::Result<Self> {
		let root = Path::new("/sys/fs/cgroup");
		if !root.join("cgroup.controllers").exists() {
			return Err(tg::error!("cgroup v2 is not available"));
		}
		let current = std::fs::read_to_string("/proc/self/cgroup")
			.map_err(|source| tg::error!(!source, "failed to read the current cgroup"))?;
		let current = current
			.lines()
			.find_map(|line| {
				let (_, path) = line.split_once("::")?;
				Some(path.trim().to_owned())
			})
			.unwrap_or_else(|| "/".to_owned());
		let current = root.join(current.trim_start_matches('/'));
		let path = current.join(sanitize_name(name));
		std::fs::create_dir(&path).map_err(|source| {
			tg::error!(
				!source,
				path = %path.display(),
				"failed to create the cgroup"
			)
		})?;

		if let Some(cpu) = options.cpu {
			let quota = cpu
				.checked_mul(100_000)
				.ok_or_else(|| tg::error!("sandbox cpu is too large"))?;
			let cpu_max = path.join("cpu.max");
			write_file(&cpu_max, format!("{quota} 100000\n").as_bytes()).map_err(|source| {
				tg::error!(
					!source,
					path = %cpu_max.display(),
					"failed to set cpu.max"
				)
			})?;
		}

		if let Some(memory) = options.memory {
			let memory_max = path.join("memory.max");
			write_file(&memory_max, format!("{memory}\n").as_bytes()).map_err(|source| {
				tg::error!(
					!source,
					path = %memory_max.display(),
					"failed to set memory.max"
				)
			})?;
		}

		if options.memory_oom_group {
			let oom_group = path.join("memory.oom.group");
			if oom_group.exists() {
				write_file(&oom_group, b"1\n").map_err(|source| {
					tg::error!(
						!source,
						path = %oom_group.display(),
						"failed to set memory.oom.group"
					)
				})?;
			}
		}

		Ok(Self { path })
	}

	pub fn open_fd(&self) -> tg::Result<OwnedFd> {
		use std::os::unix::fs::OpenOptionsExt as _;
		let file = std::fs::OpenOptions::new()
			.read(true)
			.custom_flags(libc::O_PATH | libc::O_DIRECTORY)
			.open(&self.path)
			.map_err(|source| {
				tg::error!(
					!source,
					path = %self.path.display(),
					"failed to open the cgroup directory",
				)
			})?;
		Ok(OwnedFd::from(file))
	}

	pub fn move_self(&self) -> tg::Result<()> {
		let path = self.path.join("cgroup.procs");
		write_file(&path, b"0\n").map_err(|source| {
			tg::error!(
				!source,
				path = %path.display(),
				"failed to move the process into the cgroup"
			)
		})
	}
}

impl Drop for Cgroup {
	fn drop(&mut self) {
		std::fs::remove_dir(&self.path)
			.inspect_err(
				|error| tracing::error!(%error, path = %self.path.display(), "failed to remove cgrup"),
			)
			.ok();
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

fn write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
	let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
	std::io::Write::write_all(&mut file, bytes)
}
