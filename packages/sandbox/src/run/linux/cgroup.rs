use {
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

pub struct Cgroup {
	path: PathBuf,
}

pub fn create(name: &str, memory_oom_group: bool) -> tg::Result<Cgroup> {
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
	if memory_oom_group {
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
	Ok(Cgroup { path })
}

pub fn move_self(cgroup: &Cgroup) -> tg::Result<()> {
	let path = cgroup.path.join("cgroup.procs");
	write_file(&path, b"0\n").map_err(|source| {
		tg::error!(
			!source,
			path = %path.display(),
			"failed to move the process into the cgroup"
		)
	})
}

pub fn remove(cgroup: &Cgroup) {
	std::fs::remove_dir(&cgroup.path).ok();
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
