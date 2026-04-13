use {super::run::Arg, tangram_client::prelude::*};

pub fn validate(arg: &Arg) -> tg::Result<()> {
	if !arg.unshare_all {
		if arg.as_pid_1 {
			return Err(tg::error!("--as-pid-1 requires --unshare-all"));
		}
		if arg.hostname.is_some() {
			return Err(tg::error!("--hostname requires --unshare-all"));
		}
		if has_mounts(arg) {
			return Err(tg::error!("mount operations require --unshare-all"));
		}
	}
	if arg.command.is_empty() {
		return Err(tg::error!("a command is required"));
	}
	if !arg.chdir.is_absolute() {
		return Err(tg::error!(
			path = %arg.chdir.display(),
			"the working directory must be an absolute path"
		));
	}
	if arg.cgroup_memory_oom_group && arg.cgroup.is_none() {
		return Err(tg::error!("--cgroup-memory-oom-group requires --cgroup"));
	}
	if arg.cgroup_cpu.is_some() && arg.cgroup.is_none() {
		return Err(tg::error!("--cgroup-cpu requires --cgroup"));
	}
	if arg.cgroup_memory.is_some() && arg.cgroup.is_none() {
		return Err(tg::error!("--cgroup-memory requires --cgroup"));
	}
	if arg.cgroup_cpu == Some(0) {
		return Err(tg::error!("--cgroup-cpu must be greater than zero"));
	}
	if arg.cgroup_memory == Some(0) {
		return Err(tg::error!("--cgroup-memory must be greater than zero"));
	}
	if !arg.overlays.is_empty() && arg.overlay_sources.is_empty() {
		return Err(tg::error!(
			"an overlay requires at least one overlay source"
		));
	}
	for path in &arg.overlay_sources {
		if !path.is_absolute() {
			return Err(tg::error!(
				path = %path.display(),
				"overlay sources must be absolute paths"
			));
		}
	}
	for overlay in &arg.overlays {
		for path in [&overlay.target, &overlay.upperdir, &overlay.workdir] {
			if !path.is_absolute() {
				return Err(tg::error!(
					path = %path.display(),
					"overlay paths must be absolute"
				));
			}
		}
	}
	let mut targets = std::collections::BTreeSet::new();
	for bind in arg.binds.iter().chain(&arg.ro_binds) {
		if !bind.source.is_absolute() || !bind.target.is_absolute() {
			return Err(tg::error!("bind mount paths must be absolute"));
		}
		if bind.target == std::path::Path::new("/") {
			return Err(tg::error!("bind mounts to / are not supported"));
		}
		if !targets.insert(bind.target.clone()) {
			return Err(tg::error!(
				target = %bind.target.display(),
				"duplicate mount targets are not supported"
			));
		}
	}
	for path in &arg.devs {
		if !path.is_absolute() {
			return Err(tg::error!(
				path = %path.display(),
				"mount targets must be absolute"
			));
		}
		if path == std::path::Path::new("/") {
			return Err(tg::error!("device mounts to / are not supported"));
		}
		if !targets.insert(path.clone()) {
			return Err(tg::error!(
				target = %path.display(),
				"duplicate mount targets are not supported"
			));
		}
	}
	for path in &arg.procs {
		if !path.is_absolute() {
			return Err(tg::error!(
				path = %path.display(),
				"mount targets must be absolute"
			));
		}
		if path == std::path::Path::new("/") {
			return Err(tg::error!("proc mounts to / are not supported"));
		}
		if !targets.insert(path.clone()) {
			return Err(tg::error!(
				target = %path.display(),
				"duplicate mount targets are not supported"
			));
		}
	}
	for path in &arg.tmpfs {
		if !path.is_absolute() {
			return Err(tg::error!(
				path = %path.display(),
				"mount targets must be absolute"
			));
		}
		if path == std::path::Path::new("/") {
			return Err(tg::error!("tmpfs mounts to / are not supported"));
		}
		if !targets.insert(path.clone()) {
			return Err(tg::error!(
				target = %path.display(),
				"duplicate mount targets are not supported"
			));
		}
	}
	for path in arg.overlays.iter().map(|overlay| &overlay.target) {
		if !path.is_absolute() {
			return Err(tg::error!(
				path = %path.display(),
				"mount targets must be absolute"
			));
		}
		if path == std::path::Path::new("/")
			&& arg
				.overlays
				.iter()
				.filter(|overlay| overlay.target == std::path::Path::new("/"))
				.count() > 1
		{
			return Err(tg::error!("multiple overlays to / are not supported"));
		}
		if path != std::path::Path::new("/") && !targets.insert(path.clone()) {
			return Err(tg::error!(
				target = %path.display(),
				"duplicate mount targets are not supported"
			));
		}
	}
	Ok(())
}

fn has_mounts(arg: &Arg) -> bool {
	!arg.binds.is_empty()
		|| !arg.ro_binds.is_empty()
		|| !arg.devs.is_empty()
		|| !arg.overlay_sources.is_empty()
		|| !arg.overlays.is_empty()
		|| !arg.procs.is_empty()
		|| !arg.tmpfs.is_empty()
}
