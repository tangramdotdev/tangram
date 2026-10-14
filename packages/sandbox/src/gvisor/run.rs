use {
	rustix::thread::{
		CapabilitySet, capabilities, configure_capability_in_ambient_set, set_capabilities,
	},
	std::{
		os::unix::{fs::FileTypeExt as _, process::CommandExt as _},
		path::PathBuf,
	},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Arg {
	pub gid: u32,
	pub hostname: Option<String>,
	pub mounts: Vec<tg::sandbox::Mount>,
	pub path: PathBuf,
	pub platform: crate::Platform,
	pub root: Option<PathBuf>,
	pub uid: u32,
}

#[derive(Clone, Debug)]
pub struct Prepared {
	pub mounts: Vec<tg::sandbox::Mount>,
	pub network: Option<Network>,
	pub rlimit_nofile: Option<u64>,
	pub root: PathBuf,
}

#[derive(Clone, Copy, Debug)]
pub enum Network {
	Host,
	Sandbox,
}

pub fn run(command_arg: &crate::run::Command, arg: &Arg, prepared: &Prepared) -> tg::Result<u8> {
	// Resolve the prepared root and isolate the helper mounts.
	let root = &prepared.root;

	crate::linux::unshare(
		libc::CLONE_NEWNS,
		"failed to isolate the gvisor helper mounts",
	)?;

	// Mount proc for the launcher's PID and user namespaces before runsc reexecutes its helpers.
	// SAFETY: The pointers reference valid, terminated strings and no data pointer is required.
	if unsafe {
		libc::mount(
			c"proc".as_ptr(),
			c"/proc".as_ptr(),
			c"proc".as_ptr(),
			libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
			std::ptr::null(),
		)
	} != 0
	{
		return Err(tg::error!(
			source = std::io::Error::last_os_error(),
			"failed to mount proc for gvisor"
		));
	}

	if matches!(prepared.network, Some(Network::Sandbox)) {
		retain_setup_capabilities()?;
	}

	// Write an OCI bundle referencing the prepared filesystem.
	let bundle = root.parent().unwrap().join("gvisor");
	std::fs::create_dir(&bundle)
		.map_err(|error| tg::error!(!error, "failed to create the gvisor bundle"))?;
	let spec = spec(command_arg, arg, prepared)?;
	crate::run::write_config(&bundle.join("config.json"), &spec)?;
	let state = bundle.join("state");

	// Run the setup with capabilities scoped to the prepared user namespace.
	let mut command = std::process::Command::new(&arg.path);
	command
		.arg(format!("--platform={}", arg.platform.name()))
		.arg("--rootless=false")
		.arg("--ignore-cgroups=true")
		.arg("--host-uds=all")
		.arg("--overlay2=none")
		.arg("--root")
		.arg(state);
	let network = match prepared.network {
		None => "none",
		Some(Network::Host) => "host",
		Some(Network::Sandbox) => "sandbox",
	};
	command
		.arg(format!("--network={network}"))
		.arg("run")
		.arg("--bundle")
		.arg(bundle)
		.arg("sandbox");
	let error = command.exec();

	Err(tg::error!(!error, "failed to execute gvisor"))
}

fn spec(
	command: &crate::run::Command,
	arg: &Arg,
	prepared: &Prepared,
) -> tg::Result<serde_json::Value> {
	// Collect the prepared process settings.
	let root = &prepared.root;
	let args = &command.args;
	let env = command
		.env
		.iter()
		.map(|(key, value)| format!("{key}={value}"))
		.collect::<Vec<_>>();

	// Create the filesystem mounts.
	let mut mounts = vec![
		serde_json::json!({ "destination": "/proc", "type": "proc", "source": "proc" }),
		serde_json::json!({ "destination": "/dev", "type": "tmpfs", "source": "tmpfs" }),
		serde_json::json!({ "destination": "/dev/shm", "type": "tmpfs", "source": "tmpfs", "options": ["mode=1777"] }),
		serde_json::json!({ "destination": "/dev/pts", "type": "devpts", "source": "devpts" }),
		serde_json::json!({ "destination": "/sys", "type": "sysfs", "source": "sysfs", "options": ["ro"] }),
	];
	for mount in &prepared.mounts {
		let source = &mount.source;
		let metadata = std::fs::metadata(source)
			.map_err(|error| tg::error!(!error, "failed to inspect the prepared gvisor mount"))?;

		// Expose socket nodes through the root gofer because runsc cannot open them as bind mount sources.
		if metadata.file_type().is_socket() {
			continue;
		}
		let options = if mount.readonly {
			vec!["rbind", "ro"]
		} else {
			vec!["rbind", "rw"]
		};
		mounts.push(serde_json::json!({ "destination": mount.target, "type": "bind", "source": source, "options": options }));
	}
	mounts.sort_by_key(|mount| mount["destination"].as_str().unwrap().matches('/').count());

	// Create the OCI process configuration.
	let mut process = serde_json::json!({
		"args": args, "cwd": command.cwd, "env": env, "noNewPrivileges": true, "terminal": false,
		"user": { "uid": arg.uid, "gid": arg.gid },
		"capabilities": { "bounding": [], "effective": [], "inheritable": [], "permitted": [], "ambient": [] },
	});
	if let Some(limit) = prepared.rlimit_nofile {
		process["rlimits"] =
			serde_json::json!([{ "type": "RLIMIT_NOFILE", "hard": limit, "soft": limit }]);
	}

	// Read the setup process identity.
	// SAFETY: The identity getters have no preconditions.
	let setup_uid = unsafe { libc::geteuid() };
	let setup_gid = unsafe { libc::getegid() };

	// Create the OCI specification.
	let spec = serde_json::json!({
		"ociVersion": "1.2.0", "hostname": arg.hostname.clone().unwrap_or_else(|| "tangram".into()),
		"root": { "path": root, "readonly": false }, "process": process, "mounts": mounts,
		"linux": {
			"gidMappings": [{ "containerID": arg.gid, "hostID": setup_gid, "size": 1 }],
			"uidMappings": [{ "containerID": arg.uid, "hostID": setup_uid, "size": 1 }],
			"namespaces": [{ "type": "user" }, { "type": "mount" }, { "type": "pid" }, { "type": "ipc" }, { "type": "uts" }],
		},
	});

	Ok(spec)
}

fn retain_setup_capabilities() -> tg::Result<()> {
	let mut sets = capabilities(None)
		.map_err(|error| tg::error!(!error, "failed to read the gvisor setup capabilities"))?;
	let setup = CapabilitySet::NET_ADMIN | CapabilitySet::NET_RAW | CapabilitySet::SYS_ADMIN;
	sets.inheritable |= setup;
	set_capabilities(None, sets)
		.map_err(|error| tg::error!(!error, "failed to retain the gvisor setup capabilities"))?;
	for capability in [
		CapabilitySet::NET_ADMIN,
		CapabilitySet::NET_RAW,
		CapabilitySet::SYS_ADMIN,
	] {
		configure_capability_in_ambient_set(capability, true).map_err(|error| {
			tg::error!(!error, "failed to inherit the gvisor setup capabilities")
		})?;
	}
	Ok(())
}
