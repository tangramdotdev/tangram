use {crate::serve, tangram_client::prelude::*};

pub(crate) fn spawn(arg: &crate::Arg, serve_arg: &serve::Arg) -> tg::Result<tokio::process::Child> {
	if !serve_arg.library_paths.is_empty() {
		return Err(tg::error!(
			"vm sandboxes do not support additional library paths"
		));
	}

	let crate::Isolation::Vm(vm) = &arg.isolation else {
		unreachable!()
	};
	let mut command = tokio::process::Command::new(&arg.tangram_path);
	command.arg("sandbox").arg("vm").arg("run");
	command
		.arg("--id")
		.arg(arg.id.to_string())
		.arg("--artifacts-path")
		.arg(&arg.artifacts_path)
		.arg("--kernel-path")
		.arg(&vm.kernel_path)
		.arg("--path")
		.arg(&arg.path)
		.arg("--rootfs-path")
		.arg(&arg.rootfs_path)
		.arg("--tangram-path")
		.arg(&arg.tangram_path)
		.arg("--url")
		.arg(serve_arg.url.to_string());
	if arg.network {
		let host_ip = arg
			.host_ip
			.ok_or_else(|| tg::error!("expected a host IP"))?;
		let guest_ip = arg
			.guest_ip
			.ok_or_else(|| tg::error!("expected a guest IP"))?;
		command.arg("--network");
		command.arg("--host-ip").arg(host_ip.to_string());
		command.arg("--guest-ip").arg(guest_ip.to_string());
		for server in &arg.dns {
			command.arg("--dns").arg(server.to_string());
		}
	}
	if let Some(hostname) = &arg.hostname {
		command.arg("--hostname").arg(hostname);
	}
	if let Some(cpu) = arg.cpu {
		command.arg("--cpu").arg(cpu.to_string());
	}
	if let Some(memory) = arg.memory {
		command.arg("--memory").arg(memory.to_string());
	}
	if let Some(user) = &arg.user {
		command.arg("--user").arg(user);
	}
	for mount in &arg.mounts {
		command.arg("--mount").arg(mount.to_string());
	}
	command
		.kill_on_drop(true)
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit());
	command
		.spawn()
		.map_err(|source| tg::error!(!source, "failed to spawn sandbox vm"))
}
