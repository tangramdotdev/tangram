use {crate::serve, tangram_client::prelude::*};

pub(crate) fn spawn(
	arg: &crate::Arg,
	serve_arg: &serve::Arg,
	network: Option<&crate::network::Network>,
) -> tg::Result<tokio::process::Child> {
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
		.arg("--firewall")
		.arg(arg.firewall.to_string())
		.arg("--path")
		.arg(&arg.path)
		.arg("--rootfs-path")
		.arg(&arg.rootfs_path)
		.arg("--tangram-path")
		.arg(&arg.tangram_path)
		.arg("--url")
		.arg(serve_arg.url.to_string());
	if let Some(snapshot) = &vm.snapshot {
		command.arg("--snapshot").arg(snapshot);
	}
	if let Some(network) = network {
		match network {
			crate::network::Network::Host => {
				return Err(tg::error!("vm sandboxes do not support host networking"));
			},
			crate::network::Network::Passt(_) => {
				let host_ip = network
					.host_ip()
					.ok_or_else(|| tg::error!("missing host IP address"))?;
				let guest_ip = network
					.guest_ip()
					.ok_or_else(|| tg::error!("missing guest IP address"))?;
				command.arg("--network").arg(network.kind());
				command.arg("--host-ip").arg(host_ip.to_string());
				command.arg("--guest-ip").arg(guest_ip.to_string());
				for server in &arg.dns {
					command.arg("--dns").arg(server.to_string());
				}
			},
			crate::network::Network::Pasta(_) => {
				return Err(tg::error!("vm sandboxes do not support pasta networking"));
			},
			crate::network::Network::Tap(_) => {
				let host_ip = network
					.host_ip()
					.ok_or_else(|| tg::error!("missing host IP address"))?;
				let guest_ip = network
					.guest_ip()
					.ok_or_else(|| tg::error!("missing guest IP address"))?;
				command.arg("--network").arg(network.kind());
				command.arg("--host-ip").arg(host_ip.to_string());
				command.arg("--guest-ip").arg(guest_ip.to_string());
				for server in &arg.dns {
					command.arg("--dns").arg(server.to_string());
				}
			},
			crate::network::Network::Veth(_) => {
				return Err(tg::error!("vm sandboxes do not support veth networking"));
			},
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
	let ports = arg
		.network
		.as_ref()
		.map(crate::Network::ports)
		.unwrap_or_default();
	for port in ports {
		command.arg("--port").arg(port.to_string());
	}
	command
		.kill_on_drop(true)
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit());
	command
		.spawn()
		.map_err(|error| tg::error!(!error, "failed to spawn sandbox vm"))
}
