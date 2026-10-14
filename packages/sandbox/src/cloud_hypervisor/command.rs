use {
	crate::{cloud_hypervisor::run, serve},
	tangram_client::prelude::*,
};

pub(crate) fn command(
	arg: &crate::Arg,
	serve_arg: &serve::Arg,
	network: Option<&crate::network::Network>,
) -> tg::Result<tokio::process::Command> {
	if !serve_arg.library_paths.is_empty() {
		return Err(tg::error!(
			"cloud-hypervisor does not support additional library paths"
		));
	}
	let crate::Backend::CloudHypervisor(config) = &arg.backend else {
		unreachable!()
	};
	let (network_kind, host_ip, guest_ip) = match network {
		None => (None, None, None),
		Some(crate::network::Network::Passt(network)) => (
			Some(run::NetworkKind::Passt),
			Some(network.host_ip()),
			Some(network.guest_ip()),
		),
		Some(crate::network::Network::Tap(network)) => (
			Some(run::NetworkKind::Tap),
			Some(network.host_ip()),
			Some(network.guest_ip()),
		),
		Some(
			crate::network::Network::Host
			| crate::network::Network::Pasta(_)
			| crate::network::Network::Veth(_),
		) => return Err(tg::error!("unsupported cloud-hypervisor networking")),
	};
	let cpu = arg
		.cpu
		.map(|cpu| crate::cpu::threads(cpu, arg.cpu_pool.as_ref()))
		.transpose()?;
	let launch = run::Arg {
		cloud_hypervisor_path: config.cloud_hypervisor_path.clone(),
		create_snapshot: None,
		cpu,
		dax: config.dax,
		dns: arg.dns.clone(),
		firewall: arg.firewall,
		guest_ip,
		host_ip,
		hostname: arg.hostname.clone(),
		http2: arg.http2,
		image_path: config.image_path.clone(),
		index: arg.index,
		kernel_path: config.kernel_path.clone(),
		max_cpu: config.max_cpu,
		max_memory: config.max_memory,
		memory: arg.memory.map(|memory| memory.max),
		mounts: arg.mounts.clone(),
		network: network_kind,
		network_interface_timeout: config.network_interface_timeout,
		network_startup_timeout: arg.network_startup_timeout,
		path: arg.path.clone(),
		ports: arg
			.network
			.as_ref()
			.map(crate::Network::ports)
			.unwrap_or_default()
			.to_owned(),
		ready_timeout: config.ready_timeout,
		rootfs_path: arg.rootfs_path.clone(),
		serial_connect_interval: config.serial_connect_interval,
		serial_connect_timeout: config.serial_connect_timeout,
		snapshot: config.snapshot.clone(),
		snapshot_cpu: config.snapshot_cpu,
		snapshot_memory: config.snapshot_memory,
		store_path: arg.store_path.clone(),
		tangram_path: arg.tangram_path.clone(),
		url: serve_arg.url.clone(),
	};
	let path = arg.path.join("launch.json");
	crate::run::write_config(&path, &launch)?;
	let mut command = tokio::process::Command::new(&arg.tangram_path);
	command
		.arg("sandbox")
		.arg("run")
		.arg("--backend")
		.arg("cloud-hypervisor")
		.arg("--config")
		.arg(path)
		.kill_on_drop(true)
		.stdin(std::process::Stdio::null())
		.stdout(std::process::Stdio::inherit())
		.stderr(std::process::Stdio::inherit());
	Ok(command)
}
