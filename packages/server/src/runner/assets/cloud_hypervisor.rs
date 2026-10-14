use {
	super::Assets,
	crate::{Server, temp::Temp},
	tangram_client::prelude::*,
};

impl Assets {
	pub(crate) async fn ensure_cloud_hypervisor(
		&self,
		server: &Server,
		backend: &tangram_sandbox::Backend,
	) -> tg::Result<()> {
		let tangram_sandbox::Backend::CloudHypervisor(vm) = backend else {
			return Ok(());
		};
		let Some(snapshot_path) = vm.snapshot.as_deref() else {
			return Ok(());
		};

		let mut image_created = self.image_ready.lock().await;
		if !*image_created {
			let _file_lock = acquire_cloud_hypervisor_lock(&server.path).await?;
			let arg = tangram_sandbox::cloud_hypervisor::image::Arg {
				build_attempts: server.config.sandbox.build_attempts,
				image_path: vm.image_path.clone(),
				path: self.root.clone(),
			};
			let created = tokio::task::spawn_blocking(move || {
				tangram_sandbox::cloud_hypervisor::image::ensure(&arg)
			})
			.await
			.map_err(|error| tg::error!(!error, "the vm image task panicked"))??;
			if created {
				std::fs::remove_dir_all(snapshot_path).ok();
				std::fs::remove_file(snapshot_path).ok();
			}
			*image_created = true;
		}
		self.ensure_cloud_hypervisor_snapshot(server, snapshot_path, &vm.kernel_path, vm)
			.await?;
		Ok(())
	}

	async fn ensure_cloud_hypervisor_snapshot(
		&self,
		server: &Server,
		snapshot_path: &std::path::Path,
		kernel_path: &std::path::Path,
		vm: &tangram_sandbox::CloudHypervisor,
	) -> tg::Result<()> {
		if snapshot_path.exists() {
			return Ok(());
		}
		let _guard = self.snapshot_lock.lock().await;
		if snapshot_path.exists() {
			return Ok(());
		}
		let _file_lock = acquire_cloud_hypervisor_lock(&server.path).await?;
		if snapshot_path.exists() {
			return Ok(());
		}
		if let Some(parent) = snapshot_path.parent() {
			tokio::fs::create_dir_all(parent).await.map_err(|error| {
				tg::error!(!error, path = %parent.display(), "failed to create the vm snapshot parent directory")
			})?;
		}
		let temp = Temp::new(server);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to create the vm snapshot temp directory")
			})?;
		let _vfs = {
			let socket = temp.path().join("vfs.sock");
			// Run the shared VM snapshot mount as root; per-sandbox mounts enforce permissions after resume.
			let principal = Some(tg::Principal::Root);
			crate::vfs::Server::start_virtiofs(
				server,
				&socket,
				vm.dax,
				crate::Origin::Host,
				principal,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the store VFS"))?
		};
		let image_path = self.image.as_ref().ok_or_else(|| {
			tg::error!(
				"cannot create the vm snapshot without an image; ensure vm isolation is configured"
			)
		})?;
		let id = tg::sandbox::Id::new();
		let index = server.runner.state.create_sandbox_index();
		tracing::info!(
			snapshot = %snapshot_path.display(),
			sandbox = %id,
			"creating vm snapshot",
		);
		let config = &server.config.sandbox;
		let firewall = match config.network.firewall {
			crate::config::SandboxNetworkFirewall::Iptables => tangram_sandbox::Firewall::Iptables,
			crate::config::SandboxNetworkFirewall::Nft => tangram_sandbox::Firewall::Nft,
		};
		let launch = tangram_sandbox::cloud_hypervisor::run::Arg {
			cloud_hypervisor_path: vm.cloud_hypervisor_path.clone(),
			create_snapshot: Some(snapshot_path.to_owned()),
			cpu: None,
			dax: vm.dax,
			dns: config.network.dns.clone(),
			firewall,
			guest_ip: None,
			host_ip: None,
			hostname: None,
			http2: config.http2,
			image_path: image_path.clone(),
			index,
			kernel_path: kernel_path.to_owned(),
			max_cpu: vm.max_cpu,
			max_memory: vm.max_memory,
			memory: None,
			mounts: Vec::new(),
			network: None,
			network_interface_timeout: vm.network_interface_timeout,
			network_startup_timeout: config.network.startup_timeout,
			path: temp.path().to_owned(),
			ports: Vec::new(),
			ready_timeout: vm.ready_timeout,
			rootfs_path: self.root.clone(),
			serial_connect_interval: vm.serial_connect_interval,
			serial_connect_timeout: vm.serial_connect_timeout,
			snapshot: None,
			snapshot_cpu: vm.snapshot_cpu,
			snapshot_memory: vm.snapshot_memory,
			store_path: server.store_path(),
			tangram_path: server.tangram_path.clone(),
			url: "http+vsock://2:6748".parse().unwrap(),
		};
		let launch = tangram_sandbox::run::Arg {
			backend: tangram_sandbox::run::Backend::CloudHypervisor(launch),
		};
		let mut command = tokio::process::Command::new(&server.tangram_path);
		command.kill_on_drop(true);
		let config = tangram_sandbox::run::configure_command(&mut command, &launch)?;
		let child = command
			.spawn()
			.map_err(|error| tg::error!(!error, "failed to spawn the snapshot process"));
		let configuration = config.finish();
		let mut child = child?;
		configuration?;
		let status = child
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "failed to wait for the snapshot process"))?;
		if !status.success() {
			return Err(tg::error!(
				%status,
				snapshot = %snapshot_path.display(),
				"the snapshot process exited with a non-zero status",
			));
		}
		tracing::info!(
			snapshot = %snapshot_path.display(),
			"vm snapshot created",
		);
		Ok(())
	}
}

async fn acquire_cloud_hypervisor_lock(data_dir: &std::path::Path) -> tg::Result<std::fs::File> {
	let lock_path = data_dir.join(".tangram/vm.lock");
	tokio::task::spawn_blocking(move || -> tg::Result<std::fs::File> {
		if let Some(parent) = lock_path.parent() {
			std::fs::create_dir_all(parent).map_err(
				|error| tg::error!(!error, path = %parent.display(), "failed to create the vm lock parent"),
			)?;
		}
		let lock = std::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.open(&lock_path)
			.map_err(
				|error| tg::error!(!error, path = %lock_path.display(), "failed to open the vm lock"),
			)?;
		// SAFETY: The file descriptor remains valid for the duration of the call.
		let ret = unsafe { libc::flock(std::os::fd::AsRawFd::as_raw_fd(&lock), libc::LOCK_EX) };
		if ret != 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(!error, "failed to acquire the vm lock"));
		}
		Ok(lock)
	})
	.await
	.map_err(|error| tg::error!(!error, "the vm lock task panicked"))?
}
