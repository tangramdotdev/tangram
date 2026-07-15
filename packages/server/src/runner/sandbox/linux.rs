use {
	crate::{Server, Session, temp::Temp},
	tangram_client::prelude::*,
};

impl Session {
	pub(super) async fn ensure_vm_isolation(
		&self,
		isolation: &tangram_sandbox::Isolation,
	) -> tg::Result<()> {
		let tangram_sandbox::Isolation::Vm(vm) = isolation else {
			return Ok(());
		};
		let Some(snapshot_path) = vm.snapshot.as_deref() else {
			return Ok(());
		};

		let mut image_created = self.server.sandbox_vm_image_lock.lock().await;
		if !*image_created {
			let _file_lock = acquire_vm_lock(&self.server.path).await?;
			let arg = tangram_sandbox::vm::image::Arg {
				image_path: vm.image_path.clone(),
				path: self.server.sandbox_container_root.clone(),
				tangram_path: self.server.tangram_path.clone(),
			};
			let created =
				tokio::task::spawn_blocking(move || tangram_sandbox::vm::image::ensure(&arg))
					.await
					.map_err(|error| tg::error!(!error, "the vm image task panicked"))??;
			if created {
				std::fs::remove_dir_all(snapshot_path).ok();
				std::fs::remove_file(snapshot_path).ok();
			}
			*image_created = true;
		}
		self.server
			.ensure_vm_snapshot(snapshot_path, &vm.kernel_path, vm)
			.await?;
		Ok(())
	}
}

impl Server {
	async fn ensure_vm_snapshot(
		&self,
		snapshot_path: &std::path::Path,
		kernel_path: &std::path::Path,
		vm: &tangram_sandbox::VmIsolation,
	) -> tg::Result<()> {
		if snapshot_path.exists() {
			return Ok(());
		}
		let _guard = self.sandbox_vm_snapshot_lock.lock().await;
		if snapshot_path.exists() {
			return Ok(());
		}
		let _file_lock = acquire_vm_lock(&self.path).await?;
		if snapshot_path.exists() {
			return Ok(());
		}
		if let Some(parent) = snapshot_path.parent() {
			tokio::fs::create_dir_all(parent).await.map_err(|error| {
				tg::error!(!error, path = %parent.display(), "failed to create the vm snapshot parent directory")
			})?;
		}
		let temp = Temp::new(self);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to create the vm snapshot temp directory")
			})?;
		let _vfs = {
			let socket = temp.path().join("vfs.sock");
			crate::vfs::Server::start_virtiofs(self, &socket, vm.dax)
				.await
				.map_err(|error| tg::error!(!error, "failed to start the artifacts vfs"))?
		};
		let image_path = self.sandbox_vm_image.as_ref().ok_or_else(|| {
			tg::error!(
				"cannot create the vm snapshot without an image; ensure vm isolation is configured"
			)
		})?;
		let snapshot_id = tg::sandbox::Id::new();
		tracing::info!(
			snapshot = %snapshot_path.display(),
			sandbox = %snapshot_id,
			"creating vm snapshot",
		);
		let firewall = match self.config.sandbox.network.firewall {
			crate::config::SandboxNetworkFirewall::Iptables => tangram_sandbox::Firewall::Iptables,
			crate::config::SandboxNetworkFirewall::Nft => tangram_sandbox::Firewall::Nft,
		};
		let mut command = tokio::process::Command::new(&self.tangram_path);
		command
			.arg("sandbox")
			.arg("vm")
			.arg("run")
			.arg("--create-snapshot")
			.arg(snapshot_path)
			.arg("--id")
			.arg(snapshot_id.to_string())
			.arg("--artifacts-path")
			.arg(self.artifacts_path())
			.arg("--firewall")
			.arg(firewall.to_string())
			.arg("--kernel-path")
			.arg(kernel_path)
			.arg("--max-cpu")
			.arg(vm.max_cpu.to_string())
			.arg("--max-memory")
			.arg(vm.max_memory.to_string())
			.arg("--rootfs-path")
			.arg(&self.sandbox_container_root)
			.arg("--image-path")
			.arg(image_path)
			.arg("--snapshot-cpu")
			.arg(vm.snapshot_cpu.to_string())
			.arg("--snapshot-memory")
			.arg(vm.snapshot_memory.to_string())
			.arg("--tangram-path")
			.arg(&self.tangram_path)
			.arg("--path")
			.arg(temp.path())
			.arg("--url")
			.arg("http+vsock://2:6748");
		if let Some(dax) = vm.dax {
			command.arg("--dax").arg(dax.to_string());
		}
		if let Some(path) = &vm.cloud_hypervisor_path {
			command.arg("--cloud-hypervisor-path").arg(path);
		}
		let status = command
			.status()
			.await
			.map_err(|error| tg::error!(!error, "failed to spawn the snapshot process"))?;
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

async fn acquire_vm_lock(data_dir: &std::path::Path) -> tg::Result<std::fs::File> {
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
