use {
	crate::{Server, config::RunnerBackend},
	tangram_client::prelude::*,
};

impl Server {
	pub(crate) fn resolve_sandbox_backend(&self) -> tg::Result<tangram_sandbox::Backend> {
		let backend = match &self.config.runner.backend {
			RunnerBackend::Builtin => tangram_sandbox::Backend::Builtin,
			RunnerBackend::Bubblewrap { path } => {
				tangram_sandbox::Backend::Bubblewrap { path: path.clone() }
			},
			RunnerBackend::CloudHypervisor(config) => {
				let image_path = self
					.sandbox_assets
					.image
					.clone()
					.ok_or_else(|| tg::error!("the cloud-hypervisor image is unavailable"))?;
				let snapshot = self.sandbox_assets.snapshot.clone();
				tangram_sandbox::Backend::CloudHypervisor(tangram_sandbox::CloudHypervisor {
					cloud_hypervisor_path: config.cloud_hypervisor_path.clone(),
					dax: config.dax.map(|dax| dax.window_size as u64),
					image_path,
					kernel_path: config.kernel_path.clone(),
					max_cpu: config.max_cpu,
					max_memory: config.max_memory,
					network_interface_timeout: config.network_interface_timeout,
					ready_timeout: config.ready_timeout,
					serial_connect_interval: config.serial_connect_interval,
					serial_connect_timeout: config.serial_connect_timeout,
					snapshot,
					snapshot_cpu: config.snapshot_cpu,
					snapshot_memory: config.snapshot_memory,
				})
			},
			RunnerBackend::Gvisor { path, platform } => tangram_sandbox::Backend::Gvisor {
				path: path.clone(),
				platform: *platform,
			},
			RunnerBackend::Seatbelt => tangram_sandbox::Backend::Seatbelt,
		};
		if cfg!(target_os = "linux") && matches!(backend, tangram_sandbox::Backend::Seatbelt) {
			return Err(tg::error!("the seatbelt backend requires macos"));
		}
		if cfg!(target_os = "macos") && !matches!(backend, tangram_sandbox::Backend::Seatbelt) {
			return Err(tg::error!("the selected backend requires linux"));
		}
		Ok(backend)
	}

	pub(crate) fn sandbox_resources(&self) -> tangram_sandbox::Resources {
		let config = &self.config.runner.resources;
		tangram_sandbox::Resources {
			cgroup_readonly: config.harden,
			filesystem_project_id: None,
			gid_map: config.gid_map.as_ref().map(Into::into),
			max_duration: config.max_duration(),
			max_filesystem_inodes: config.max_filesystem_inodes(),
			max_filesystem_size: config.max_filesystem_size(),
			max_open_files: config.max_open_files(),
			max_pids: config.max_pids(),
			memory_swap: config.memory_swap(),
			seccomp: config.seccomp(),
			uid_map: config.uid_map.as_ref().map(Into::into),
		}
	}
}
