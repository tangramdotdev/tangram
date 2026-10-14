use {
	crate::{Server, config::RunnerBackend},
	tangram_client::prelude::*,
};

impl Server {
	pub(crate) fn resolve_sandbox_backend(&self) -> tg::Result<tangram_sandbox::Backend> {
		let backend = match &self.config.runner.backend {
			RunnerBackend::Builtin(_) => tangram_sandbox::Backend::Builtin,
			RunnerBackend::Bubblewrap(config) => tangram_sandbox::Backend::Bubblewrap {
				path: config.path.clone(),
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
			RunnerBackend::Gvisor(config) => tangram_sandbox::Backend::Gvisor {
				path: config.path.clone(),
				platform: config.platform,
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
		self.config
			.runner
			.backend
			.resources(&self.config.runner.resources)
	}
}
