use {crate::Server, tangram_client::prelude::*};

impl Server {
	pub(crate) fn resolve_sandbox_isolation(&self) -> tg::Result<tangram_sandbox::Isolation> {
		if let Some(default) = self.config.sandbox.isolation.default {
			return match default {
				crate::config::SandboxIsolationDefault::Container => {
					Ok(tangram_sandbox::Isolation::Container(
						tangram_sandbox::ContainerIsolation::default(),
					))
				},
				crate::config::SandboxIsolationDefault::Seatbelt => {
					Ok(tangram_sandbox::Isolation::Seatbelt(
						tangram_sandbox::SeatbeltIsolation::default(),
					))
				},
				crate::config::SandboxIsolationDefault::Vm => {
					let vm = self
						.config
						.sandbox
						.isolation
						.vm
						.as_ref()
						.ok_or_else(|| tg::error!("vm isolation is not configured"))?;
					let image_path = self
						.sandbox_vm_image
						.clone()
						.ok_or_else(|| tg::error!("vm image is not available"))?;
					let snapshot = Some(
						vm.snapshot
							.clone()
							.unwrap_or_else(|| self.vm_snapshot_path()),
					);
					Ok(tangram_sandbox::Isolation::Vm(
						tangram_sandbox::VmIsolation {
							dax: vm.dax.map(|dax| dax.window_size_kib as u64 * 1024),
							kernel_path: vm.kernel_path.clone(),
							max_cpu: vm.max_cpu,
							max_memory: vm.max_memory,
							image_path,
							snapshot,
							snapshot_cpu: vm.snapshot_cpu,
							snapshot_memory: vm.snapshot_memory,
						},
					))
				},
			};
		}
		let isolation = self
			.sandbox_isolation_from_config(&self.config.sandbox.isolation)
			.ok_or_else(|| tg::error!("at least one isolation level must be configured"))?;
		#[cfg(target_os = "linux")]
		{
			match isolation {
				tangram_sandbox::Isolation::Container(_) | tangram_sandbox::Isolation::Vm(_) => {
					Ok(isolation)
				},
				tangram_sandbox::Isolation::Seatbelt(_) => {
					Err(tg::error!("seatbelt isolation is not supported on linux"))
				},
			}
		}
		#[cfg(target_os = "macos")]
		{
			match isolation {
				tangram_sandbox::Isolation::Container(_) => {
					Err(tg::error!("container isolation is not supported on macos"))
				},
				tangram_sandbox::Isolation::Seatbelt(_) => Ok(isolation),
				tangram_sandbox::Isolation::Vm(_) => {
					Err(tg::error!("vm isolation is not supported on macos"))
				},
			}
		}
	}

	fn sandbox_isolation_from_config(
		&self,
		isolation: &crate::config::SandboxIsolation,
	) -> Option<tangram_sandbox::Isolation> {
		if isolation.container.is_some() {
			return Some(tangram_sandbox::Isolation::Container(
				tangram_sandbox::ContainerIsolation::default(),
			));
		}
		if isolation.seatbelt.is_some() {
			return Some(tangram_sandbox::Isolation::Seatbelt(
				tangram_sandbox::SeatbeltIsolation::default(),
			));
		}
		if let Some(vm) = &isolation.vm {
			let image_path = self.sandbox_vm_image.clone()?;
			let snapshot = Some(
				vm.snapshot
					.clone()
					.unwrap_or_else(|| self.vm_snapshot_path()),
			);
			return Some(tangram_sandbox::Isolation::Vm(
				tangram_sandbox::VmIsolation {
					dax: vm.dax.map(|dax| dax.window_size_kib as u64 * 1024),
					kernel_path: vm.kernel_path.clone(),
					max_cpu: vm.max_cpu,
					max_memory: vm.max_memory,
					image_path,
					snapshot,
					snapshot_cpu: vm.snapshot_cpu,
					snapshot_memory: vm.snapshot_memory,
				},
			));
		}
		None
	}
}
