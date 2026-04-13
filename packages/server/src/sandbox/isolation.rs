use {crate::Server, tangram_client::prelude::*};

impl Server {
	pub(crate) fn resolve_sandbox_isolation(&self) -> tg::Result<tangram_sandbox::Isolation> {
		let isolation = Self::sandbox_isolation_from_config(&self.config.sandbox.isolation)
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
			return Some(tangram_sandbox::Isolation::Vm(
				tangram_sandbox::VmIsolation {
					kernel_path: vm.kernel_path.clone(),
				},
			));
		}
		None
	}
}
