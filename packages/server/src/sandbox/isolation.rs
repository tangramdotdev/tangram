use {crate::Server, tangram_client::prelude::*};

impl Server {
	fn sandbox_isolation_from_config(
		isolation: crate::config::SandboxIsolation,
	) -> tangram_sandbox::Isolation {
		match isolation {
			crate::config::SandboxIsolation::Container(_) => {
				tangram_sandbox::Isolation::Container(tangram_sandbox::ContainerIsolation::default())
			},
			crate::config::SandboxIsolation::Seatbelt(_) => {
				tangram_sandbox::Isolation::Seatbelt(tangram_sandbox::SeatbeltIsolation::default())
			},
			crate::config::SandboxIsolation::Vm(_) => {
				tangram_sandbox::Isolation::Vm(tangram_sandbox::VmIsolation::default())
			},
		}
	}

	pub(crate) fn resolve_sandbox_isolation(&self) -> tg::Result<tangram_sandbox::Isolation> {
		let isolation = Self::sandbox_isolation_from_config(self.config.sandbox.isolation);
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
}
