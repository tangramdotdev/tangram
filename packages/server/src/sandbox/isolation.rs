use {crate::Server, tangram_client::prelude::*};

impl Server {
	pub(crate) fn resolve_sandbox_isolation(&self) -> tg::Result<tangram_sandbox::Isolation> {
		let isolation = Self::sandbox_isolation_from_config(&self.config.sandbox.isolation);
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
	) -> tangram_sandbox::Isolation {
		match isolation {
			crate::config::SandboxIsolation::Container(isolation) => {
				let network = isolation.network.as_ref().map(|network| match network {
					crate::config::ContainerNetwork::Host => tangram_sandbox::Network::Host,
					crate::config::ContainerNetwork::Bridge(bridge) => {
						tangram_sandbox::Network::Bridge(tangram_sandbox::Bridge {
							ip: bridge.ip.unwrap_or_else(crate::config::default_bridge_ip),
							name: bridge.name.clone(),
						})
					},
				});
				tangram_sandbox::Isolation::Container(tangram_sandbox::ContainerIsolation {
					network,
				})
			},
			crate::config::SandboxIsolation::Seatbelt(_) => {
				tangram_sandbox::Isolation::Seatbelt(tangram_sandbox::SeatbeltIsolation::default())
			},
			crate::config::SandboxIsolation::Vm(isolation) => {
				tangram_sandbox::Isolation::Vm(tangram_sandbox::VmIsolation {
					kernel_path: isolation.kernel_path.clone(),
				})
			},
		}
	}
}
