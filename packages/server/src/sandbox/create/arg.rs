use {
	crate::Session,
	std::net::{IpAddr, Ipv4Addr, SocketAddr},
	tangram_client::prelude::*,
};

impl Session {
	pub(crate) fn normalize_sandbox_create_arg(
		mut arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Arg> {
		if let Some(tg::sandbox::Network::Bridge(bridge)) = &mut arg.network {
			bridge.ports = resolve_sandbox_ports(std::mem::take(&mut bridge.ports))?;
		}
		Ok(arg)
	}

	pub(crate) async fn validate_sandbox_create_arg_with_parent(
		&self,
		arg: &tg::sandbox::create::Arg,
		parent: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let parent = if let Some(parent) = self.server.runner.state().try_get_sandbox(parent) {
			parent
		} else {
			self.try_get_sandbox_from_index(parent)
				.await
				.map_err(|error| tg::error!(!error, %parent, "failed to get the parent sandbox"))?
				.and_then(|sandbox| sandbox.data)
				.ok_or_else(|| tg::error!(%parent, "failed to find the parent sandbox"))?
		};

		if !sandbox_network_contains(parent.data.network.as_ref(), arg.network.as_ref()) {
			let parent = &parent.data.id;
			return Err(tg::error!(
				%parent,
				"the child sandbox network is more permissive than the parent sandbox network"
			));
		}

		validate_sandbox_mounts(&parent.data.mounts, &arg.mounts).await?;

		Ok(())
	}
}

fn sandbox_network_contains(
	parent: Option<&tg::sandbox::Network>,
	child: Option<&tg::sandbox::Network>,
) -> bool {
	match child {
		None => true,
		Some(tg::sandbox::Network::Bridge(child)) => match parent {
			Some(tg::sandbox::Network::Bridge(_) | tg::sandbox::Network::Default) => {
				child.ports.is_empty()
			},
			Some(tg::sandbox::Network::Host) => true,
			None => false,
		},
		Some(tg::sandbox::Network::Default) => parent.is_some(),
		Some(tg::sandbox::Network::Host) => {
			matches!(parent, Some(tg::sandbox::Network::Host))
		},
	}
}

async fn validate_sandbox_mounts(
	parent: &[tg::sandbox::Mount],
	child: &[tg::sandbox::Mount],
) -> tg::Result<()> {
	if child.is_empty() {
		return Ok(());
	}

	let mut parent_sources = Vec::with_capacity(parent.len());
	for mount in parent {
		let source = tokio::fs::canonicalize(&mount.source)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					path = %mount.source.display(),
					"failed to resolve a parent sandbox mount source"
				)
			})?;
		parent_sources.push((source, mount.readonly));
	}

	for mount in child {
		let source = tokio::fs::canonicalize(&mount.source)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					path = %mount.source.display(),
					"failed to resolve a child sandbox mount source"
				)
			})?;
		let allowed = parent_sources
			.iter()
			.any(|(parent, readonly)| source.starts_with(parent) && (!readonly || mount.readonly));
		if !allowed {
			return Err(tg::error!(
				source = %mount.source.display(),
				target = %mount.target.display(),
				"the child sandbox mount is more permissive than the parent sandbox mounts"
			));
		}
	}

	Ok(())
}

fn resolve_sandbox_ports(ports: Vec<tg::sandbox::Port>) -> tg::Result<Vec<tg::sandbox::Port>> {
	let mut output = Vec::new();
	for port in ports {
		for mut port in port.expand() {
			if port.host.is_none() {
				let host = allocate_sandbox_port(&output, port.host_ip, port.protocol)?;
				port = port.with_host(tg::sandbox::PortRange::single(host));
			}
			validate_sandbox_port(&output, port)?;
			output.push(port);
		}
	}
	Ok(output)
}

fn allocate_sandbox_port(
	existing: &[tg::sandbox::Port],
	host_ip: Option<Ipv4Addr>,
	protocol: tg::sandbox::PortProtocol,
) -> tg::Result<u16> {
	for _ in 0..100 {
		let port = bind_ephemeral_port(host_ip, protocol)?;
		if !existing
			.iter()
			.any(|existing| host_port_conflicts(*existing, host_ip, port, protocol))
		{
			return Ok(port);
		}
	}
	Err(tg::error!("failed to allocate a host port"))
}

fn validate_sandbox_port(
	existing: &[tg::sandbox::Port],
	port: tg::sandbox::Port,
) -> tg::Result<()> {
	let host = port
		.host
		.ok_or_else(|| tg::error!("expected a resolved host port"))?;
	if !host.is_single() || !port.guest.is_single() {
		return Err(tg::error!("expected resolved port mappings"));
	}
	let host_port = host.start;
	if existing
		.iter()
		.any(|existing| host_port_conflicts(*existing, port.host_ip, host_port, port.protocol))
	{
		return Err(tg::error!(%host_port, "duplicate host port mapping"));
	}
	bind_specific_port(port.host_ip, host_port, port.protocol)?;
	Ok(())
}

fn host_port_conflicts(
	existing: tg::sandbox::Port,
	host_ip: Option<Ipv4Addr>,
	host_port: u16,
	protocol: tg::sandbox::PortProtocol,
) -> bool {
	existing.protocol == protocol
		&& existing
			.host
			.is_some_and(|host| host.start <= host_port && host.end >= host_port)
		&& host_ips_conflict(existing.host_ip, host_ip)
}

fn host_ips_conflict(a: Option<Ipv4Addr>, b: Option<Ipv4Addr>) -> bool {
	a.is_none() || b.is_none() || a == b
}

fn bind_ephemeral_port(
	host_ip: Option<Ipv4Addr>,
	protocol: tg::sandbox::PortProtocol,
) -> tg::Result<u16> {
	let addr = socket_addr(host_ip, 0);
	match protocol {
		tg::sandbox::PortProtocol::Tcp => {
			let listener = std::net::TcpListener::bind(addr)
				.map_err(|error| tg::error!(!error, "failed to bind a host port"))?;
			listener
				.local_addr()
				.map(|addr| addr.port())
				.map_err(|error| tg::error!(!error, "failed to get the host port"))
		},
		tg::sandbox::PortProtocol::Udp => {
			let socket = std::net::UdpSocket::bind(addr)
				.map_err(|error| tg::error!(!error, "failed to bind a host port"))?;
			socket
				.local_addr()
				.map(|addr| addr.port())
				.map_err(|error| tg::error!(!error, "failed to get the host port"))
		},
	}
}

fn bind_specific_port(
	host_ip: Option<Ipv4Addr>,
	port: u16,
	protocol: tg::sandbox::PortProtocol,
) -> tg::Result<()> {
	let addr = socket_addr(host_ip, port);
	match protocol {
		tg::sandbox::PortProtocol::Tcp => std::net::TcpListener::bind(addr)
			.map(drop)
			.map_err(|error| tg::error!(!error, %port, "failed to bind the host port")),
		tg::sandbox::PortProtocol::Udp => std::net::UdpSocket::bind(addr)
			.map(drop)
			.map_err(|error| tg::error!(!error, %port, "failed to bind the host port")),
	}
}

fn socket_addr(host_ip: Option<Ipv4Addr>, port: u16) -> SocketAddr {
	let host_ip = host_ip.unwrap_or(Ipv4Addr::UNSPECIFIED);
	SocketAddr::new(IpAddr::V4(host_ip), port)
}
