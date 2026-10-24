use {
	std::net::{IpAddr, Ipv4Addr, SocketAddr},
	tangram_client::prelude::*,
};

pub mod control;

/// Fill in the resource bounds that a sandbox omits from the default bounds, raising a default maximum to meet a larger minimum, and leaving a resource with no minimum and no maximum omitted.
pub fn resolve_sandbox_resources(
	defaults: &tg::runner::Capacity,
	arg: &mut tg::sandbox::create::Arg,
) {
	if clear_darwin_sandbox_resources(arg) {
		return;
	}
	arg.cpu = resolve_sandbox_resource(arg.cpu, defaults.cpu);
	arg.memory = resolve_sandbox_resource(arg.memory, defaults.memory);
	arg.storage = resolve_sandbox_resource(arg.storage, defaults.storage);
}

/// Remove the resource bounds of a sandbox for a darwin host, which neither applies nor enforces them, and return whether the host is darwin.
pub fn clear_darwin_sandbox_resources(arg: &mut tg::sandbox::create::Arg) -> bool {
	let darwin = match &arg.host {
		Some(host) => host.ends_with("-darwin"),
		None => cfg!(target_os = "macos"),
	};
	if darwin {
		arg.cpu = None;
		arg.memory = None;
		arg.storage = None;
	}
	darwin
}

/// Give each resource of a sandbox that has no maximum the maximum of the capacity that it was allocated.
pub fn limit_sandbox_resources(arg: &mut tg::sandbox::create::Arg, capacity: tg::runner::Capacity) {
	arg.cpu = limit_sandbox_resource(arg.cpu, capacity.cpu);
	arg.memory = limit_sandbox_resource(arg.memory, capacity.memory);
	arg.storage = limit_sandbox_resource(arg.storage, capacity.storage);
}

/// Get the capacity that a sandbox with resolved resources reserves.
#[must_use]
pub fn sandbox_capacity(arg: &tg::sandbox::create::Arg) -> tg::runner::Capacity {
	tg::runner::Capacity {
		cpu: arg.cpu.unwrap_or_default(),
		memory: arg.memory.unwrap_or_default(),
		sandboxes: 1,
		storage: arg.storage.unwrap_or_default(),
	}
}

pub fn validate_sandbox_resources(
	isolation: &tangram_sandbox::Backend,
	cpu: Option<tg::sandbox::Cpu>,
	memory: Option<tg::sandbox::Resource<u64>>,
	hostname: Option<&str>,
	storage: Option<tg::sandbox::Resource<u64>>,
) -> tg::Result<()> {
	// Validate the resource bounds.
	if let Some(cpu) = cpu {
		cpu.threads()?;
	}
	for resource in [memory, storage].into_iter().flatten() {
		resource.validate()?;
	}

	// Validate the backend resource support.
	if !matches!(
		isolation,
		tangram_sandbox::Backend::Builtin
			| tangram_sandbox::Backend::Bubblewrap { .. }
			| tangram_sandbox::Backend::Gvisor { .. }
	) && storage.is_some()
	{
		return Err(tg::error!("storage bounds require container isolation"));
	}
	if matches!(isolation, tangram_sandbox::Backend::CloudHypervisor(_))
		&& (cpu.is_some_and(|cpu| {
			cpu.min.millis() > 0 || cpu.max.is_some_and(|max| max.millis() % 1000 != 0)
		}) || memory.is_some_and(|memory| memory.min > 0))
	{
		return Err(tg::error!(
			"resource minima and fractional CPUs require container isolation"
		));
	}

	// Validate the memory limit.
	if memory.is_some_and(|memory| memory.max == Some(0)) {
		return Err(tg::error!("sandbox memory must be greater than zero"));
	}

	// Validate the seatbelt options.
	if matches!(isolation, tangram_sandbox::Backend::Seatbelt)
		&& (cpu.is_some() || memory.is_some())
	{
		return Err(tg::error!(
			"sandbox cpu and memory are not supported with seatbelt isolation"
		));
	}
	if matches!(isolation, tangram_sandbox::Backend::Seatbelt) && hostname.is_some() {
		return Err(tg::error!(
			"setting a hostname is not supported with seatbelt isolation"
		));
	}

	Ok(())
}

pub fn normalize_sandbox_create_arg(
	mut arg: tg::sandbox::create::Arg,
) -> tg::Result<tg::sandbox::create::Arg> {
	if let Some(tg::sandbox::Network::Bridge(bridge)) = &mut arg.network {
		bridge.ports = resolve_sandbox_ports(std::mem::take(&mut bridge.ports))?;
	}
	Ok(arg)
}

fn limit_sandbox_resource<T>(
	requested: Option<tg::sandbox::Resource<T>>,
	capacity: tg::sandbox::Resource<T>,
) -> Option<tg::sandbox::Resource<T>>
where
	T: Copy + Default + PartialOrd,
{
	let Some(max) = capacity.max else {
		return requested;
	};
	let requested = requested.unwrap_or_default();
	if requested.max.is_some() {
		return Some(requested);
	}
	let max = if max < requested.min {
		requested.min
	} else {
		max
	};
	let resource = tg::sandbox::Resource {
		max: Some(max),
		min: requested.min,
	};

	Some(resource)
}

fn resolve_sandbox_resource<T>(
	requested: Option<tg::sandbox::Resource<T>>,
	default: tg::sandbox::Resource<T>,
) -> Option<tg::sandbox::Resource<T>>
where
	T: Copy + Default + PartialOrd,
{
	let min = requested.map_or(default.min, |requested| requested.min);
	let max = requested
		.and_then(|requested| requested.max)
		.or_else(|| default.max.map(|max| if max < min { min } else { max }));
	let resource = tg::sandbox::Resource { max, min };
	(resource != tg::sandbox::Resource::default()).then_some(resource)
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn defaults_fill_omitted_bounds_and_rise_to_meet_a_minimum() {
		let default = tg::sandbox::Resource::from(4);
		for (requested, expected) in [
			(None, Some(tg::sandbox::Resource::new(0, 4))),
			(
				Some(tg::sandbox::Resource::with_min(2)),
				Some(tg::sandbox::Resource::new(2, 4)),
			),
			(
				Some(tg::sandbox::Resource::with_min(8)),
				Some(tg::sandbox::Resource::new(8, 8)),
			),
			(
				Some(tg::sandbox::Resource::new(1, 2)),
				Some(tg::sandbox::Resource::new(1, 2)),
			),
		] {
			assert_eq!(resolve_sandbox_resource(requested, default), expected);
		}
	}

	#[test]
	fn darwin_hosts_neither_apply_nor_enforce_resource_bounds() {
		let defaults = tg::runner::Capacity {
			cpu: tg::sandbox::Amount::from_millis(1000).into(),
			memory: tg::sandbox::Resource::from(1024),
			..Default::default()
		};
		let mut arg = tg::sandbox::create::Arg {
			cpu: Some(tg::sandbox::Amount::from_millis(2000).into()),
			host: Some("aarch64-darwin".to_owned()),
			storage: Some(tg::sandbox::Resource::from(1024)),
			..Default::default()
		};
		resolve_sandbox_resources(&defaults, &mut arg);
		assert_eq!((arg.cpu, arg.memory, arg.storage), (None, None, None));
	}

	#[test]
	fn borrowers_without_a_maximum_take_the_maximum_of_their_allocation() {
		let capacity = tg::sandbox::Resource::new(0, 4);
		for (requested, expected) in [
			(None, Some(tg::sandbox::Resource::new(0, 4))),
			(
				Some(tg::sandbox::Resource::with_min(2)),
				Some(tg::sandbox::Resource::new(2, 4)),
			),
			(
				Some(tg::sandbox::Resource::new(1, 2)),
				Some(tg::sandbox::Resource::new(1, 2)),
			),
		] {
			assert_eq!(limit_sandbox_resource(requested, capacity), expected);
		}
		assert_eq!(
			limit_sandbox_resource(None, tg::sandbox::Resource::<u64>::default()),
			None
		);
	}

	#[test]
	fn unset_defaults_leave_unbounded_resources_omitted() {
		let default = tg::sandbox::Resource::<u64>::default();
		assert_eq!(resolve_sandbox_resource(None, default), None);
		assert_eq!(
			resolve_sandbox_resource(Some(tg::sandbox::Resource::default()), default),
			None
		);
		assert_eq!(
			resolve_sandbox_resource(Some(tg::sandbox::Resource::with_min(2)), default),
			Some(tg::sandbox::Resource::with_min(2))
		);
	}
}
