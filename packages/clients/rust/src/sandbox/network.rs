use {crate::prelude::*, std::net::Ipv4Addr};

#[derive(
	Clone,
	Debug,
	Default,
	derive_more::IsVariant,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Network {
	#[tangram_serialize(id = 0)]
	Bridge(Bridge),

	#[default]
	#[tangram_serialize(id = 1)]
	Default,

	#[tangram_serialize(id = 2)]
	Host,
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Bridge {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub ports: Vec<Port>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	PartialEq,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Port {
	pub host_ip: Option<Ipv4Addr>,
	pub host: Option<Range>,
	pub guest: Range,
	pub protocol: Protocol,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Range {
	pub start: u16,
	pub end: u16,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum Protocol {
	#[default]
	Tcp,
	Udp,
}

impl Network {
	#[must_use]
	pub fn ports(&self) -> &[Port] {
		match self {
			Self::Bridge(bridge) => &bridge.ports,
			Self::Default | Self::Host => &[],
		}
	}
}

impl Port {
	#[must_use]
	pub fn expand(self) -> Vec<Self> {
		(0..self.guest.len())
			.map(|index| {
				let host = self.host.map(|range| Range::single(range.port_at(index)));
				let guest = Range::single(self.guest.port_at(index));
				Self {
					host_ip: self.host_ip,
					host,
					guest,
					protocol: self.protocol,
				}
			})
			.collect()
	}

	#[must_use]
	pub fn with_host(self, host: Range) -> Self {
		Self {
			host: Some(host),
			..self
		}
	}
}

impl Range {
	#[must_use]
	pub fn single(port: u16) -> Self {
		Self {
			start: port,
			end: port,
		}
	}

	#[must_use]
	pub fn is_single(self) -> bool {
		self.start == self.end
	}

	fn len(self) -> usize {
		usize::from(self.end - self.start) + 1
	}

	fn port_at(self, index: usize) -> u16 {
		let index = u16::try_from(index).unwrap();
		self.start + index
	}
}

impl std::fmt::Display for Port {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(host_ip) = self.host_ip {
			write!(f, "{host_ip}:")?;
			if let Some(host) = self.host {
				write!(f, "{host}")?;
			}
			write!(f, ":")?;
		} else if let Some(host) = self.host {
			write!(f, "{host}:")?;
		}
		write!(f, "{}", self.guest)?;
		if self.protocol != Protocol::Tcp {
			write!(f, "/{}", self.protocol)?;
		}
		Ok(())
	}
}

impl std::fmt::Display for Range {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if self.is_single() {
			write!(f, "{}", self.start)
		} else {
			write!(f, "{}-{}", self.start, self.end)
		}
	}
}

impl std::fmt::Display for Protocol {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Tcp => write!(f, "tcp"),
			Self::Udp => write!(f, "udp"),
		}
	}
}

impl std::str::FromStr for Port {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let (s, protocol) = if let Some((s, protocol)) = s.rsplit_once('/') {
			(s, protocol.parse()?)
		} else {
			(s, Protocol::Tcp)
		};
		if s.is_empty() {
			return Err(tg::error!("expected a port mapping"));
		}
		let parts = s.split(':').collect::<Vec<_>>();
		let (host_ip, host, guest) = match parts.as_slice() {
			[guest] => (None, None, guest.parse::<Range>()?),
			[host, guest] => {
				if host.is_empty() {
					return Err(tg::error!("expected a host port"));
				}
				if host.parse::<Ipv4Addr>().is_ok() {
					return Err(tg::error!("expected a host port after the host ip"));
				}
				(None, Some(host.parse::<Range>()?), guest.parse::<Range>()?)
			},
			[host_ip, host, guest] => {
				if host_ip.is_empty() {
					return Err(tg::error!("expected a host ip"));
				}
				let host_ip = host_ip
					.parse::<Ipv4Addr>()
					.map_err(|error| tg::error!(!error, "invalid host ip"))?;
				let host_ip = (!host_ip.is_unspecified()).then_some(host_ip);
				let host = if host.is_empty() {
					None
				} else {
					Some(host.parse::<Range>()?)
				};
				(host_ip, host, guest.parse::<Range>()?)
			},
			_ => return Err(tg::error!("invalid port mapping")),
		};
		if let Some(host) = host
			&& host.len() != guest.len()
		{
			return Err(tg::error!(
				"host and guest port ranges must have the same length"
			));
		}
		Ok(Self {
			host_ip,
			host,
			guest,
			protocol,
		})
	}
}

impl std::str::FromStr for Range {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.is_empty() {
			return Err(tg::error!("expected a port"));
		}
		let (start, end) = if let Some((start, end)) = s.split_once('-') {
			(parse_port(start)?, parse_port(end)?)
		} else {
			let port = parse_port(s)?;
			(port, port)
		};
		if end < start {
			return Err(tg::error!("port ranges must be ascending"));
		}
		Ok(Self { start, end })
	}
}

impl std::str::FromStr for Protocol {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"tcp" => Ok(Self::Tcp),
			"udp" => Ok(Self::Udp),
			_ => Err(tg::error!(%s, "unsupported port protocol")),
		}
	}
}

fn parse_port(s: &str) -> tg::Result<u16> {
	let port = s
		.parse::<u16>()
		.map_err(|error| tg::error!(!error, "invalid port"))?;
	if port == 0 {
		return Err(tg::error!("ports must be greater than zero"));
	}
	Ok(port)
}
