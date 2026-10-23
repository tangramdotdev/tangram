use {std::net::Ipv4Addr, tangram_client::prelude::*};

/// An inclusive range of IPv4 addresses.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Range {
	pub max: Ipv4Addr,

	pub min: Ipv4Addr,
}

impl std::fmt::Display for Range {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}-{}", self.min, self.max)
	}
}

impl std::str::FromStr for Range {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Some((min, max)) = s.split_once('-') {
			let min = min
				.trim()
				.parse()
				.map_err(|error| tg::error!(!error, "invalid minimum address"))?;
			let max = max
				.trim()
				.parse()
				.map_err(|error| tg::error!(!error, "invalid maximum address"))?;
			Ok(Range { max, min })
		} else if let Some((addr, prefix)) = s.split_once('/') {
			let addr: Ipv4Addr = addr
				.trim()
				.parse()
				.map_err(|error| tg::error!(!error, "invalid address"))?;
			let prefix: u8 = prefix
				.trim()
				.parse()
				.map_err(|error| tg::error!(!error, "invalid prefix"))?;
			if prefix > 32 {
				return Err(tg::error!(%prefix, "invalid prefix"));
			}
			let bits = u32::from(addr);
			let mask = if prefix == 0 {
				0
			} else {
				u32::MAX << (32 - prefix)
			};
			let min = Ipv4Addr::from(bits & mask);
			let max = Ipv4Addr::from((bits & mask) | !mask);
			Ok(Range { max, min })
		} else {
			Err(tg::error!(%s, "invalid IP range"))
		}
	}
}
