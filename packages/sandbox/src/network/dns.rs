use {std::net::Ipv4Addr, tangram_client::prelude::*};

pub(super) fn resolve(configured: &[Ipv4Addr]) -> tg::Result<Vec<Ipv4Addr>> {
	if !configured.is_empty() {
		return Ok(configured.to_vec());
	}

	// Discover upstream servers that are reachable outside the host network namespace.
	for path in ["/etc/resolv.conf", "/run/systemd/resolve/resolv.conf"] {
		let contents = match std::fs::read_to_string(path) {
			Ok(contents) => contents,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
			Err(error) => {
				return Err(tg::error!(!error, %path, "failed to read the host DNS configuration"));
			},
		};
		let servers = parse(&contents);
		if !servers.is_empty() {
			return Ok(servers);
		}
	}

	Err(tg::error!(
		"failed to discover an upstream IPv4 DNS server; configure sandbox.network.dns for bridge networking"
	))
}

fn parse(contents: &str) -> Vec<Ipv4Addr> {
	let mut servers = Vec::new();
	for line in contents.lines() {
		let mut words = line.split_whitespace();
		if words.next() != Some("nameserver") {
			continue;
		}
		let Some(server) = words.next().and_then(|word| word.parse::<Ipv4Addr>().ok()) else {
			continue;
		};
		if server.is_loopback() || server.is_unspecified() || servers.contains(&server) {
			continue;
		}
		servers.push(server);
	}

	servers
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn configured_servers_take_precedence() {
		let servers = [Ipv4Addr::new(192, 0, 2, 53)];
		assert_eq!(resolve(&servers).unwrap(), servers);
	}

	#[test]
	fn parses_upstream_servers_in_order() {
		let contents = "# nameserver 192.0.2.1\nsearch example.com\n nameserver 192.0.2.53 # primary\nnameserver 192.0.2.54\nnameserver 192.0.2.53\n";
		assert_eq!(
			parse(contents),
			[Ipv4Addr::new(192, 0, 2, 53), Ipv4Addr::new(192, 0, 2, 54)]
		);
	}

	#[test]
	fn ignores_unreachable_and_invalid_servers() {
		let contents = "nameserver 127.0.0.53\nnameserver 0.0.0.0\nnameserver ::1\nnameserver invalid\nnameserver\n";
		assert_eq!(parse(contents), [] as [Ipv4Addr; 0]);
	}
}
