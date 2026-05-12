use {std::net::Ipv4Addr, tangram_client::prelude::*};

pub(crate) fn create(
	dns: &[Ipv4Addr],
	network: Option<&crate::Network>,
	pool: &crate::network::ip::Pool,
	ports: &[tg::sandbox::Port],
) -> tg::Result<Option<crate::network::Network>> {
	let Some(network) = network else {
		return Ok(None);
	};
	match network {
		crate::Network::Bridge(_) | crate::Network::Default => {
			if crate::network::root() {
				crate::network::veth::setup()?;
				let guest = reserve_veth_guest(pool)?;
				let network = crate::network::veth::Network::new(guest, ports)?;
				let network = crate::network::Network::Veth(network);
				Ok(Some(network))
			} else {
				let options = crate::network::pasta::Options {
					dns: dns.to_owned(),
					ports: ports.to_owned(),
					..Default::default()
				};
				let network = crate::network::pasta::Network::new(options)?;
				let network = crate::network::Network::Pasta(network);
				Ok(Some(network))
			}
		},
		crate::Network::Host => Ok(Some(crate::network::Network::Host)),
	}
}

fn reserve_veth_guest(pool: &crate::network::ip::Pool) -> tg::Result<crate::network::ip::Lease> {
	pool.try_reserve_in(
		crate::network::veth::guest_ip_min(),
		crate::network::veth::guest_ip_max(),
	)
	.map_err(|source| {
		tg::error!(
			!source,
			"failed to reserve a veth address in the bridge subnet"
		)
	})
}
