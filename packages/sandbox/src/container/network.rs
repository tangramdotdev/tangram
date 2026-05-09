use {std::net::Ipv4Addr, tangram_client::prelude::*};

pub(crate) fn create(
	dns: &[Ipv4Addr],
	network: Option<&crate::Network>,
	pool: &crate::network::ip::Pool,
) -> tg::Result<Option<crate::network::Network>> {
	let Some(network) = network else {
		return Ok(None);
	};
	match network {
		crate::Network::Bridge | crate::Network::Default => {
			if crate::network::root() {
				crate::network::veth::setup()?;
				let guest = pool.try_reserve()?;
				let network = crate::network::veth::Network::new(guest);
				let network = crate::network::Network::Veth(network);
				Ok(Some(network))
			} else {
				let options = crate::network::pasta::Options {
					dns: dns.to_owned(),
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
