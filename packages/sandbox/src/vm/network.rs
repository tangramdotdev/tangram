use {std::path::Path, tangram_client::prelude::*};

pub(crate) fn create(
	id: u64,
	identity: &Path,
	firewall: crate::Firewall,
	network: Option<&crate::Network>,
	pool: &crate::network::ip::Pool,
	ports: &[tg::sandbox::Port],
) -> tg::Result<Option<crate::network::Network>> {
	let Some(network) = network else {
		return Ok(None);
	};
	match network {
		crate::Network::Bridge(_) | crate::Network::Default => {
			let (host, guest) = pool.try_reserve_pair()?;
			let network = if crate::network::root() {
				crate::network::tap::setup(firewall)?;
				let network =
					crate::network::tap::Network::new(id, identity, firewall, host, guest, ports)?;
				crate::network::Network::Tap(network)
			} else {
				let network = crate::network::passt::Network::new(host, guest);
				crate::network::Network::Passt(network)
			};
			Ok(Some(network))
		},
		crate::Network::Host => Err(tg::error!("vm sandboxes do not support host networking")),
	}
}
