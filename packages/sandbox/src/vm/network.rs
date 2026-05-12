use tangram_client::prelude::*;

pub(crate) fn create(
	network: Option<&crate::Network>,
	pool: &crate::network::ip::Pool,
	ports: &[tg::sandbox::Port],
) -> tg::Result<Option<crate::network::Network>> {
	let Some(network) = network else {
		return Ok(None);
	};
	match network {
		crate::Network::Bridge | crate::Network::Default => {
			let (host, guest) = pool.try_reserve_pair()?;
			let network = if crate::network::root() {
				crate::network::tap::setup()?;
				let network = crate::network::tap::Network::new(host, guest, ports)?;
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
