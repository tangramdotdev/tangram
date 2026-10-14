use tangram_client::prelude::*;

pub(crate) fn create(arg: &crate::Arg) -> tg::Result<Option<crate::network::Network>> {
	let Some(network) = arg.network.as_ref() else {
		return Ok(None);
	};
	let ports = network.ports();
	match network {
		crate::Network::Bridge(_) | crate::Network::Default => {
			if crate::network::root() {
				crate::network::veth::setup(arg.firewall, &arg.dns)?;
				let guest = reserve_veth_guest(&arg.ip_pool)?;
				let network = crate::network::veth::Network::new(
					arg.index,
					&arg.identity,
					arg.firewall,
					guest,
					ports,
				)?;
				let network = crate::network::Network::Veth(network);
				Ok(Some(network))
			} else {
				let options = crate::network::pasta::Options {
					dns: arg.dns.clone(),
					executable: None,
					interface: None,
					ports: ports.to_owned(),
					startup_timeout: arg.network_startup_timeout,
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
