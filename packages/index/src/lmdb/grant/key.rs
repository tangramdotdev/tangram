use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	ResourceGrant {
		resource: tg::Id,
		principal: tg::grant::Principal,
		permission: tg::grant::Permission,
	},
	PrincipalGrant {
		principal: tg::grant::Principal,
		resource: tg::Id,
		permission: tg::grant::Permission,
	},
}
