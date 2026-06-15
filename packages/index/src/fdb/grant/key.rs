use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	ResourceGrant {
		resource: tg::Id,
		principal: tg::grant::Principal,
		permission: tg::grant::Permission,
		expires_at: Option<i64>,
	},
	PrincipalGrant {
		principal: tg::grant::Principal,
		resource: tg::Id,
		permission: tg::grant::Permission,
		expires_at: Option<i64>,
	},
	Visibility {
		resource: tg::Id,
		principal: tg::grant::Principal,
		grant_resource: tg::Id,
		permission: tg::grant::Permission,
		expires_at: Option<i64>,
	},
	GrantExpiresAt {
		partition: u64,
		expires_at: i64,
		resource: tg::Id,
		principal: tg::grant::Principal,
		permission: tg::grant::Permission,
	},
}
