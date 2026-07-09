use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	ResourceGrant {
		resource: tg::Id,
		principal: tg::grant::Principal,
		creator: Option<tg::Principal>,
		permission: tg::grant::Permission,
	},
	PrincipalGrant {
		principal: tg::grant::Principal,
		resource: tg::Id,
		creator: Option<tg::Principal>,
		permission: tg::grant::Permission,
	},
	Visibility {
		resource: tg::Id,
		principal: tg::grant::Principal,
		grant_resource: tg::Id,
		creator: Option<tg::Principal>,
		permission: tg::grant::Permission,
	},
	GrantExpiresAt {
		expires_at: i64,
		resource: tg::Id,
		principal: tg::grant::Principal,
		creator: Option<tg::Principal>,
		permission: tg::grant::Permission,
		source: super::GrantSource,
	},
}
