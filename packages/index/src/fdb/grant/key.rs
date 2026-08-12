use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	ResourceGrant {
		resource: tg::Id,
		subject: tg::authorization::Subject,
		creator: Option<tg::Principal>,
		permission: tg::authorization::Permission,
	},
	SubjectGrant {
		subject: tg::authorization::Subject,
		resource: tg::Id,
		creator: Option<tg::Principal>,
		permission: tg::authorization::Permission,
	},
	Visibility {
		resource: tg::Id,
		subject: tg::authorization::Subject,
		grant_resource: tg::Id,
		creator: Option<tg::Principal>,
		permission: tg::authorization::Permission,
	},
	GrantExpiresAt {
		partition: u64,
		expires_at: i64,
		resource: tg::Id,
		subject: tg::authorization::Subject,
		creator: Option<tg::Principal>,
		permission: tg::authorization::Permission,
		source: super::GrantSource,
	},
}
