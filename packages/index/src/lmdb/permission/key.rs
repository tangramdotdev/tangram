use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	ResourcePermission {
		resource: tg::Id,
		subject: tg::authorization::Subject,
		creator: Option<tg::Principal>,
		permission: tg::authorization::Permission,
	},
	SubjectPermission {
		subject: tg::authorization::Subject,
		resource: tg::Id,
		creator: Option<tg::Principal>,
		permission: tg::authorization::Permission,
	},
	Visibility {
		resource: tg::Id,
		subject: tg::authorization::Subject,
		permission_resource: tg::Id,
		creator: Option<tg::Principal>,
		permission: tg::authorization::Permission,
	},
	PermissionExpiresAt {
		expires_at: i64,
		resource: tg::Id,
		subject: tg::authorization::Subject,
		creator: Option<tg::Principal>,
		permission: tg::authorization::Permission,
		source: super::PermissionSource,
	},
}
