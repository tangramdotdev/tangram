use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	ResourcePermission {
		resource: tg::Id,
		subject: tg::authorization::Subject,
	},
	SubjectPermission {
		subject: tg::authorization::Subject,
		resource: tg::Id,
	},
	Visibility {
		resource: tg::Id,
		subject: tg::authorization::Subject,
		permission_resource: tg::Id,
	},
	PermissionExpiresAt {
		expires_at: i64,
		resource: tg::Id,
		subject: tg::authorization::Subject,
		permission: tg::authorization::Permission,
		source: super::PermissionSource,
	},
}
