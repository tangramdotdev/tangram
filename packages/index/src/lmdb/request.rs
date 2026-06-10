use tangram_client::prelude::*;

#[derive(Clone)]
pub(super) enum Request {
	Clean(Clean),
	DeleteGrants(Vec<crate::grant::delete::Arg>),
	DeleteGroupMembers(Vec<crate::group::member::delete::Arg>),
	DeleteGroups(Vec<tg::group::Id>),
	DeleteOrganizationMembers(Vec<crate::organization::member::delete::Arg>),
	DeleteOrganizations(Vec<tg::organization::Id>),
	DeleteTags(Vec<tg::tag::Id>),
	DeleteUsers(Vec<tg::user::Id>),
	PutCacheEntries(Vec<crate::cache::put::Arg>),
	PutGrants(Vec<crate::grant::put::Arg>),
	PutGroupMembers(Vec<crate::group::member::put::Arg>),
	PutGroups(Vec<crate::group::put::Arg>),
	PutObjects(Vec<crate::object::put::Arg>),
	PutOrganizationMembers(Vec<crate::organization::member::put::Arg>),
	PutOrganizations(Vec<crate::organization::put::Arg>),
	PutProcesses(Vec<crate::process::put::Arg>),
	PutTags(Vec<crate::tag::put::Arg>),
	PutUsers(Vec<crate::user::put::Arg>),
	TouchCacheEntries(TouchCacheEntries),
	TouchObjects(TouchObjects),
	TouchProcesses(TouchProcesses),
	Update(Update),
}

#[derive(Clone)]
pub(super) struct Clean {
	pub batch_size: usize,
	pub max_object_touched_at: i64,
	pub max_process_touched_at: i64,
}

#[derive(Clone)]
pub(super) struct TouchCacheEntries {
	pub ids: Vec<tg::artifact::Id>,
	pub time_to_touch: std::time::Duration,
	pub touched_at: i64,
}

#[derive(Clone)]
pub(super) struct TouchObjects {
	pub ids: Vec<tg::object::Id>,
	pub time_to_touch: std::time::Duration,
	pub touched_at: i64,
}

#[derive(Clone)]
pub(super) struct TouchProcesses {
	pub ids: Vec<tg::process::Id>,
	pub time_to_touch: std::time::Duration,
	pub touched_at: i64,
}

#[derive(Clone)]
pub(super) struct Update {
	pub batch_size: usize,
}

pub(super) enum Item {
	Clean,
	DeleteGrant(crate::grant::delete::Arg),
	DeleteGroup(tg::group::Id),
	DeleteGroupMember(crate::group::member::delete::Arg),
	DeleteOrganization(tg::organization::Id),
	DeleteOrganizationMember(crate::organization::member::delete::Arg),
	DeleteTag(tg::tag::Id),
	DeleteUser(tg::user::Id),
	PutCacheEntry(crate::cache::put::Arg),
	PutGrant(crate::grant::put::Arg),
	PutGroup(crate::group::put::Arg),
	PutGroupMember(crate::group::member::put::Arg),
	PutObject(crate::object::put::Arg),
	PutOrganization(crate::organization::put::Arg),
	PutOrganizationMember(crate::organization::member::put::Arg),
	PutProcess(crate::process::put::Arg),
	PutTag(crate::tag::put::Arg),
	PutUser(crate::user::put::Arg),
	TouchCacheEntry(tg::artifact::Id),
	TouchObject(tg::object::Id),
	TouchProcess(tg::process::Id),
	Update,
}

pub(super) enum Kind {
	Clean {
		max_object_touched_at: i64,
		max_process_touched_at: i64,
	},
	DeleteGrants,
	DeleteGroupMembers,
	DeleteGroups,
	DeleteOrganizationMembers,
	DeleteOrganizations,
	DeleteTags,
	DeleteUsers,
	PutCacheEntries,
	PutGrants,
	PutGroupMembers,
	PutGroups,
	PutObjects,
	PutOrganizationMembers,
	PutOrganizations,
	PutProcesses,
	PutTags,
	PutUsers,
	TouchCacheEntries {
		time_to_touch: std::time::Duration,
		touched_at: i64,
	},
	TouchObjects {
		time_to_touch: std::time::Duration,
		touched_at: i64,
	},
	TouchProcesses {
		time_to_touch: std::time::Duration,
		touched_at: i64,
	},
	Update,
}
