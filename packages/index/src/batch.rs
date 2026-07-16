use tangram_client::prelude::*;

#[derive(Clone, Debug, Default)]
pub struct Arg {
	pub delete_grants: Vec<crate::grant::delete::Arg>,
	pub delete_group_members: Vec<crate::group::member::delete::Arg>,
	pub delete_groups: Vec<tg::group::Id>,
	pub delete_organization_members: Vec<crate::organization::member::delete::Arg>,
	pub delete_organizations: Vec<tg::organization::Id>,
	pub delete_sandboxes: Vec<tg::sandbox::Id>,
	pub delete_tags: Vec<tg::tag::Id>,
	pub delete_users: Vec<tg::user::Id>,
	pub put_cache_entries: Vec<crate::cache::put::Arg>,
	pub put_grants: Vec<crate::grant::put::Arg>,
	pub put_group_members: Vec<crate::group::member::put::Arg>,
	pub put_groups: Vec<crate::group::put::Arg>,
	pub put_objects: Vec<crate::object::put::Arg>,
	pub put_organization_members: Vec<crate::organization::member::put::Arg>,
	pub put_organizations: Vec<crate::organization::put::Arg>,
	pub put_processes: Vec<crate::process::put::Arg>,
	pub put_runners: Vec<crate::runner::put::Arg>,
	pub put_sandboxes: Vec<crate::sandbox::put::Arg>,
	pub put_tags: Vec<crate::tag::put::Arg>,
	pub put_users: Vec<crate::user::put::Arg>,
}

impl Arg {
	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.delete_grants.is_empty()
			&& self.delete_group_members.is_empty()
			&& self.delete_groups.is_empty()
			&& self.delete_organization_members.is_empty()
			&& self.delete_organizations.is_empty()
			&& self.delete_sandboxes.is_empty()
			&& self.delete_tags.is_empty()
			&& self.delete_users.is_empty()
			&& self.put_cache_entries.is_empty()
			&& self.put_grants.is_empty()
			&& self.put_group_members.is_empty()
			&& self.put_groups.is_empty()
			&& self.put_objects.is_empty()
			&& self.put_organization_members.is_empty()
			&& self.put_organizations.is_empty()
			&& self.put_processes.is_empty()
			&& self.put_runners.is_empty()
			&& self.put_sandboxes.is_empty()
			&& self.put_tags.is_empty()
			&& self.put_users.is_empty()
	}
}
