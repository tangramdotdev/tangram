use crate::prelude::*;

impl tg::handle::Group for tg::Session {
	fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> impl Future<Output = tg::Result<tg::group::create::Output>> {
		self.create_group(arg)
	}

	fn list_groups(
		&self,
		arg: tg::group::list::Arg,
	) -> impl Future<Output = tg::Result<tg::group::list::Output>> {
		self.list_groups(arg)
	}

	fn try_get_group(
		&self,
		group: &tg::group::Selector,
	) -> impl Future<Output = tg::Result<Option<tg::Group>>> {
		self.try_get_group(group)
	}

	fn try_delete_group(
		&self,
		group: &tg::group::Selector,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_group(group)
	}

	fn try_get_group_grants(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::group::grants::Output>>> {
		self.try_get_group_grants(group, arg)
	}

	fn list_group_members(
		&self,
		group: &tg::group::Selector,
	) -> impl Future<Output = tg::Result<tg::group::members::list::Output>> {
		self.list_group_members(group)
	}

	fn add_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_group_member(group, member)
	}

	fn remove_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.remove_group_member(group, member)
	}
}
