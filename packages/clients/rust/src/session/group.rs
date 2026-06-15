use crate::prelude::*;

impl tg::handle::Group for tg::Session {
	fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> impl Future<Output = tg::Result<tg::group::create::Output>> {
		self.create_group(arg)
	}

	fn try_get_group(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::Group>>> {
		self.try_get_group(group, arg)
	}

	fn try_delete_group(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_group(group, arg)
	}

	fn list_group_members(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::members::list::Arg,
	) -> impl Future<Output = tg::Result<tg::group::members::list::Output>> {
		self.list_group_members(group, arg)
	}

	fn add_group_member(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::members::add::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_group_member(group, arg)
	}

	fn remove_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
		arg: tg::group::members::remove::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.remove_group_member(group, member, arg)
	}
}
