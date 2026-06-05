use crate::prelude::*;

impl tg::handle::Organization for tg::Session {
	fn create_organization(
		&self,
		arg: tg::organization::create::Arg,
	) -> impl Future<Output = tg::Result<tg::organization::create::Output>> {
		self.create_organization(arg)
	}

	fn try_get_organization(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::Organization>>> {
		self.try_get_organization(organization, arg)
	}

	fn try_delete_organization(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_organization(organization, arg)
	}

	fn try_get_organization_grants(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::organization::grants::Output>>> {
		self.try_get_organization_grants(organization, arg)
	}

	fn list_organization_members(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::members::list::Arg,
	) -> impl Future<Output = tg::Result<tg::organization::members::list::Output>> {
		self.list_organization_members(organization, arg)
	}

	fn add_organization_member(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::members::add::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_organization_member(organization, arg)
	}

	fn remove_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
		arg: tg::organization::members::remove::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.remove_organization_member(organization, member, arg)
	}
}
