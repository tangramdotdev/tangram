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
	) -> impl Future<Output = tg::Result<Option<tg::Organization>>> {
		self.try_get_organization(organization)
	}

	fn try_delete_organization(
		&self,
		organization: &tg::organization::Selector,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_organization(organization)
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
	) -> impl Future<Output = tg::Result<tg::organization::members::list::Output>> {
		self.list_organization_members(organization)
	}

	fn add_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> impl Future<Output = tg::Result<()>> {
		self.add_organization_member(organization, member)
	}

	fn remove_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.remove_organization_member(organization, member)
	}
}
