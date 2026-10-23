use {crate::Session, tangram_client::prelude::*};

impl tg::instance::Organization for Session {
	async fn create_organization(
		&self,
		arg: tg::organization::create::Arg,
	) -> tg::Result<tg::organization::create::Output> {
		self.server
			.components
			.organization
			.create_organization(self, arg)
			.await
	}

	async fn try_get_organization(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::get::Arg,
	) -> tg::Result<Option<tg::organization::get::Output>> {
		self.server
			.components
			.organization
			.try_get_organization(self, organization, arg)
			.await
	}

	async fn try_get_organization_usage(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::usage::Arg,
	) -> tg::Result<Option<tg::usage::Output>> {
		self.server
			.components
			.organization
			.try_get_organization_usage(self, organization, arg)
			.await
	}

	async fn try_delete_organization(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.organization
			.try_delete_organization(self, organization, arg)
			.await
	}

	async fn list_organization_members(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::members::list::Arg,
	) -> tg::Result<tg::organization::members::list::Output> {
		self.server
			.components
			.organization
			.list_organization_members(self, organization, arg)
			.await
	}

	async fn add_organization_member(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::members::add::Arg,
	) -> tg::Result<()> {
		self.server
			.components
			.organization
			.add_organization_member(self, organization, arg)
			.await
	}

	async fn remove_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
		arg: tg::organization::members::remove::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.organization
			.remove_organization_member(self, organization, member, arg)
			.await
	}

	async fn manage_organization_billing(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::billing::manage::Arg,
	) -> tg::Result<tg::organization::billing::manage::Output> {
		self.server
			.components
			.organization
			.manage_organization_billing(self, organization, arg)
			.await
	}
}
