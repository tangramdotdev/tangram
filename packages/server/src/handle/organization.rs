use {crate::Server, tangram_client::prelude::*};

impl tg::handle::Organization for Server {
	async fn create_organization(
		&self,
		arg: tg::organization::create::Arg,
	) -> tg::Result<tg::organization::create::Output> {
		self.session(&self.context).create_organization(arg).await
	}

	async fn try_get_organization(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<Option<tg::Organization>> {
		self.session(&self.context)
			.try_get_organization(organization)
			.await
	}

	async fn try_delete_organization(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_delete_organization(organization)
			.await
	}

	async fn try_get_organization_grants(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::grants::Arg,
	) -> tg::Result<Option<tg::organization::grants::Output>> {
		self.session(&self.context)
			.try_get_organization_grants(organization, arg)
			.await
	}

	async fn list_organization_members(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<tg::organization::members::list::Output> {
		self.session(&self.context)
			.list_organization_members(organization)
			.await
	}

	async fn add_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<()> {
		self.session(&self.context)
			.add_organization_member(organization, member)
			.await
	}

	async fn remove_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.remove_organization_member(organization, member)
			.await
	}
}
