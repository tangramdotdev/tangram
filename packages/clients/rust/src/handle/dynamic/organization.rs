use {super::Handle, crate::prelude::*, futures::future::BoxFuture};

impl tg::handle::Organization for Handle {
	fn create_organization(
		&self,
		arg: tg::organization::create::Arg,
	) -> impl Future<Output = tg::Result<tg::organization::create::Output>> {
		self.0.create_organization(arg)
	}

	fn try_get_organization(
		&self,
		organization: &tg::organization::Selector,
	) -> impl Future<Output = tg::Result<Option<tg::Organization>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_organization(organization))
		}
	}

	fn try_delete_organization(
		&self,
		organization: &tg::organization::Selector,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_delete_organization(organization))
		}
	}

	fn try_get_organization_grants(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::organization::grants::Output>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.try_get_organization_grants(organization, arg),
			)
		}
	}

	fn list_organization_members(
		&self,
		organization: &tg::organization::Selector,
	) -> impl Future<Output = tg::Result<tg::organization::members::list::Output>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.list_organization_members(organization),
			)
		}
	}

	fn add_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.add_organization_member(organization, member),
			)
		}
	}

	fn remove_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.remove_organization_member(organization, member),
			)
		}
	}
}
