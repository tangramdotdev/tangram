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
		arg: tg::organization::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::Organization>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.try_get_organization(organization, arg),
			)
		}
	}

	fn try_delete_organization(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.try_delete_organization(organization, arg),
			)
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
		arg: tg::organization::members::list::Arg,
	) -> impl Future<Output = tg::Result<tg::organization::members::list::Output>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.list_organization_members(organization, arg),
			)
		}
	}

	fn add_organization_member(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::members::add::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.add_organization_member(organization, arg),
			)
		}
	}

	fn remove_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
		arg: tg::organization::members::remove::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.remove_organization_member(
				organization,
				member,
				arg,
			))
		}
	}
}
