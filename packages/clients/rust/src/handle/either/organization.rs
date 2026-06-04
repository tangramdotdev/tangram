use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Organization for tg::Either<L, R>
where
	L: tg::handle::Organization,
	R: tg::handle::Organization,
{
	fn create_organization(
		&self,
		arg: tg::organization::create::Arg,
	) -> impl Future<Output = tg::Result<tg::organization::create::Output>> {
		match self {
			tg::Either::Left(s) => s.create_organization(arg).left_future(),
			tg::Either::Right(s) => s.create_organization(arg).right_future(),
		}
	}

	fn try_get_organization(
		&self,
		organization: &tg::organization::Selector,
	) -> impl Future<Output = tg::Result<Option<tg::Organization>>> {
		match self {
			tg::Either::Left(s) => s.try_get_organization(organization).left_future(),
			tg::Either::Right(s) => s.try_get_organization(organization).right_future(),
		}
	}

	fn try_delete_organization(
		&self,
		organization: &tg::organization::Selector,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.try_delete_organization(organization).left_future(),
			tg::Either::Right(s) => s.try_delete_organization(organization).right_future(),
		}
	}

	fn try_get_organization_grants(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::organization::grants::Output>>> {
		match self {
			tg::Either::Left(s) => s
				.try_get_organization_grants(organization, arg)
				.left_future(),
			tg::Either::Right(s) => s
				.try_get_organization_grants(organization, arg)
				.right_future(),
		}
	}

	fn list_organization_members(
		&self,
		organization: &tg::organization::Selector,
	) -> impl Future<Output = tg::Result<tg::organization::members::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_organization_members(organization).left_future(),
			tg::Either::Right(s) => s.list_organization_members(organization).right_future(),
		}
	}

	fn add_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s
				.add_organization_member(organization, member)
				.left_future(),
			tg::Either::Right(s) => s
				.add_organization_member(organization, member)
				.right_future(),
		}
	}

	fn remove_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s
				.remove_organization_member(organization, member)
				.left_future(),
			tg::Either::Right(s) => s
				.remove_organization_member(organization, member)
				.right_future(),
		}
	}
}
