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
		arg: tg::organization::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::Organization>>> {
		match self {
			tg::Either::Left(s) => s.try_get_organization(organization, arg).left_future(),
			tg::Either::Right(s) => s.try_get_organization(organization, arg).right_future(),
		}
	}

	fn try_delete_organization(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.try_delete_organization(organization, arg).left_future(),
			tg::Either::Right(s) => s.try_delete_organization(organization, arg).right_future(),
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
		arg: tg::organization::members::list::Arg,
	) -> impl Future<Output = tg::Result<tg::organization::members::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_organization_members(organization, arg).left_future(),
			tg::Either::Right(s) => s
				.list_organization_members(organization, arg)
				.right_future(),
		}
	}

	fn add_organization_member(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::members::add::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.add_organization_member(organization, arg).left_future(),
			tg::Either::Right(s) => s.add_organization_member(organization, arg).right_future(),
		}
	}

	fn remove_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
		arg: tg::organization::members::remove::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s
				.remove_organization_member(organization, member, arg)
				.left_future(),
			tg::Either::Right(s) => s
				.remove_organization_member(organization, member, arg)
				.right_future(),
		}
	}
}
