use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Group for tg::Either<L, R>
where
	L: tg::handle::Group,
	R: tg::handle::Group,
{
	fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> impl Future<Output = tg::Result<tg::group::create::Output>> {
		match self {
			tg::Either::Left(s) => s.create_group(arg).left_future(),
			tg::Either::Right(s) => s.create_group(arg).right_future(),
		}
	}

	fn list_groups(
		&self,
		arg: tg::group::list::Arg,
	) -> impl Future<Output = tg::Result<tg::group::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_groups(arg).left_future(),
			tg::Either::Right(s) => s.list_groups(arg).right_future(),
		}
	}

	fn try_get_group(&self, group: &str) -> impl Future<Output = tg::Result<Option<tg::Group>>> {
		match self {
			tg::Either::Left(s) => s.try_get_group(group).left_future(),
			tg::Either::Right(s) => s.try_get_group(group).right_future(),
		}
	}

	fn try_delete_group(&self, group: &str) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.try_delete_group(group).left_future(),
			tg::Either::Right(s) => s.try_delete_group(group).right_future(),
		}
	}

	fn list_group_namespace_grants(
		&self,
		group: &str,
		arg: tg::group::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::group::grants::Output>>> {
		match self {
			tg::Either::Left(s) => s.list_group_namespace_grants(group, arg).left_future(),
			tg::Either::Right(s) => s.list_group_namespace_grants(group, arg).right_future(),
		}
	}

	fn list_group_members(
		&self,
		group: &str,
		arg: tg::group::member::list::Arg,
	) -> impl Future<Output = tg::Result<tg::group::member::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_group_members(group, arg).left_future(),
			tg::Either::Right(s) => s.list_group_members(group, arg).right_future(),
		}
	}

	fn add_group_member(&self, group: &str, user: &str) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.add_group_member(group, user).left_future(),
			tg::Either::Right(s) => s.add_group_member(group, user).right_future(),
		}
	}

	fn remove_group_member(
		&self,
		group: &str,
		user: &str,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.remove_group_member(group, user).left_future(),
			tg::Either::Right(s) => s.remove_group_member(group, user).right_future(),
		}
	}
}
