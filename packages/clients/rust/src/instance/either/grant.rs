use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::instance::Grant for tg::Either<L, R>
where
	L: tg::instance::Grant,
	R: tg::instance::Grant,
{
	fn grant(
		&self,
		arg: tg::grant::grant::Arg,
	) -> impl Future<Output = tg::Result<tg::grant::grant::Output>> {
		match self {
			tg::Either::Left(s) => s.grant(arg).left_future(),
			tg::Either::Right(s) => s.grant(arg).right_future(),
		}
	}

	fn revoke(&self, arg: tg::grant::revoke::Arg) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.revoke(arg).left_future(),
			tg::Either::Right(s) => s.revoke(arg).right_future(),
		}
	}

	fn try_delete_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.try_delete_grant(grant, arg).left_future(),
			tg::Either::Right(s) => s.try_delete_grant(grant, arg).right_future(),
		}
	}

	fn try_get_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::grant::get::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_get_grant(grant, arg).left_future(),
			tg::Either::Right(s) => s.try_get_grant(grant, arg).right_future(),
		}
	}

	fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::grant::list::Output>>> {
		match self {
			tg::Either::Left(s) => s.list_grants(arg).left_future(),
			tg::Either::Right(s) => s.list_grants(arg).right_future(),
		}
	}
}
