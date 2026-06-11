use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::User for tg::Either<L, R>
where
	L: tg::handle::User,
	R: tg::handle::User,
{
	fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		match self {
			tg::Either::Left(s) => s.get_current_user(arg).left_future(),
			tg::Either::Right(s) => s.get_current_user(arg).right_future(),
		}
	}

	fn try_get_user(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		match self {
			tg::Either::Left(s) => s.try_get_user(user, arg).left_future(),
			tg::Either::Right(s) => s.try_get_user(user, arg).right_future(),
		}
	}

	fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::Output>> {
		match self {
			tg::Either::Left(s) => s.login_user(arg).left_future(),
			tg::Either::Right(s) => s.login_user(arg).right_future(),
		}
	}
}
