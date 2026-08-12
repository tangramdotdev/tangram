use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::User for tg::Either<L, R>
where
	L: tg::handle::User,
	R: tg::handle::User,
{
	fn create_user_token(
		&self,
		arg: tg::user::token::create::Arg,
	) -> impl Future<Output = tg::Result<tg::user::token::create::Output>> {
		match self {
			tg::Either::Left(s) => s.create_user_token(arg).left_future(),
			tg::Either::Right(s) => s.create_user_token(arg).right_future(),
		}
	}

	fn try_delete_user_token(
		&self,
		token: &tg::token::Id,
		arg: tg::user::token::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.try_delete_user_token(token, arg).left_future(),
			tg::Either::Right(s) => s.try_delete_user_token(token, arg).right_future(),
		}
	}

	fn list_user_tokens(
		&self,
		arg: tg::user::token::list::Arg,
	) -> impl Future<Output = tg::Result<tg::user::token::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_user_tokens(arg).left_future(),
			tg::Either::Right(s) => s.list_user_tokens(arg).right_future(),
		}
	}

	fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		match self {
			tg::Either::Left(s) => s.get_current_user(arg).left_future(),
			tg::Either::Right(s) => s.get_current_user(arg).right_future(),
		}
	}

	fn create_login(
		&self,
		arg: tg::user::login::create::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::create::Output>> {
		match self {
			tg::Either::Left(s) => s.create_login(arg).left_future(),
			tg::Either::Right(s) => s.create_login(arg).right_future(),
		}
	}

	fn logout(&self) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.logout().left_future(),
			tg::Either::Right(s) => s.logout().right_future(),
		}
	}

	fn try_get_user(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::user::get::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_get_user(user, arg).left_future(),
			tg::Either::Right(s) => s.try_get_user(user, arg).right_future(),
		}
	}

	fn try_get_user_usage(
		&self,
		user: &tg::user::Selector,
		arg: tg::usage::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::usage::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_get_user_usage(user, arg).left_future(),
			tg::Either::Right(s) => s.try_get_user_usage(user, arg).right_future(),
		}
	}

	fn manage_user_billing(
		&self,
		arg: tg::user::billing::manage::Arg,
	) -> impl Future<Output = tg::Result<tg::user::billing::manage::Output>> {
		match self {
			tg::Either::Left(s) => s.manage_user_billing(arg).left_future(),
			tg::Either::Right(s) => s.manage_user_billing(arg).right_future(),
		}
	}

	fn wait_login(
		&self,
		arg: tg::user::login::wait::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::wait::Output>> {
		match self {
			tg::Either::Left(s) => s.wait_login(arg).left_future(),
			tg::Either::Right(s) => s.wait_login(arg).right_future(),
		}
	}
}
