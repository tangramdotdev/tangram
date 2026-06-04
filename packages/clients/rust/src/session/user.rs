use crate::prelude::*;

impl tg::handle::User for tg::Session {
	fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		self.get_current_user(arg)
	}

	fn try_get_user(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		self.try_get_user(user, arg)
	}

	fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::Output>> {
		self.login_user(arg)
	}

	fn try_get_user_grants(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::user::grants::Output>>> {
		self.try_get_user_grants(user, arg)
	}
}
