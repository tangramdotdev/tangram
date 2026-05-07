use crate::prelude::*;

impl tg::handle::User for tg::Session {
	fn get_user(
		&self,
		arg: tg::user::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		self.get_user(arg)
	}

	fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::Output>> {
		self.login_user(arg)
	}
}
