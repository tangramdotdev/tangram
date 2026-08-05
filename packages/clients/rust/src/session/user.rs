use crate::prelude::*;

impl tg::handle::User for tg::Session {
	fn create_user_token(
		&self,
		arg: tg::user::token::create::Arg,
	) -> impl Future<Output = tg::Result<tg::user::token::create::Output>> {
		self.create_user_token(arg)
	}

	fn try_delete_user_token(
		&self,
		token: &tg::token::Id,
		arg: tg::user::token::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_user_token(token, arg)
	}

	fn list_user_tokens(
		&self,
		arg: tg::user::token::list::Arg,
	) -> impl Future<Output = tg::Result<tg::user::token::list::Output>> {
		self.list_user_tokens(arg)
	}

	fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		self.get_current_user(arg)
	}

	fn create_login(
		&self,
		arg: tg::user::login::create::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::create::Output>> {
		self.create_login(arg)
	}

	fn logout(&self) -> impl Future<Output = tg::Result<()>> {
		self.logout()
	}

	fn try_get_user(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::user::get::Output>>> {
		self.try_get_user(user, arg)
	}

	fn manage_user_billing(
		&self,
		arg: tg::user::billing::manage::Arg,
	) -> impl Future<Output = tg::Result<tg::user::billing::manage::Output>> {
		self.manage_user_billing(arg)
	}

	fn wait_login(
		&self,
		arg: tg::user::login::wait::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::wait::Output>> {
		self.wait_login(arg)
	}
}
