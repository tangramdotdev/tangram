use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait User: Send + Sync + 'static {
	fn create_user_token(
		&self,
		arg: tg::user::token::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::token::create::Output>>;

	fn try_delete_user_token<'a>(
		&'a self,
		token: &'a tg::token::Id,
		arg: tg::user::token::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_user_tokens(
		&self,
		arg: tg::user::token::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::token::list::Output>>;

	fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::User>>>;

	fn create_login(
		&self,
		arg: tg::user::login::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::login::create::Output>>;

	fn logout(&self) -> BoxFuture<'_, tg::Result<()>>;

	fn try_get_user<'a>(
		&'a self,
		user: &'a tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::user::get::Output>>>;

	fn try_get_user_usage<'a>(
		&'a self,
		user: &'a tg::user::Selector,
		arg: tg::usage::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::usage::Output>>>;

	fn manage_user_billing(
		&self,
		arg: tg::user::billing::manage::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::billing::manage::Output>>;

	fn wait_login(
		&self,
		arg: tg::user::login::wait::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::login::wait::Output>>;
}

impl<T> User for T
where
	T: tg::handle::User,
{
	fn create_user_token(
		&self,
		arg: tg::user::token::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::token::create::Output>> {
		self.create_user_token(arg).boxed()
	}

	fn try_delete_user_token<'a>(
		&'a self,
		token: &'a tg::token::Id,
		arg: tg::user::token::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_user_token(token, arg).boxed()
	}

	fn list_user_tokens(
		&self,
		arg: tg::user::token::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::token::list::Output>> {
		self.list_user_tokens(arg).boxed()
	}

	fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::User>>> {
		self.get_current_user(arg).boxed()
	}

	fn create_login(
		&self,
		arg: tg::user::login::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::login::create::Output>> {
		self.create_login(arg).boxed()
	}

	fn logout(&self) -> BoxFuture<'_, tg::Result<()>> {
		self.logout().boxed()
	}

	fn try_get_user<'a>(
		&'a self,
		user: &'a tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::user::get::Output>>> {
		self.try_get_user(user, arg).boxed()
	}

	fn try_get_user_usage<'a>(
		&'a self,
		user: &'a tg::user::Selector,
		arg: tg::usage::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::usage::Output>>> {
		self.try_get_user_usage(user, arg).boxed()
	}

	fn manage_user_billing(
		&self,
		arg: tg::user::billing::manage::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::billing::manage::Output>> {
		self.manage_user_billing(arg).boxed()
	}

	fn wait_login(
		&self,
		arg: tg::user::login::wait::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::login::wait::Output>> {
		self.wait_login(arg).boxed()
	}
}
