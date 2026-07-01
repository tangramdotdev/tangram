use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait User: Send + Sync + 'static {
	fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::User>>>;

	fn create_login(
		&self,
		arg: tg::user::login::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::login::create::Output>>;

	fn try_get_user<'a>(
		&'a self,
		user: &'a tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::User>>>;

	fn wait_login(
		&self,
		arg: tg::user::login::wait::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::login::wait::Output>>;
}

impl<T> User for T
where
	T: tg::handle::User,
{
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

	fn try_get_user<'a>(
		&'a self,
		user: &'a tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::User>>> {
		self.try_get_user(user, arg).boxed()
	}

	fn wait_login(
		&self,
		arg: tg::user::login::wait::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::login::wait::Output>> {
		self.wait_login(arg).boxed()
	}
}
