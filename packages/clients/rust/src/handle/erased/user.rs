use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait User: Send + Sync + 'static {
	fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::User>>>;

	fn try_get_user<'a>(
		&'a self,
		user: &'a tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::User>>>;

	fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::login::Output>>;

	fn try_get_user_grants<'a>(
		&'a self,
		user: &'a tg::user::Selector,
		arg: tg::user::grants::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::user::grants::Output>>>;
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

	fn try_get_user<'a>(
		&'a self,
		user: &'a tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::User>>> {
		self.try_get_user(user, arg).boxed()
	}

	fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::login::Output>> {
		self.login_user(arg).boxed()
	}

	fn try_get_user_grants<'a>(
		&'a self,
		user: &'a tg::user::Selector,
		arg: tg::user::grants::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::user::grants::Output>>> {
		self.try_get_user_grants(user, arg).boxed()
	}
}
