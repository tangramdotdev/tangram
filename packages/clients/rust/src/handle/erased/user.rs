use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait User: Send + Sync + 'static {
	fn get_user<'a>(&'a self, token: &'a str) -> BoxFuture<'a, tg::Result<Option<tg::User>>>;

	fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::login::Output>>;
}

impl<T> User for T
where
	T: tg::handle::User,
{
	fn get_user<'a>(&'a self, token: &'a str) -> BoxFuture<'a, tg::Result<Option<tg::User>>> {
		self.get_user(token).boxed()
	}

	fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> BoxFuture<'_, tg::Result<tg::user::login::Output>> {
		self.login_user(arg).boxed()
	}
}
