use crate::prelude::*;

pub trait User: Clone + Unpin + Send + Sync + 'static {
	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> + Send;

	fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::Output>> + Send;
}

impl tg::handle::User for tg::Client {
	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		self.get_user(token)
	}

	fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::Output>> {
		self.login_user(arg)
	}
}
