use {super::Handle, crate::prelude::*, futures::future::BoxFuture};

impl tg::handle::User for Handle {
	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.get_user(token)) }
	}

	fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.login_user(arg)) }
	}
}
