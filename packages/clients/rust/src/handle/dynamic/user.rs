use {super::Handle, crate::prelude::*, futures::future::BoxFuture};

impl tg::handle::User for Handle {
	fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.get_current_user(arg)) }
	}

	fn login_user(
		&self,
		arg: tg::user::login::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.login_user(arg)) }
	}

	fn list_user_namespace_grants(
		&self,
		user: &str,
		arg: tg::user::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::user::grants::Output>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.list_user_namespace_grants(user, arg))
		}
	}
}
