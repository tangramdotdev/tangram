use {super::Handle, crate::prelude::*, futures::future::BoxFuture};

impl tg::handle::User for Handle {
	fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.get_current_user(arg)) }
	}

	fn create_login(
		&self,
		arg: tg::user::login::create::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::create::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.create_login(arg)) }
	}

	fn try_get_user(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_user(user, arg)) }
	}

	fn wait_login(
		&self,
		arg: tg::user::login::wait::Arg,
	) -> impl Future<Output = tg::Result<tg::user::login::wait::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.wait_login(arg)) }
	}
}
