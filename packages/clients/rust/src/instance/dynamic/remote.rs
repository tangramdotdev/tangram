use {super::Instance, crate::prelude::*, futures::future::BoxFuture};

impl tg::instance::Remote for Instance {
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> {
		self.0.list_remotes(arg)
	}

	fn try_get_remote(
		&self,
		remote: &tg::remote::Selector,
		arg: tg::remote::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_remote(remote, arg)) }
	}

	fn put_remote(
		&self,
		remote: &tg::remote::Selector,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.put_remote(remote, arg)) }
	}

	fn try_delete_remote(
		&self,
		remote: &tg::remote::Selector,
		arg: tg::remote::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_delete_remote(remote, arg)) }
	}
}
