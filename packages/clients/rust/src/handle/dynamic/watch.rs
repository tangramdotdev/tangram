use {super::Handle, crate::prelude::*, futures::future::BoxFuture};

impl tg::handle::Watch for Handle {
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> impl Future<Output = tg::Result<tg::watch::list::Output>> {
		self.0.list_watches(arg)
	}

	fn delete_watch(&self, arg: tg::watch::delete::Arg) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.delete_watch(arg)) }
	}

	fn touch_watch(&self, arg: tg::watch::touch::Arg) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.touch_watch(arg)) }
	}
}
