use {super::Instance, crate::prelude::*, futures::future::BoxFuture};

impl tg::instance::Clock for Instance {
	fn try_get_clock(&self) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> {
		// SAFETY: The erased future borrows the instance for the returned future's lifetime.
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_clock()) }
	}

	fn try_set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> {
		// SAFETY: The erased future borrows the instance for the returned future's lifetime.
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_set_clock(arg)) }
	}

	fn try_advance_clock(
		&self,
		arg: tg::clock::advance::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> {
		// SAFETY: The erased future borrows the instance for the returned future's lifetime.
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_advance_clock(arg)) }
	}
}
