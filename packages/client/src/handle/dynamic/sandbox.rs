use crate::prelude::*;

impl tg::handle::Sandbox for super::Handle {
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::create::Output>> {
		self.0.create_sandbox(arg)
	}

	fn delete_sandbox(&self, id: &tg::sandbox::Id) -> impl Future<Output = tg::Result<()>> {
		unsafe {
			std::mem::transmute::<_, futures::future::BoxFuture<'_, tg::Result<()>>>(
				self.0.delete_sandbox(id),
			)
		}
	}

	fn sandbox_spawn(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::spawn::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::spawn::Output>> {
		unsafe {
			std::mem::transmute::<
				_,
				futures::future::BoxFuture<'_, tg::Result<tg::sandbox::spawn::Output>>,
			>(self.0.sandbox_spawn(id, arg))
		}
	}

	fn try_sandbox_wait_future(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Future<Output = tg::Result<Option<tg::sandbox::wait::Output>>> + Send + 'static,
			>,
		>,
	> {
		unsafe {
			std::mem::transmute::<
				_,
				futures::future::BoxFuture<
					'_,
					tg::Result<
						Option<
							futures::future::BoxFuture<
								'static,
								tg::Result<Option<tg::sandbox::wait::Output>>,
							>,
						>,
					>,
				>,
			>(self.0.try_sandbox_wait_future(id, arg))
		}
	}
}
