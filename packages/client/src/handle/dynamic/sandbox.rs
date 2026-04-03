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
}
