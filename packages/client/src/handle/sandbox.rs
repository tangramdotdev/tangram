use crate::prelude::*;

pub trait Sandbox: Clone + Unpin + Send + Sync + 'static {
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::create::Output>> + Send;

	fn delete_sandbox(
		&self,
		id: &tg::sandbox::Id,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn sandbox_spawn(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::spawn::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::spawn::Output>> + Send;

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
	> + Send;
}

impl tg::handle::Sandbox for tg::Client {
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::create::Output>> {
		self.create_sandbox(arg)
	}

	fn delete_sandbox(
		&self,
		id: &tg::sandbox::Id,
	) -> impl Future<Output = tg::Result<()>> {
		self.delete_sandbox(id)
	}

	fn sandbox_spawn(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::spawn::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::spawn::Output>> {
		self.sandbox_spawn(id, arg)
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
		self.try_sandbox_wait_future(id, arg)
	}
}
