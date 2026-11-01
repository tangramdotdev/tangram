use {
	super::Instance,
	crate::prelude::*,
	futures::{future::BoxFuture, stream::BoxStream},
	std::ops::ControlFlow,
};

impl tg::instance::Sandbox for Instance {
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::create::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.create_sandbox(arg)) }
	}

	fn try_get_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::sandbox::get::Output>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_sandbox(id, arg)) }
	}

	fn try_get_sandbox_processes_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl futures::Stream<Item = tg::Result<tg::sandbox::processes::get::Event>>
				+ Send
				+ 'static,
			>,
		>,
	> {
		// SAFETY: The erased instance returns the same boxed future and stream output types.
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_get_sandbox_processes_stream(id, arg),
			)
		}
	}

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::list::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.list_sandboxes(arg)) }
	}

	fn try_destroy_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> impl Future<Output = tg::Result<Option<bool>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_destroy_sandbox(id, arg)) }
	}

	fn try_stop_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::stop::Arg,
	) -> impl Future<Output = tg::Result<Option<bool>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_stop_sandbox(id, arg)) }
	}

	fn try_start_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::start::Arg,
	) -> impl Future<Output = tg::Result<Option<bool>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_start_sandbox(id, arg)) }
	}

	fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl futures::Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static,
			>,
		>,
	> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_get_sandbox_status_stream(id, arg),
			)
		}
	}

	fn try_wait_sandbox_future(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Future<Output = tg::Result<Option<tg::sandbox::outcome::Data>>> + Send + 'static,
			>,
		>,
	> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxFuture<_>>>>>(
				self.0.try_wait_sandbox_future(id, arg),
			)
		}
	}

	fn get_sandbox_control_stream(
		&self,
		arg: tg::sandbox::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::sandbox::control::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<
			ControlFlow<
				(
					tg::sandbox::control::Header,
					impl futures::Stream<Item = tg::Result<tg::sandbox::control::ServerMessage>>
					+ Send
					+ 'static,
				),
				tg::Error,
			>,
		>,
	> {
		unsafe {
			std::mem::transmute::<
				_,
				BoxFuture<'_, tg::Result<ControlFlow<(_, BoxStream<_>), tg::Error>>>,
			>(self.0.get_sandbox_control_stream(arg, stream))
		}
	}
}
