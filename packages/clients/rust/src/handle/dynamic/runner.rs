use {
	super::Handle,
	crate::prelude::*,
	futures::{Stream, future::BoxFuture, stream::BoxStream},
};

impl tg::handle::Runner for Handle {
	fn get_runner_control_stream(
		&self,
		id: &tg::runner::Id,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::InputEvent>>,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::runner::control::OutputEvent>> + Send + 'static,
		>,
	> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<BoxStream<_>>>>(
				self.0.get_runner_control_stream(id, arg, stream),
			)
		}
	}
}
