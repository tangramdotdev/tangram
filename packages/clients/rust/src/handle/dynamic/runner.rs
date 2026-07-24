use {
	super::Handle,
	crate::prelude::*,
	futures::{Stream, future::BoxFuture, stream::BoxStream},
};

impl tg::handle::Runner for Handle {
	fn get_runner_control_stream(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<(
			tg::runner::control::Output,
			impl Stream<Item = tg::Result<tg::runner::control::ServerMessage>> + Send + 'static,
		)>,
	> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<(_, BoxStream<_>)>>>(
				self.0.get_runner_control_stream(arg, stream),
			)
		}
	}
}
