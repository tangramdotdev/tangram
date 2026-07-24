use {
	crate::prelude::*,
	futures::{FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, stream::BoxStream},
};

impl<L, R> tg::handle::Runner for tg::Either<L, R>
where
	L: tg::handle::Runner,
	R: tg::handle::Runner,
{
	fn get_runner_control_stream(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<(
			tg::runner::control::Output,
			impl Stream<Item = tg::Result<tg::runner::control::ServerMessage>> + Send + 'static,
		)>,
	> + Send {
		match self {
			tg::Either::Left(s) => s
				.get_runner_control_stream(arg, stream)
				.map_ok(|(output, stream)| (output, stream.left_stream()))
				.left_future(),
			tg::Either::Right(s) => s
				.get_runner_control_stream(arg, stream)
				.map_ok(|(output, stream)| (output, stream.right_stream()))
				.right_future(),
		}
	}
}
