use {
	crate::prelude::*,
	futures::{FutureExt as _, Stream, TryFutureExt as _, stream::BoxStream},
};

impl<L, R> tg::handle::Runner for tg::Either<L, R>
where
	L: tg::handle::Runner,
	R: tg::handle::Runner,
{
	fn get_runner_control_stream(
		&self,
		id: &tg::runner::Id,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::runner::control::ServerMessage>> + Send + 'static,
		>,
	> + Send {
		match self {
			tg::Either::Left(s) => s
				.get_runner_control_stream(id, arg, stream)
				.map_ok(futures::StreamExt::left_stream)
				.left_future(),
			tg::Either::Right(s) => s
				.get_runner_control_stream(id, arg, stream)
				.map_ok(futures::StreamExt::right_stream)
				.right_future(),
		}
	}
}
