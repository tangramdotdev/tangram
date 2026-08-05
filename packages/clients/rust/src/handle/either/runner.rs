use {
	crate::prelude::*,
	futures::{FutureExt as _, Stream, StreamExt as _, TryFutureExt as _, stream::BoxStream},
};

impl<L, R> tg::handle::Runner for tg::Either<L, R>
where
	L: tg::handle::Runner,
	R: tg::handle::Runner,
{
	fn create_runner(
		&self,
		arg: tg::runner::create::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::create::Output>> {
		match self {
			tg::Either::Left(s) => s.create_runner(arg).left_future(),
			tg::Either::Right(s) => s.create_runner(arg).right_future(),
		}
	}

	fn create_runner_token(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::token::create::Output>> {
		match self {
			tg::Either::Left(s) => s.create_runner_token(runner, arg).left_future(),
			tg::Either::Right(s) => s.create_runner_token(runner, arg).right_future(),
		}
	}

	fn try_delete_runner(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.try_delete_runner(runner, arg).left_future(),
			tg::Either::Right(s) => s.try_delete_runner(runner, arg).right_future(),
		}
	}

	fn try_delete_runner_token(
		&self,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.try_delete_runner_token(runner, token, arg).left_future(),
			tg::Either::Right(s) => s.try_delete_runner_token(runner, token, arg).right_future(),
		}
	}

	fn list_runners(
		&self,
		arg: tg::runner::list::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_runners(arg).left_future(),
			tg::Either::Right(s) => s.list_runners(arg).right_future(),
		}
	}

	fn list_runner_tokens(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::token::list::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::token::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_runner_tokens(runner, arg).left_future(),
			tg::Either::Right(s) => s.list_runner_tokens(runner, arg).right_future(),
		}
	}

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
