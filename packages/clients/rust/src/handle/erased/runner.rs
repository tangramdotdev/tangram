use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*, stream::BoxStream},
};

pub trait Runner: Send + Sync + 'static {
	fn create_runner(
		&self,
		arg: tg::runner::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::runner::create::Output>>;

	fn create_runner_token<'a>(
		&'a self,
		runner: &'a tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::token::create::Output>>;

	fn try_delete_runner<'a>(
		&'a self,
		runner: &'a tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_delete_runner_token<'a>(
		&'a self,
		runner: &'a tg::runner::Id,
		token: &'a tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_runners(
		&self,
		arg: tg::runner::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::runner::list::Output>>;

	fn list_runner_tokens<'a>(
		&'a self,
		runner: &'a tg::runner::Id,
		arg: tg::runner::token::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::token::list::Output>>;

	fn get_runner_control_stream<'a>(
		&'a self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::runner::control::Output,
			BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>,
		)>,
	>;
}

impl<T> Runner for T
where
	T: tg::handle::Runner,
{
	fn create_runner(
		&self,
		arg: tg::runner::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::runner::create::Output>> {
		self.create_runner(arg).boxed()
	}

	fn create_runner_token<'a>(
		&'a self,
		runner: &'a tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::token::create::Output>> {
		self.create_runner_token(runner, arg).boxed()
	}

	fn try_delete_runner<'a>(
		&'a self,
		runner: &'a tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_runner(runner, arg).boxed()
	}

	fn try_delete_runner_token<'a>(
		&'a self,
		runner: &'a tg::runner::Id,
		token: &'a tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_runner_token(runner, token, arg).boxed()
	}

	fn list_runners(
		&self,
		arg: tg::runner::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::runner::list::Output>> {
		self.list_runners(arg).boxed()
	}

	fn list_runner_tokens<'a>(
		&'a self,
		runner: &'a tg::runner::Id,
		arg: tg::runner::token::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::runner::token::list::Output>> {
		self.list_runner_tokens(runner, arg).boxed()
	}

	fn get_runner_control_stream<'a>(
		&'a self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> BoxFuture<
		'a,
		tg::Result<(
			tg::runner::control::Output,
			BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>,
		)>,
	> {
		self.get_runner_control_stream(arg, stream)
			.map_ok(|(output, stream)| (output, stream.boxed()))
			.boxed()
	}
}
