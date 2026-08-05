use {
	crate::prelude::*,
	futures::{Stream, stream::BoxStream},
};

impl tg::handle::Runner for tg::Session {
	fn create_runner(
		&self,
		arg: tg::runner::create::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::create::Output>> {
		self.create_runner(arg)
	}

	fn create_runner_token(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::token::create::Output>> {
		self.create_runner_token(runner, arg)
	}

	fn try_delete_runner(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_runner(runner, arg)
	}

	fn try_delete_runner_token(
		&self,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_runner_token(runner, token, arg)
	}

	fn list_runners(
		&self,
		arg: tg::runner::list::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::list::Output>> {
		self.list_runners(arg)
	}

	fn list_runner_tokens(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::token::list::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::token::list::Output>> {
		self.list_runner_tokens(runner, arg)
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
	> {
		self.get_runner_control_stream(arg, stream)
	}
}
