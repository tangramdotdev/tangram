use {
	crate::Session,
	futures::{Stream, stream::BoxStream},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl tg::instance::Runner for Session {
	async fn create_runner(
		&self,
		arg: tg::runner::create::Arg,
	) -> tg::Result<tg::runner::create::Output> {
		self.server.components.runner.create_runner(self, arg).await
	}

	async fn create_runner_token(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> tg::Result<tg::runner::token::create::Output> {
		self.server
			.components
			.runner
			.create_runner_token(self, runner, arg)
			.await
	}

	async fn try_delete_runner(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.runner
			.try_delete_runner(self, runner, arg)
			.await
	}

	async fn try_get_runner(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::get::Arg,
	) -> tg::Result<Option<tg::runner::get::Output>> {
		self.server
			.components
			.runner
			.try_get_runner(self, runner, arg)
			.await
	}

	async fn try_delete_runner_token(
		&self,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.runner
			.try_delete_runner_token(self, runner, token, arg)
			.await
	}

	async fn try_get_runner_token(
		&self,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		arg: tg::runner::token::get::Arg,
	) -> tg::Result<Option<tg::runner::token::get::Output>> {
		self.server
			.components
			.runner
			.try_get_runner_token(self, runner, token, arg)
			.await
	}

	async fn list_runners(
		&self,
		arg: tg::runner::list::Arg,
	) -> tg::Result<tg::runner::list::Output> {
		self.server.components.runner.list_runners(self, arg).await
	}

	async fn list_runner_tokens(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::token::list::Arg,
	) -> tg::Result<tg::runner::token::list::Output> {
		self.server
			.components
			.runner
			.list_runner_tokens(self, runner, arg)
			.await
	}

	async fn get_runner_control_stream(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> tg::Result<
		ControlFlow<
			(
				tg::runner::control::Header,
				impl Stream<Item = tg::Result<tg::runner::control::ServerMessage>> + Send + 'static,
			),
			tg::Error,
		>,
	> {
		self.server
			.components
			.runner
			.get_runner_control_stream(self, arg, stream)
			.await
	}
}
