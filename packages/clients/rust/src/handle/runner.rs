use {
	crate::prelude::*,
	futures::{Stream, stream::BoxStream},
};

pub trait Runner: Clone + Unpin + Send + Sync + 'static {
	fn create_runner(
		&self,
		arg: tg::runner::create::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::create::Output>> + Send;

	fn create_runner_token(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::token::create::Output>> + Send;

	fn try_delete_runner(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

	fn try_delete_runner_token(
		&self,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

	fn list_runners(
		&self,
		arg: tg::runner::list::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::list::Output>> + Send;

	fn list_runner_tokens(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::token::list::Arg,
	) -> impl Future<Output = tg::Result<tg::runner::token::list::Output>> + Send;

	fn get_runner_control_stream(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> impl Future<
		Output = tg::Result<(
			tg::runner::control::Output,
			impl Stream<Item = tg::Result<tg::runner::control::ServerMessage>> + Send + 'static,
		)>,
	> + Send;
}

impl tg::handle::Runner for tg::Client {
	async fn create_runner(
		&self,
		arg: tg::runner::create::Arg,
	) -> tg::Result<tg::runner::create::Output> {
		self.session(&self.context).create_runner(arg).await
	}

	async fn create_runner_token(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::token::create::Arg,
	) -> tg::Result<tg::runner::token::create::Output> {
		self.session(&self.context)
			.create_runner_token(runner, arg)
			.await
	}

	async fn try_delete_runner(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_delete_runner(runner, arg)
			.await
	}

	async fn try_delete_runner_token(
		&self,
		runner: &tg::runner::Id,
		token: &tg::token::Id,
		arg: tg::runner::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_delete_runner_token(runner, token, arg)
			.await
	}

	async fn list_runners(
		&self,
		arg: tg::runner::list::Arg,
	) -> tg::Result<tg::runner::list::Output> {
		self.session(&self.context).list_runners(arg).await
	}

	async fn list_runner_tokens(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::token::list::Arg,
	) -> tg::Result<tg::runner::token::list::Output> {
		self.session(&self.context)
			.list_runner_tokens(runner, arg)
			.await
	}

	async fn get_runner_control_stream(
		&self,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> tg::Result<(
		tg::runner::control::Output,
		impl Stream<Item = tg::Result<tg::runner::control::ServerMessage>> + Send + 'static,
	)> {
		self.session(&self.context)
			.get_runner_control_stream(arg, stream)
			.await
	}
}
