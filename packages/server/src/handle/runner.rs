use {
	crate::Server,
	futures::{Stream, stream::BoxStream},
	tangram_client::prelude::*,
};

impl tg::handle::Runner for Server {
	async fn get_runner_control_stream(
		&self,
		id: &tg::runner::Id,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::runner::control::ServerMessage>> + Send + 'static,
	> {
		self.session(&self.context)
			.get_runner_control_stream_with_context(id, arg, stream)
			.await
	}
}
