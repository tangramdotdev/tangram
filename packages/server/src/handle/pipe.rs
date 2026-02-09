use {
	super::ServerWithContext,
	crate::{Context, Owned, Server},
	futures::Stream,
	tangram_client::prelude::*,
};

impl tg::handle::Pipe for Owned {
	async fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> tg::Result<tg::pipe::create::Output> {
		self.0.create_pipe(arg).await
	}

	async fn close_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::close::Arg) -> tg::Result<()> {
		self.0.close_pipe(id, arg).await
	}

	async fn delete_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::delete::Arg) -> tg::Result<()> {
		self.0.delete_pipe(id, arg).await
	}

	async fn try_read_pipe_stream(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>> {
		self.0.try_read_pipe_stream(id, arg).await
	}

	async fn write_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::write::Arg) -> tg::Result<()> {
		self.0.write_pipe(id, arg).await
	}
}

impl tg::handle::Pipe for Server {
	async fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> tg::Result<tg::pipe::create::Output> {
		self.create_pipe_with_context(&Context::default(), arg)
			.await
	}

	async fn close_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::close::Arg) -> tg::Result<()> {
		self.close_pipe_with_context(&Context::default(), id, arg)
			.await
	}

	async fn delete_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::delete::Arg) -> tg::Result<()> {
		self.delete_pipe_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_read_pipe_stream(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>> {
		self.try_read_pipe_with_context(&Context::default(), id, arg)
			.await
	}

	async fn write_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::write::Arg) -> tg::Result<()> {
		self.write_pipe_with_context(&Context::default(), id, arg)
			.await
	}
}

impl tg::handle::Pipe for ServerWithContext {
	async fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> tg::Result<tg::pipe::create::Output> {
		self.0.create_pipe_with_context(&self.1, arg).await
	}

	async fn close_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::close::Arg) -> tg::Result<()> {
		self.0.close_pipe_with_context(&self.1, id, arg).await
	}

	async fn delete_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::delete::Arg) -> tg::Result<()> {
		self.0.delete_pipe_with_context(&self.1, id, arg).await
	}

	async fn try_read_pipe_stream(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>> {
		self.0.try_read_pipe_with_context(&self.1, id, arg).await
	}

	async fn write_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::write::Arg) -> tg::Result<()> {
		self.0.write_pipe_with_context(&self.1, id, arg).await
	}
}
