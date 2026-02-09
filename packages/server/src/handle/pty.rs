use {
	super::ServerWithContext,
	crate::{Context, Owned, Server},
	futures::Stream,
	tangram_client::prelude::*,
};

impl tg::handle::Pty for Owned {
	async fn create_pty(&self, arg: tg::pty::create::Arg) -> tg::Result<tg::pty::create::Output> {
		self.0.create_pty(arg).await
	}

	async fn close_pty(&self, id: &tg::pty::Id, arg: tg::pty::close::Arg) -> tg::Result<()> {
		self.0.close_pty(id, arg).await
	}

	async fn delete_pty(&self, id: &tg::pty::Id, arg: tg::pty::delete::Arg) -> tg::Result<()> {
		self.0.delete_pty(id, arg).await
	}

	async fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::get::Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		self.0.get_pty_size(id, arg).await
	}

	async fn put_pty_size(&self, id: &tg::pty::Id, arg: tg::pty::size::put::Arg) -> tg::Result<()> {
		self.0.put_pty_size(id, arg).await
	}

	async fn try_read_pty_stream(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>> {
		self.0.try_read_pty_stream(id, arg).await
	}

	async fn write_pty(&self, id: &tg::pty::Id, arg: tg::pty::write::Arg) -> tg::Result<()> {
		self.0.write_pty(id, arg).await
	}
}

impl tg::handle::Pty for Server {
	async fn create_pty(&self, arg: tg::pty::create::Arg) -> tg::Result<tg::pty::create::Output> {
		self.create_pty_with_context(&Context::default(), arg).await
	}

	async fn close_pty(&self, id: &tg::pty::Id, arg: tg::pty::close::Arg) -> tg::Result<()> {
		self.close_pty_with_context(&Context::default(), id, arg)
			.await
	}

	async fn delete_pty(&self, id: &tg::pty::Id, arg: tg::pty::delete::Arg) -> tg::Result<()> {
		self.delete_pty_with_context(&Context::default(), id, arg)
			.await
	}

	async fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::get::Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		self.try_get_pty_size_with_context(&Context::default(), id, arg)
			.await
	}

	async fn put_pty_size(&self, id: &tg::pty::Id, arg: tg::pty::size::put::Arg) -> tg::Result<()> {
		self.try_put_pty_size_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_read_pty_stream(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>> {
		self.try_read_pty_with_context(&Context::default(), id, arg)
			.await
	}

	async fn write_pty(&self, id: &tg::pty::Id, arg: tg::pty::write::Arg) -> tg::Result<()> {
		self.write_pty_with_context(&Context::default(), id, arg)
			.await
	}
}

impl tg::handle::Pty for ServerWithContext {
	async fn create_pty(&self, arg: tg::pty::create::Arg) -> tg::Result<tg::pty::create::Output> {
		self.0.create_pty_with_context(&self.1, arg).await
	}

	async fn close_pty(&self, id: &tg::pty::Id, arg: tg::pty::close::Arg) -> tg::Result<()> {
		self.0.close_pty_with_context(&self.1, id, arg).await
	}

	async fn delete_pty(&self, id: &tg::pty::Id, arg: tg::pty::delete::Arg) -> tg::Result<()> {
		self.0.delete_pty_with_context(&self.1, id, arg).await
	}

	async fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::get::Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		self.0.try_get_pty_size_with_context(&self.1, id, arg).await
	}

	async fn put_pty_size(&self, id: &tg::pty::Id, arg: tg::pty::size::put::Arg) -> tg::Result<()> {
		self.0.try_put_pty_size_with_context(&self.1, id, arg).await
	}

	async fn try_read_pty_stream(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>> {
		self.0.try_read_pty_with_context(&self.1, id, arg).await
	}

	async fn write_pty(&self, id: &tg::pty::Id, arg: tg::pty::write::Arg) -> tg::Result<()> {
		self.0.write_pty_with_context(&self.1, id, arg).await
	}
}
