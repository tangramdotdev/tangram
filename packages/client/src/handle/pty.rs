use {crate::prelude::*, futures::Stream};

pub trait Pty: Clone + Unpin + Send + Sync + 'static {
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pty::create::Output>> + Send;

	fn close_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::pty::Size>>> + Send;

	fn put_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_read_pty_stream(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>,
		>,
	> + Send;

	fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;
}

impl tg::handle::Pty for tg::Client {
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pty::create::Output>> {
		self.create_pty(arg)
	}

	fn close_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.close_pty(id, arg)
	}

	fn delete_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.delete_pty(id, arg)
	}

	fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::pty::Size>>> {
		self.get_pty_size(id, arg)
	}

	fn put_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_pty_size(id, arg)
	}

	fn try_read_pty_stream(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>,
		>,
	> {
		self.try_read_pty_stream(id, arg)
	}

	fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.write_pty(id, arg)
	}
}
