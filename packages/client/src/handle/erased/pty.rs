use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*, stream::BoxStream},
};

pub trait Pty: Send + Sync + 'static {
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::pty::create::Output>>;

	fn close_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn delete_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn get_pty_size<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::size::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::pty::Size>>>;

	fn put_pty_size<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::size::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_read_pty_stream<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::pty::Event>>>>>;

	fn write_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::write::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;
}

impl<T> Pty for T
where
	T: tg::handle::Pty,
{
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::pty::create::Output>> {
		self.create_pty(arg).boxed()
	}

	fn close_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.close_pty(id, arg).boxed()
	}

	fn delete_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.delete_pty(id, arg).boxed()
	}

	fn get_pty_size<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::size::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::pty::Size>>> {
		self.get_pty_size(id, arg).boxed()
	}

	fn put_pty_size<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::size::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_pty_size(id, arg).boxed()
	}

	fn try_read_pty_stream<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::pty::Event>>>>> {
		tg::handle::Pty::try_read_pty_stream(self, id, arg)
			.map_ok(|opt| opt.map(futures::StreamExt::boxed))
			.boxed()
	}

	fn write_pty<'a>(
		&'a self,
		id: &'a tg::pty::Id,
		arg: tg::pty::write::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.write_pty(id, arg).boxed()
	}
}
