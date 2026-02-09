use {
	super::Handle,
	crate::prelude::*,
	futures::{Stream, future::BoxFuture, stream::BoxStream},
};

impl tg::handle::Pty for Handle {
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pty::create::Output>> {
		self.0.create_pty(arg)
	}

	fn close_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.close_pty(id, arg)) }
	}

	fn delete_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.delete_pty(id, arg)) }
	}

	fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::pty::Size>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.get_pty_size(id, arg)) }
	}

	fn put_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.put_pty_size(id, arg)) }
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
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_read_pty_stream(id, arg),
			)
		}
	}

	fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.write_pty(id, arg)) }
	}
}
