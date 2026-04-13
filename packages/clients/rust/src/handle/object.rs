use crate::prelude::*;

pub trait Object: Clone + Unpin + Send + Sync + 'static {
	fn get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<tg::object::Metadata>> + Send {
		let arg = tg::object::metadata::Arg::default();
		async move {
			self.try_get_object_metadata(id, arg)
				.await?
				.ok_or_else(|| tg::error!(%id, "failed to get the object metadata"))
		}
	}

	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> + Send;

	fn get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> impl Future<Output = tg::Result<tg::object::get::Output>> + Send {
		async move {
			self.try_get_object(id, arg)
				.await?
				.ok_or_else(|| tg::error!(%id, "failed to find the object"))
		}
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> + Send;

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn post_object_batch(
		&self,
		arg: tg::object::batch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_touch_object(id, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the object"))
		}
	}

	fn try_touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;
}

impl tg::handle::Object for tg::Client {
	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> {
		self.try_get_object_metadata(id, arg)
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> {
		self.try_get_object(id, arg)
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_object(id, arg)
	}

	fn post_object_batch(
		&self,
		arg: tg::object::batch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.post_object_batch(arg)
	}

	fn try_touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_touch_object(id, arg)
	}
}
