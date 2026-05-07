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
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		self.session(&self.context)
			.try_get_object_metadata(id, arg)
			.await
	}

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		self.session(&self.context).try_get_object(id, arg).await
	}

	async fn put_object(&self, id: &tg::object::Id, arg: tg::object::put::Arg) -> tg::Result<()> {
		self.session(&self.context).put_object(id, arg).await
	}

	async fn post_object_batch(&self, arg: tg::object::batch::Arg) -> tg::Result<()> {
		self.session(&self.context).post_object_batch(arg).await
	}

	async fn try_touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context).try_touch_object(id, arg).await
	}
}
