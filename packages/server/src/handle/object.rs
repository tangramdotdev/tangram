use {
	super::ServerWithContext,
	crate::{Context, Server, Shared},
	tangram_client::prelude::*,
};

impl tg::handle::Object for Shared {
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		self.0.try_get_object_metadata(id, arg).await
	}

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		self.0.try_get_object(id, arg).await
	}

	async fn put_object(&self, id: &tg::object::Id, arg: tg::object::put::Arg) -> tg::Result<()> {
		self.0.put_object(id, arg).await
	}

	async fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<()> {
		self.0.touch_object(id, arg).await
	}

	async fn post_object_batch(&self, arg: tg::object::batch::Arg) -> tg::Result<()> {
		self.0.post_object_batch(arg).await
	}
}

impl tg::handle::Object for Server {
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		self.try_get_object_metadata_with_context(&Context::default(), id, arg)
			.await
	}

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		self.try_get_object_with_context(&Context::default(), id, arg)
			.await
	}

	async fn put_object(&self, id: &tg::object::Id, arg: tg::object::put::Arg) -> tg::Result<()> {
		self.put_object_with_context(&Context::default(), id, arg)
			.await
	}

	async fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<()> {
		self.touch_object_with_context(&Context::default(), id, arg)
			.await
	}

	async fn post_object_batch(&self, arg: tg::object::batch::Arg) -> tg::Result<()> {
		self.post_object_batch_with_context(&Context::default(), arg)
			.await
	}
}

impl tg::handle::Object for ServerWithContext {
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		self.0
			.try_get_object_metadata_with_context(&self.1, id, arg)
			.await
	}

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		self.0.try_get_object_with_context(&self.1, id, arg).await
	}

	async fn put_object(&self, id: &tg::object::Id, arg: tg::object::put::Arg) -> tg::Result<()> {
		self.0.put_object_with_context(&self.1, id, arg).await
	}

	async fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<()> {
		self.0.touch_object_with_context(&self.1, id, arg).await
	}

	async fn post_object_batch(&self, arg: tg::object::batch::Arg) -> tg::Result<()> {
		self.0.post_object_batch_with_context(&self.1, arg).await
	}
}
