use {crate::Server, tangram_client::prelude::*};

impl tg::handle::Object for Server {
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

	async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<tg::object::put::Output> {
		self.session(&self.context).put_object(id, arg).await
	}

	async fn try_touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context).try_touch_object(id, arg).await
	}

	async fn post_object_batch(
		&self,
		arg: tg::object::batch::Arg,
	) -> tg::Result<tg::object::batch::Output> {
		self.session(&self.context).post_object_batch(arg).await
	}
}
