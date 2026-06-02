use {crate::Session, tangram_client::prelude::*};

impl tg::handle::Object for Session {
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		// self.try_get_object_metadata(id, arg).await
		Err(tg::error!("todo"))
	}

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		// self.try_get_object(id, arg).await
		Err(tg::error!("todo"))
	}

	async fn put_object(&self, id: &tg::object::Id, arg: tg::object::put::Arg) -> tg::Result<()> {
		// self.put_object(id, arg).await
		Err(tg::error!("todo"))
	}

	async fn try_touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<Option<()>> {
		// self.try_touch_object(id, arg).await
		Err(tg::error!("todo"))
	}

	async fn post_object_batch(&self, arg: tg::object::batch::Arg) -> tg::Result<()> {
		// self.post_object_batch(arg).await
		Err(tg::error!("todo"))
	}
}
