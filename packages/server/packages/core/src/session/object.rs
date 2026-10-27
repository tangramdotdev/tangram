use {crate::Session, tangram_client::prelude::*};

impl tg::instance::Object for Session {
	async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		self.server
			.components
			.object
			.try_get_object_metadata(self, id, arg)
			.await
	}

	async fn try_get_object_availability(
		&self,
		id: &tg::object::Id,
		arg: tg::object::availability::Arg,
	) -> tg::Result<Option<tg::object::Availability>> {
		self.server
			.components
			.object
			.try_get_object_availability(self, id, arg)
			.await
	}

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> tg::Result<Option<tg::object::get::Output>> {
		self.server
			.components
			.object
			.try_get_object(self, id, arg)
			.await
	}

	async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<tg::object::put::Output> {
		self.server
			.components
			.object
			.put_object(self, id, arg)
			.await
	}

	async fn try_touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.object
			.try_touch_object(self, id, arg)
			.await
	}

	async fn post_object_batch(
		&self,
		arg: tg::object::batch::Arg,
	) -> tg::Result<tg::object::batch::Output> {
		self.server
			.components
			.object
			.post_object_batch(self, arg)
			.await
	}
}
