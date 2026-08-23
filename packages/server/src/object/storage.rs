use {crate::Server, tangram_client::prelude::*, tangram_index::prelude::*};

impl Server {
	pub(crate) async fn try_get_object_storage_local(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tangram_index::object::Storage>> {
		Ok(self
			.index
			.try_get_object(id)
			.await?
			.map(|object| object.storage))
	}
}
