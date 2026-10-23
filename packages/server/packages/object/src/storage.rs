use {
	crate::Component, tangram_client::prelude::*, tangram_index::prelude::*,
	tangram_server_core::Server,
};

impl Component {
	pub(crate) async fn try_get_object_storage_local(
		&self,
		server: &Server,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::storage::Set>> {
		Ok(server
			.index
			.try_get_object(id)
			.await?
			.map(|object| object.storage))
	}
}
