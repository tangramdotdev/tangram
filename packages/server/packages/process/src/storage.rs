use {
	crate::Component, tangram_client::prelude::*, tangram_index::prelude::*,
	tangram_server_core::Server,
};

impl Component {
	pub(crate) async fn try_get_process_storage_local(
		&self,
		server: &Server,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::storage::Set>> {
		Ok(server
			.index
			.try_get_process(id)
			.await?
			.map(|process| process.storage))
	}
}
