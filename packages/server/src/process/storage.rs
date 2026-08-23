use {crate::Server, tangram_client::prelude::*, tangram_index::prelude::*};

impl Server {
	pub(crate) async fn try_get_process_storage_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tangram_index::process::Storage>> {
		Ok(self
			.index
			.try_get_process(id)
			.await?
			.map(|process| process.storage))
	}
}
