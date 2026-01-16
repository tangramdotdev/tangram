use {super::Index, tangram_client::prelude::*};

impl Index {
	pub async fn try_get_object_metadata(
		&self,
		_id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		todo!()
	}

	pub async fn try_get_object_metadata_batch(
		&self,
		_ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
		todo!()
	}

	pub async fn try_get_process_metadata(
		&self,
		_id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		todo!()
	}

	pub async fn try_get_process_metadata_batch(
		&self,
		_ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::Metadata>>> {
		todo!()
	}
}
