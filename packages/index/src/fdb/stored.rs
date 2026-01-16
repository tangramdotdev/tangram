use {
	super::Index,
	crate::{ObjectStored, ProcessStored},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_object_stored(
		&self,
		_id: &tg::object::Id,
	) -> tg::Result<Option<ObjectStored>> {
		todo!()
	}

	pub async fn try_get_object_stored_batch(
		&self,
		_ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<ObjectStored>>> {
		todo!()
	}

	pub async fn try_get_object_stored_and_metadata(
		&self,
		_id: &tg::object::Id,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		todo!()
	}

	pub async fn try_get_object_stored_and_metadata_batch(
		&self,
		_ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
		todo!()
	}

	pub async fn try_get_process_stored(
		&self,
		_id: &tg::process::Id,
	) -> tg::Result<Option<ProcessStored>> {
		todo!()
	}

	pub async fn try_get_process_stored_batch(
		&self,
		_ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<ProcessStored>>> {
		todo!()
	}

	pub async fn try_get_process_stored_and_metadata_batch(
		&self,
		_ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
		todo!()
	}
}
