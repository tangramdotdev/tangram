use {
	super::Index,
	crate::{ObjectStored, ProcessStored},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_touch_object_and_get_stored_and_metadata(
		&self,
		_id: &tg::object::Id,
		_touched_at: i64,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		todo!()
	}

	pub async fn try_touch_object_and_get_stored_and_metadata_batch(
		&self,
		_ids: &[tg::object::Id],
		_touched_at: i64,
	) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
		todo!()
	}

	pub async fn try_touch_process_and_get_stored_and_metadata(
		&self,
		_id: &tg::process::Id,
		_touched_at: i64,
	) -> tg::Result<Option<(ProcessStored, tg::process::Metadata)>> {
		todo!()
	}

	pub async fn try_touch_process_and_get_stored_and_metadata_batch(
		&self,
		_ids: &[tg::process::Id],
		_touched_at: i64,
	) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
		todo!()
	}

	pub async fn touch_object(&self, _id: &tg::object::Id) -> tg::Result<()> {
		todo!()
	}

	pub async fn touch_process(&self, _id: &tg::process::Id) -> tg::Result<()> {
		todo!()
	}
}
