use crate::Server;
use tangram_client as tg;

impl Server {
	pub(crate) async fn try_get_object_complete(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<bool>> {
		let complete = self
			.index
			.try_get_object_complete_batch(&[id.clone()])
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to get object complete for id: {}", id)
			})?[0];

		Ok(complete)
	}

	pub(crate) async fn try_get_object_complete_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
		self.index.try_get_object_complete_batch(ids).await
	}
}
