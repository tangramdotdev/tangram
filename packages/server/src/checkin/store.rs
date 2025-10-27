use {
	crate::{Server, checkin::state::Objects},
	tangram_client as tg,
	tangram_store::prelude::*,
};

impl Server {
	pub(super) async fn checkin_store(&self, objects: &Objects, touched_at: i64) -> tg::Result<()> {
		let args = objects
			.values()
			.map(|object| crate::store::PutArg {
				bytes: object.bytes.clone(),
				cache_reference: object.cache_reference.clone(),
				id: object.id.clone(),
				touched_at,
			})
			.collect();
		self.store
			.put_batch(args)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		Ok(())
	}
}
