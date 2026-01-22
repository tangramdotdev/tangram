use {
	super::{Index, Key, ObjectCoreField, ObjectField, ProcessCoreField, ProcessField},
	crate::{Object, Process},
	foundationdb as fdb,
	futures::future,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let this = self.clone();
		self.database
			.run(|txn, _| {
				let this = this.clone();
				let ids = ids.to_vec();
				async move {
					let txn = &txn;
					let results: tg::Result<Vec<Option<Object>>> =
						future::try_join_all(ids.iter().map(|id| {
							let this = this.clone();
							async move {
								let Some(mut object) =
									this.try_get_object_with_transaction(txn, id).await?
								else {
									return Ok(None);
								};
								let key = this.pack(&Key::Object {
									id: id.clone(),
									field: ObjectField::Core(ObjectCoreField::TouchedAt),
								});
								txn.atomic_op(
									&key,
									&touched_at.to_le_bytes(),
									fdb::options::MutationType::Max,
								);
								object.touched_at = touched_at;
								Ok::<_, tg::Error>(Some(object))
							}
						}))
						.await;
					Ok::<_, fdb::FdbBindingError>(results)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to touch objects"))?
	}

	pub async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let this = self.clone();
		self.database
			.run(|txn, _| {
				let this = this.clone();
				let ids = ids.to_vec();
				async move {
					let txn = &txn;
					let results: tg::Result<Vec<Option<Process>>> =
						future::try_join_all(ids.iter().map(|id| {
							let this = this.clone();
							async move {
								let Some(mut process) =
									this.try_get_process_with_transaction(txn, id).await?
								else {
									return Ok(None);
								};
								let key = this.pack(&Key::Process {
									id: id.clone(),
									field: ProcessField::Core(ProcessCoreField::TouchedAt),
								});
								txn.atomic_op(
									&key,
									&touched_at.to_le_bytes(),
									fdb::options::MutationType::Max,
								);
								process.touched_at = touched_at;
								Ok::<_, tg::Error>(Some(process))
							}
						}))
						.await;
					Ok::<_, fdb::FdbBindingError>(results)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to touch processes"))?
	}
}
