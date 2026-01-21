use {
	super::{Index, Key, ObjectCoreField, ObjectField, ProcessCoreField, ProcessField},
	crate::{Object, Process},
	foundationdb as fdb,
	foundationdb_tuple::TuplePack as _,
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
		self.database
			.run(|txn, _| {
				let ids = ids.to_vec();
				async move {
					let results: tg::Result<Vec<Option<Object>>> =
						future::try_join_all(ids.iter().map(|id| async {
							let Some(mut object) =
								Self::try_get_object_with_transaction(&txn, id).await?
							else {
								return Ok(None);
							};
							let key = Key::Object {
								id: id.clone(),
								field: ObjectField::Core(ObjectCoreField::TouchedAt),
							}
							.pack_to_vec();
							txn.atomic_op(
								&key,
								&touched_at.to_le_bytes(),
								fdb::options::MutationType::Max,
							);
							object.touched_at = touched_at;
							Ok::<_, tg::Error>(Some(object))
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
		self.database
			.run(|txn, _| {
				let ids = ids.to_vec();
				async move {
					let results: tg::Result<Vec<Option<Process>>> =
						future::try_join_all(ids.iter().map(|id| async {
							let Some(mut process) =
								Self::try_get_process_with_transaction(&txn, id).await?
							else {
								return Ok(None);
							};
							let key = Key::Process {
								id: id.clone(),
								field: ProcessField::Core(ProcessCoreField::TouchedAt),
							}
							.pack_to_vec();
							txn.atomic_op(
								&key,
								&touched_at.to_le_bytes(),
								fdb::options::MutationType::Max,
							);
							process.touched_at = touched_at;
							Ok::<_, tg::Error>(Some(process))
						}))
						.await;
					Ok::<_, fdb::FdbBindingError>(results)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to touch processes"))?
	}
}
