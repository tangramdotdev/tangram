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
		self.database
			.run(|txn, _| {
				let subspace = self.subspace.clone();
				let ids = ids.to_vec();
				async move {
					let txn = &txn;
					future::try_join_all(ids.iter().map(|id| {
						let subspace = subspace.clone();
						async move {
							// Attempt to get the object.
							let Some(mut object) =
								Self::try_get_object_with_transaction(txn, &subspace, id).await?
							else {
								return Ok(None);
							};

							// Add a conflict range on the exists key.
							let key = Key::Object {
								id: id.clone(),
								field: ObjectField::Core(ObjectCoreField::Exists),
							};
							let exists_key = Self::pack(&subspace, &key);
							let mut exists_key_end = exists_key.clone();
							exists_key_end.push(0x00);
							txn.add_conflict_range(
								&exists_key,
								&exists_key_end,
								fdb::options::ConflictRangeType::Read,
							)
							.map_err(|source| {
								tg::error!(!source, "failed to add read conflict range")
							})?;

							// Update the touched at.
							let key = Key::Object {
								id: id.clone(),
								field: ObjectField::Core(ObjectCoreField::TouchedAt),
							};
							let key = Self::pack(&subspace, &key);
							txn.atomic_op(
								&key,
								&touched_at.to_le_bytes(),
								fdb::options::MutationType::Max,
							);

							object.touched_at = touched_at;

							Ok::<_, tg::Error>(Some(object))
						}
					}))
					.await
					.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to touch objects"))
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
				let subspace = self.subspace.clone();
				let ids = ids.to_vec();
				async move {
					let txn = &txn;
					future::try_join_all(ids.iter().map(|id| {
						let subspace = subspace.clone();
						async move {
							// Attempt to get the process.
							let Some(mut process) =
								Self::try_get_process_with_transaction(txn, &subspace, id).await?
							else {
								return Ok(None);
							};

							// Add a conflict range on the exists key.
							let key = Key::Process {
								id: id.clone(),
								field: ProcessField::Core(ProcessCoreField::Exists),
							};
							let exists_key = Self::pack(&subspace, &key);
							let mut exists_key_end = exists_key.clone();
							exists_key_end.push(0x00);
							txn.add_conflict_range(
								&exists_key,
								&exists_key_end,
								fdb::options::ConflictRangeType::Read,
							)
							.map_err(|source| {
								tg::error!(!source, "failed to add read conflict range")
							})?;

							// Update the touched at.
							let key = Key::Process {
								id: id.clone(),
								field: ProcessField::Core(ProcessCoreField::TouchedAt),
							};
							let key = Self::pack(&subspace, &key);
							txn.atomic_op(
								&key,
								&touched_at.to_le_bytes(),
								fdb::options::MutationType::Max,
							);

							process.touched_at = touched_at;

							Ok::<_, tg::Error>(Some(process))
						}
					}))
					.await
					.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to touch processes"))
	}
}
