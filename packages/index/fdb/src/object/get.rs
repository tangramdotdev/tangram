use {
	crate::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn try_get_objects_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::object::Id],
	) -> tg::Result<ControlFlow<Vec<Option<tangram_index::object::Object>>, fdb::FdbError>> {
		let objects = {
			let result = futures::future::try_join_all(
				ids.iter()
					.map(|id| Self::try_get_object_with_transaction(txn, subspace, id)),
			)
			.await;
			let results = result?;
			let mut values = Vec::with_capacity(results.len());
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};

		Ok(ControlFlow::Break(objects))
	}

	pub(crate) async fn try_get_object_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<ControlFlow<Option<tangram_index::object::Object>, fdb::FdbError>> {
		let key = Key::Object(crate::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let object = Some(tangram_index::object::Object::deserialize(&bytes)?);

		Ok(ControlFlow::Break(object))
	}

	pub(crate) async fn get_object_children_page_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<ControlFlow<crate::page::Page<tg::object::Id>, fdb::FdbError>> {
		let bytes = id.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::ObjectChild.to_i32().unwrap(), bytes.as_ref()),
		);
		let (entries, cursor) = crate::propagate!(
			Self::get_entry_page_with_transaction(txn, subspace, &prefix, after, limit).await
		);
		let items = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Object(crate::object::Key::ObjectChild { child, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break((items, cursor)))
	}

	pub(crate) async fn get_object_parents_page_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<ControlFlow<crate::page::Page<tg::object::Id>, fdb::FdbError>> {
		let bytes = id.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::ChildObject.to_i32().unwrap(), bytes.as_ref()),
		);
		let (entries, cursor) = crate::propagate!(
			Self::get_entry_page_with_transaction(txn, subspace, &prefix, after, limit).await
		);
		let items = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Object(crate::object::Key::ChildObject { object, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(object)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break((items, cursor)))
	}

	pub(crate) async fn get_object_processes_page_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<
		ControlFlow<
			crate::page::Page<(tg::process::Id, tangram_index::process::object::Kind)>,
			fdb::FdbError,
		>,
	> {
		let bytes = id.to_bytes();
		let prefix = Self::pack(
			subspace,
			&(Kind::ObjectProcess.to_i32().unwrap(), bytes.as_ref()),
		);
		let (entries, cursor) = crate::propagate!(
			Self::get_entry_page_with_transaction(txn, subspace, &prefix, after, limit).await
		);
		let items = entries
			.into_iter()
			.map(|(key, _)| {
				let Key::Object(crate::object::Key::ObjectProcess { kind, process, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((process, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break((items, cursor)))
	}
}
