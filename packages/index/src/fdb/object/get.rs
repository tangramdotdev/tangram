use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<crate::object::Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetObjects {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetObjects(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_objects_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::object::Id],
	) -> tg::Result<ControlFlow<Vec<Option<crate::object::Object>>, fdb::FdbError>> {
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
	) -> tg::Result<ControlFlow<Option<crate::object::Object>, fdb::FdbError>> {
		let key = Key::Object(crate::fdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let bytes = crate::fdb::retry!(result);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let object = Some(crate::object::Object::deserialize(&bytes)?);

		Ok(ControlFlow::Break(object))
	}

	pub(crate) async fn get_object_children_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<ControlFlow<Vec<tg::object::Id>, fdb::FdbError>> {
		let bytes = id.to_bytes();
		let key = (Kind::ObjectChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);

		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Object(crate::fdb::object::Key::ObjectChild { child, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(children))
	}

	pub(crate) async fn get_object_parents_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<ControlFlow<Vec<tg::object::Id>, fdb::FdbError>> {
		let bytes = id.to_bytes();
		let key = (Kind::ChildObject.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);

		let parents = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Object(crate::fdb::object::Key::ChildObject { object, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(object)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(parents))
	}

	pub(crate) async fn get_object_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<ControlFlow<Vec<(tg::process::Id, crate::process::object::Kind)>, fdb::FdbError>>
	{
		let bytes = id.to_bytes();
		let key = (Kind::ObjectProcess.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);

		let processes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::Object(crate::fdb::object::Key::ObjectProcess { kind, process, .. }) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((process, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break(processes))
	}
}
