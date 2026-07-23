use {
	crate::fdb::{Index, Key, Kind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
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
	) -> tg::Result<Vec<Option<crate::object::Object>>> {
		futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_object_with_transaction(txn, subspace, id)),
		)
		.await
	}

	pub(crate) async fn try_get_object_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Option<crate::object::Object>> {
		let key = Key::Object(crate::fdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(crate::object::Object::deserialize(&bytes)?))
	}

	pub(crate) async fn get_object_children_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let bytes = id.to_bytes();
		let key = (Kind::ObjectChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get object children"))?;

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

		Ok(children)
	}

	pub(crate) async fn get_object_parents_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let bytes = id.to_bytes();
		let key = (Kind::ChildObject.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get object parents"))?;

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

		Ok(parents)
	}

	pub(crate) async fn get_object_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, crate::process::object::Kind)>> {
		let bytes = id.to_bytes();
		let key = (Kind::ObjectProcess.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get object process parents"))?;

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

		Ok(processes)
	}
}
