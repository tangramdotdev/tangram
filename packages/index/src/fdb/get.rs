use {
	super::{Index, Key, KeyKind},
	crate::{Object, Process, ProcessObjectKind},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_objects(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;

		let outputs = futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_object_with_transaction(&txn, &self.subspace, id)),
		)
		.await?;

		Ok(outputs)
	}

	pub async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;

		let outputs = futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_process_with_transaction(&txn, &self.subspace, id)),
		)
		.await?;

		Ok(outputs)
	}

	pub(super) async fn try_get_object_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Option<Object>> {
		let key = Key::Object(id.clone());
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(Object::deserialize(&bytes)?))
	}

	pub(super) async fn try_get_process_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Option<Process>> {
		let key = Key::Process(id.clone());
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(Process::deserialize(&bytes)?))
	}

	pub(super) async fn get_object_children_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ObjectChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object children"))?;

		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ObjectChild { child, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(children)
	}

	pub(super) async fn get_object_parents_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Vec<tg::object::Id>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ChildObject.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object parents"))?;

		let parents = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ChildObject { object, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(object)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(parents)
	}

	pub(super) async fn get_object_processes_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, ProcessObjectKind)>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ObjectProcess.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object process parents"))?;

		let processes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ObjectProcess { kind, process, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((process, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(processes)
	}

	pub(super) async fn get_process_children_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ProcessChild.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process children"))?;

		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ProcessChild { child, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(children)
	}

	pub(super) async fn get_process_parents_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Vec<tg::process::Id>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ChildProcess.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process parents"))?;

		let parents = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ChildProcess { parent, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(parent)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(parents)
	}

	pub(super) async fn get_process_objects_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Vec<(tg::object::Id, ProcessObjectKind)>> {
		let bytes = id.to_bytes();
		let key = (KeyKind::ProcessObject.to_i32().unwrap(), bytes.as_ref());
		let prefix = Self::pack(subspace, &key);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};

		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process objects"))?;

		let objects = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let Key::ProcessObject { kind, object, .. } = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok((object, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(objects)
	}
}
