use {
	super::{Db, Key, Store},
	crate::{Grant, Object, TryGetArg, TryGetBatchArg, TryGetOutput},
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Store {
	pub(super) async fn try_get(&self, arg: TryGetArg) -> tg::Result<TryGetOutput> {
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let grant_ttl = self.grant_ttl;
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let object = Self::try_get_object_with_transaction(&db, &transaction, &arg.id)?;
				let grants = Self::try_get_grant_with_transaction(
					&db,
					&transaction,
					&arg.id,
					&arg.principal,
					arg.now,
					grant_ttl,
				)?;
				Ok(TryGetOutput { grants, object })
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub(super) async fn try_get_batch(&self, arg: TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		if arg.ids.is_empty() {
			return Ok(vec![]);
		}
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let grant_ttl = self.grant_ttl;
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(arg.ids.len());
				for id in &arg.ids {
					let object = Self::try_get_object_with_transaction(&db, &transaction, id)?;
					let grants = Self::try_get_grant_with_transaction(
						&db,
						&transaction,
						id,
						&arg.principal,
						arg.now,
						grant_ttl,
					)?;
					outputs.push(TryGetOutput { grants, object });
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub fn try_get_sync(&self, arg: &TryGetArg) -> tg::Result<TryGetOutput> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		self.try_get_with_transaction(&transaction, arg)
	}

	pub fn try_get_batch_sync(&self, arg: &TryGetBatchArg) -> tg::Result<Vec<TryGetOutput>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let mut outputs = Vec::with_capacity(arg.ids.len());
		for id in &arg.ids {
			let object = Self::try_get_object_with_transaction(&self.db, &transaction, id)?;
			let grants = Self::try_get_grant_with_transaction(
				&self.db,
				&transaction,
				id,
				&arg.principal,
				arg.now,
				self.grant_ttl,
			)?;
			outputs.push(TryGetOutput { grants, object });
		}
		Ok(outputs)
	}

	pub fn try_get_data_sync(
		&self,
		id: &tg::object::Id,
		principal: &tg::Principal,
		now: i64,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let transaction = self
			.env
			.read_txn()
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		self.try_get_data_with_transaction(&transaction, id, principal, now)
	}

	pub fn try_get_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		arg: &TryGetArg,
	) -> tg::Result<TryGetOutput> {
		let object = Self::try_get_object_with_transaction(&self.db, transaction, &arg.id)?;
		let grants = Self::try_get_grant_with_transaction(
			&self.db,
			transaction,
			&arg.id,
			&arg.principal,
			arg.now,
			self.grant_ttl,
		)?;
		Ok(TryGetOutput { grants, object })
	}

	fn try_get_object_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
	) -> tg::Result<Option<Object<'static>>> {
		let key = Key::Object(id);
		let key_bytes = key.pack_to_vec();
		let Some(bytes) = db
			.get(transaction, &key_bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
		else {
			return Ok(None);
		};
		let value = Object::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object"))?;
		Ok(Some(value))
	}

	fn try_get_grant_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		principal: &tg::Principal,
		now: i64,
		grant_ttl: u64,
	) -> tg::Result<Vec<Grant>> {
		if matches!(principal, tg::Principal::Root) {
			return Ok(Vec::new());
		}
		let principal = principal.to_string();
		let key = Key::ObjectGrant(id, &principal);
		let Some(bytes) = db
			.get(transaction, &key.pack_to_vec())
			.map_err(|error| tg::error!(!error, %id, "failed to get the object grant"))?
		else {
			return Ok(Vec::new());
		};
		let grant = Grant::deserialize(bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object grant"))?;
		Ok((now - grant.created_at < grant_ttl.to_i64().unwrap())
			.then_some(grant)
			.into_iter()
			.collect())
	}

	pub fn try_get_data_with_transaction(
		&self,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::object::Id,
		principal: &tg::Principal,
		now: i64,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let kind = id.kind();
		let arg = TryGetArg {
			id: id.clone(),
			now,
			principal: principal.clone(),
		};
		let output = self.try_get_with_transaction(transaction, &arg)?;
		if !matches!(principal, tg::Principal::Root) && output.grants.is_empty() {
			return Ok(None);
		}
		let Some(value) = output.object else {
			return Ok(None);
		};
		let Some(bytes) = value.bytes else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(kind, &*bytes)
			.map_err(|error| tg::error!(!error, %id, "failed to deserialize the object data"))?;
		Ok(Some((size, data)))
	}
}
