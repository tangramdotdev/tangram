use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn authorize(
		&self,
		resource: tg::grant::Resource,
		_permission: tg::grant::Permission,
		_principal: Option<&tg::Principal>,
	) -> tg::Result<Option<bool>> {
		let id = self.try_resolve_resource(&resource).await?;
		// The permission traversal is not implemented yet, so any resolved resource is authorized.
		Ok(id.map(|_| true))
	}

	async fn try_resolve_resource(
		&self,
		resource: &tg::grant::Resource,
	) -> tg::Result<Option<tg::Id>> {
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let subspace = self.subspace.clone();
			let resource = resource.clone();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				match &resource {
					tg::grant::Resource::Id(id) => {
						Self::try_resolve_id_with_transaction(&db, &subspace, &transaction, id)
					},
					tg::grant::Resource::Specifier(specifier) => {
						// Resolve the deepest existing prefix of the specifier.
						let mut prefixes = specifier.prefixes().collect::<Vec<_>>();
						prefixes.reverse();
						for prefix in &prefixes {
							let id = Self::try_get_node_with_transaction(
								&db,
								&subspace,
								&transaction,
								prefix,
							)?;
							if let Some(id) = id {
								return Ok(Some(id));
							}
						}
						Ok(None)
					},
				}
			}
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	fn try_resolve_id_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::Id,
	) -> tg::Result<Option<tg::Id>> {
		let key = match id.kind {
			tg::id::Kind::User => Key::User(crate::lmdb::user::Key::User(id.clone().try_into()?)),
			tg::id::Kind::Group => {
				Key::Group(crate::lmdb::group::Key::Group(id.clone().try_into()?))
			},
			tg::id::Kind::Organization => Key::Organization(
				crate::lmdb::organization::Key::Organization(id.clone().try_into()?),
			),
			tg::id::Kind::Tag => Key::Tag(crate::lmdb::tag::Key::Tag(id.clone().try_into()?)),
			_ => return Ok(None),
		};
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the node"))?;
		Ok(value.map(|_| id.clone()))
	}

	pub(crate) fn try_get_node_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<Option<tg::Id>> {
		let key = Key::Node(crate::lmdb::node::Key::Node(specifier.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %specifier, "failed to get the node"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		let id = tg::Id::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the node id"))?;
		Ok(Some(id))
	}
}
