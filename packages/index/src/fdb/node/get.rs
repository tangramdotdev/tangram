use {
	crate::fdb::{Index, Key},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
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
		let txn = self
			.database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"))?;
		match resource {
			tg::grant::Resource::Id(id) => {
				Self::try_resolve_id_with_transaction(&txn, &self.subspace, id).await
			},
			tg::grant::Resource::Specifier(specifier) => {
				// Resolve the deepest existing prefix of the specifier.
				let mut prefixes = specifier.prefixes().collect::<Vec<_>>();
				prefixes.reverse();
				for prefix in &prefixes {
					let id =
						Self::try_get_node_with_transaction(&txn, &self.subspace, prefix).await?;
					if let Some(id) = id {
						return Ok(Some(id));
					}
				}
				Ok(None)
			},
		}
	}

	async fn try_resolve_id_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Id,
	) -> tg::Result<Option<tg::Id>> {
		let key = match id.kind {
			tg::id::Kind::User => Key::User(crate::fdb::user::Key::User(id.clone().try_into()?)),
			tg::id::Kind::Group => {
				Key::Group(crate::fdb::group::Key::Group(id.clone().try_into()?))
			},
			tg::id::Kind::Organization => Key::Organization(
				crate::fdb::organization::Key::Organization(id.clone().try_into()?),
			),
			tg::id::Kind::Tag => Key::Tag(crate::fdb::tag::Key::Tag(id.clone().try_into()?)),
			_ => return Ok(None),
		};
		let key = Self::pack(subspace, &key);
		let value = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the node"))?;
		Ok(value.map(|_| id.clone()))
	}

	pub(crate) async fn try_get_node_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		specifier: &tg::Specifier,
	) -> tg::Result<Option<tg::Id>> {
		let key = Key::Node(crate::fdb::node::Key::Node(specifier.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %specifier, "failed to get the node"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		let id = tg::Id::from_slice(&bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the node id"))?;
		Ok(Some(id))
	}
}
