use {
	crate::fdb::{Index, Key},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn try_resolve_resource_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::grant::Resource,
	) -> tg::Result<Option<tg::Id>> {
		match resource {
			tg::grant::Resource::Id(id) => {
				Self::try_resolve_id_with_transaction(txn, subspace, id).await
			},
			tg::grant::Resource::Specifier(specifier) => {
				// Resolve the deepest existing prefix of the specifier.
				let mut prefixes = specifier.prefixes().collect::<Vec<_>>();
				prefixes.reverse();
				for prefix in &prefixes {
					let id = Self::try_get_node_with_transaction(txn, subspace, prefix).await?;
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
			tg::id::Kind::Process => {
				Key::Process(crate::fdb::process::Key::Process(id.clone().try_into()?))
			},
			tg::id::Kind::Sandbox => {
				Key::Sandbox(crate::fdb::sandbox::Key::Sandbox(id.clone().try_into()?))
			},
			_ => {
				let Ok(object) = tg::object::Id::try_from(id.clone()) else {
					return Ok(None);
				};
				Key::Object(crate::fdb::object::Key::Object(object))
			},
		};
		let key = Self::pack(subspace, &key);
		let value = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the node"))?;
		Ok(value.map(|_| id.clone()))
	}

	pub(crate) async fn ancestor_ids_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Id,
	) -> tg::Result<Vec<tg::Id>> {
		let mut ids = Vec::new();
		let mut current = Some(id.clone());
		while let Some(id) = current {
			match id.kind {
				tg::id::Kind::Tag => {
					let Some(tag) =
						Self::try_get_tag_with_transaction(txn, subspace, &id.clone().try_into()?)
							.await?
					else {
						break;
					};
					ids.push(id);
					current = tag.parent;
				},
				tg::id::Kind::Group => {
					let Some(group) = Self::try_get_group_with_transaction(
						txn,
						subspace,
						&id.clone().try_into()?,
					)
					.await?
					else {
						break;
					};
					ids.push(id);
					current = group.parent;
				},
				tg::id::Kind::User | tg::id::Kind::Organization => {
					ids.push(id);
					current = None;
				},
				_ => break,
			}
		}
		Ok(ids)
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
