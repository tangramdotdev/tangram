use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_nodes(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<Vec<Option<tg::Id>>> {
		if specifiers.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetNodes {
			specifiers: specifiers.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetNodes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_nodes_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		specifiers: &[tg::Specifier],
	) -> tg::Result<Vec<Option<tg::Id>>> {
		specifiers
			.iter()
			.map(|specifier| {
				Self::try_get_node_with_transaction(db, subspace, transaction, specifier)
			})
			.collect()
	}

	pub(crate) fn try_resolve_resource_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		resource: &tg::grant::Resource,
	) -> tg::Result<Option<(tg::Id, bool)>> {
		match resource {
			tg::grant::Resource::Id(id) => {
				Self::try_resolve_id_with_transaction(db, subspace, transaction, id)
					.map(|id| id.map(|id| (id, true)))
			},
			tg::grant::Resource::Specifier(specifier) => {
				// Resolve the deepest existing prefix of the specifier.
				let mut prefixes = specifier.prefixes().collect::<Vec<_>>();
				prefixes.reverse();
				for prefix in &prefixes {
					let id =
						Self::try_get_node_with_transaction(db, subspace, transaction, prefix)?;
					if let Some(id) = id {
						let exact = prefix == specifier;
						return Ok(Some((id, exact)));
					}
				}
				Ok(None)
			},
		}
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
			tg::id::Kind::Process => {
				Key::Process(crate::lmdb::process::Key::Process(id.clone().try_into()?))
			},
			tg::id::Kind::Sandbox => {
				Key::Sandbox(crate::lmdb::sandbox::Key::Sandbox(id.clone().try_into()?))
			},
			_ => {
				let Ok(object) = tg::object::Id::try_from(id.clone()) else {
					return Ok(None);
				};
				Key::Object(crate::lmdb::object::Key::Object(object))
			},
		};
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, %id, "failed to get the node"))?;
		Ok(value.map(|_| id.clone()))
	}

	pub(crate) fn ancestor_ids_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		id: &tg::Id,
	) -> tg::Result<Vec<tg::Id>> {
		let mut ids = Vec::new();
		let mut current = Some(id.clone());
		while let Some(id) = current {
			match id.kind {
				tg::id::Kind::Tag => {
					let Some(tag) = Self::try_get_tag_with_transaction(
						db,
						subspace,
						transaction,
						&id.clone().try_into()?,
					)?
					else {
						break;
					};
					ids.push(id);
					current = tag.parent;
				},
				tg::id::Kind::Group => {
					let Some(group) = Self::try_get_group_with_transaction(
						db,
						subspace,
						transaction,
						&id.clone().try_into()?,
					)?
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
