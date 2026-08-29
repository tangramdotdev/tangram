use {
	crate::lmdb::{Db, Index, Key},
	foundationdb_tuple as fdbt, heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn contains_ids(&self, ids: &[tg::Id]) -> tg::Result<Vec<bool>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::ContainsIds {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::ContainsIds(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn contains_ids_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::Id],
	) -> tg::Result<Vec<bool>> {
		ids.iter()
			.map(|id| {
				Self::try_resolve_id_with_transaction(db, subspace, transaction, id)
					.map(|id| id.is_some())
			})
			.collect()
	}

	pub async fn try_get_ids_for_specifiers(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<Vec<Option<tg::Id>>> {
		if specifiers.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetIdsForSpecifiers {
			specifiers: specifiers.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetIdsForSpecifiers(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_ids_for_specifiers_with_transaction(
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

	pub async fn try_get_specifiers_for_ids(
		&self,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<tg::Specifier>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetSpecifiersForIds {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetSpecifiersForIds(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn try_get_specifiers_for_ids_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<tg::Specifier>>> {
		ids.iter()
			.map(|id| match id.kind() {
				tg::id::Kind::Group => Self::try_get_group_with_transaction(
					db,
					subspace,
					transaction,
					&id.clone().try_into()?,
				)
				.map(|group| group.map(|group| group.specifier)),
				tg::id::Kind::Organization => Self::try_get_organization_with_transaction(
					db,
					subspace,
					transaction,
					&id.clone().try_into()?,
				)
				.map(|organization| organization.map(|organization| organization.specifier)),
				tg::id::Kind::Tag => Self::try_get_tag_with_transaction(
					db,
					subspace,
					transaction,
					&id.clone().try_into()?,
				)
				.map(|tag| tag.map(|tag| tag.specifier)),
				tg::id::Kind::User => Self::try_get_user_with_transaction(
					db,
					subspace,
					transaction,
					&id.clone().try_into()?,
				)
				.map(|user| user.map(|user| user.specifier)),
				_ => Ok(None),
			})
			.collect()
	}

	pub(crate) fn try_resolve_id_with_transaction(
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
