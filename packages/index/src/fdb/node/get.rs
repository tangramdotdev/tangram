use {
	crate::fdb::{Index, Key},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	std::ops::ControlFlow,
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

	pub(crate) async fn contains_ids_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::Id],
	) -> tg::Result<ControlFlow<Vec<bool>, fdb::FdbError>> {
		let ids = {
			let result = futures::future::try_join_all(
				ids.iter()
					.map(|id| Self::try_resolve_id_with_transaction(txn, subspace, id)),
			)
			.await;
			let results = result?;
			let mut values = Vec::new();
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};
		let output = ids.into_iter().map(|id| id.is_some()).collect();

		Ok(ControlFlow::Break(output))
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

	pub(crate) async fn try_get_ids_for_specifiers_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		specifiers: &[tg::Specifier],
	) -> tg::Result<ControlFlow<Vec<Option<tg::Id>>, fdb::FdbError>> {
		let ids = {
			let result =
				futures::future::try_join_all(specifiers.iter().map(|specifier| {
					Self::try_get_node_with_transaction(txn, subspace, specifier)
				}))
				.await;
			let results = result?;
			let mut values = Vec::new();
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};

		Ok(ControlFlow::Break(ids))
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

	pub(crate) async fn try_get_specifiers_for_ids_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::Id],
	) -> tg::Result<ControlFlow<Vec<Option<tg::Specifier>>, fdb::FdbError>> {
		let specifiers = {
			let result = futures::future::try_join_all(ids.iter().map(|id| async move {
				let specifier = match id.kind() {
					tg::id::Kind::Group => crate::fdb::propagate!(
						Self::try_get_group_with_transaction(
							txn,
							subspace,
							&id.clone().try_into()?,
						)
						.await
					)
					.map(|group| group.specifier),
					tg::id::Kind::Organization => crate::fdb::propagate!(
						Self::try_get_organization_with_transaction(
							txn,
							subspace,
							&id.clone().try_into()?,
						)
						.await
					)
					.map(|organization| organization.specifier),
					tg::id::Kind::Tag => crate::fdb::propagate!(
						Self::try_get_tag_with_transaction(txn, subspace, &id.clone().try_into()?,)
							.await
					)
					.map(|tag| tag.specifier),
					tg::id::Kind::User => crate::fdb::propagate!(
						Self::try_get_user_with_transaction(
							txn,
							subspace,
							&id.clone().try_into()?,
						)
						.await
					)
					.map(|user| user.specifier),
					_ => None,
				};

				Ok::<_, tg::Error>(ControlFlow::Break(specifier))
			}))
			.await;
			let results = result?;
			let mut values = Vec::new();
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};

		Ok(ControlFlow::Break(specifiers))
	}

	pub(crate) async fn try_resolve_resource_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		resource: &tg::Selector<tg::Id>,
	) -> tg::Result<ControlFlow<Option<(tg::Id, bool)>, fdb::FdbError>> {
		let output = match resource {
			tg::Selector::Id(id) => crate::fdb::propagate!(
				Self::try_resolve_id_with_transaction(txn, subspace, id).await
			)
			.map(|id| (id, true)),
			tg::Selector::Specifier(specifier) => {
				// Resolve the deepest existing prefix of the specifier.
				let mut prefixes = specifier.prefixes().collect::<Vec<_>>();
				prefixes.reverse();
				for prefix in &prefixes {
					let id = crate::fdb::propagate!(
						Self::try_get_node_with_transaction(txn, subspace, prefix).await
					);
					if let Some(id) = id {
						let exact = prefix == specifier;
						return Ok(ControlFlow::Break(Some((id, exact))));
					}
				}
				None
			},
		};

		Ok(ControlFlow::Break(output))
	}

	async fn try_resolve_id_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Id,
	) -> tg::Result<ControlFlow<Option<tg::Id>, fdb::FdbError>> {
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
					return Ok(ControlFlow::Break(None));
				};
				Key::Object(crate::fdb::object::Key::Object(object))
			},
		};
		let key = Self::pack(subspace, &key);
		let value = crate::fdb::retry!(txn.get(&key, false).await);
		let id = value.map(|_| id.clone());

		Ok(ControlFlow::Break(id))
	}

	pub(crate) async fn ancestor_ids_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::Id,
	) -> tg::Result<ControlFlow<Vec<tg::Id>, fdb::FdbError>> {
		let mut ids = Vec::new();
		let mut current = Some(id.clone());
		while let Some(id) = current {
			match id.kind {
				tg::id::Kind::Tag => {
					let Some(tag) = crate::fdb::propagate!(
						Self::try_get_tag_with_transaction(txn, subspace, &id.clone().try_into()?,)
							.await
					) else {
						break;
					};
					ids.push(id);
					current = tag.parent;
				},
				tg::id::Kind::Group => {
					let Some(group) = crate::fdb::propagate!(
						Self::try_get_group_with_transaction(
							txn,
							subspace,
							&id.clone().try_into()?,
						)
						.await
					) else {
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
		Ok(ControlFlow::Break(ids))
	}

	pub(crate) async fn try_get_node_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		specifier: &tg::Specifier,
	) -> tg::Result<ControlFlow<Option<tg::Id>, fdb::FdbError>> {
		let key = Key::Node(crate::fdb::node::Key::Node(specifier.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = crate::fdb::retry!(txn.get(&key, false).await);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let id = tg::Id::from_slice(&bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the node id"))?;
		Ok(ControlFlow::Break(Some(id)))
	}
}
