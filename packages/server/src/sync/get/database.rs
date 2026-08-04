use {
	crate::{Session, database::Transaction, sync::graph::Graph},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(super) async fn sync_get_database(&self, graph: &Arc<Mutex<Graph>>) -> tg::Result<()> {
		// Get the staged items.
		let mut items = graph
			.lock()
			.unwrap()
			.local_messages()
			.into_iter()
			.filter(|item| !matches!(item, tg::sync::PutItemMessage::Sandbox(_)))
			.collect::<Vec<_>>();
		if items.is_empty() {
			return Ok(());
		}

		// Authorize the writes.
		self.sync_get_database_authorize(&items).await?;

		// Finalize the tag item permissions in the graph.
		self.sync_get_database_update_tag_item_permissions(graph, &items)
			.await?;
		let tag_permissions = self.sync_get_database_tag_permissions(graph, &items)?;
		let mut tag_owners = BTreeMap::new();
		for item in &items {
			let tg::sync::PutItemMessage::Tag(message) = item else {
				continue;
			};
			let owner = self.storage_owner_for_specifier(&message.specifier).await?;
			tag_owners.insert(message.id.clone(), owner);
		}
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Sort the items so that parents are written before their children.
		items.sort_by_key(Self::sync_get_database_item_depth);

		// Write all of the items and enqueue their index mutations atomically.
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let items = items.clone();
				let session = session.clone();
				let tag_owners = tag_owners.clone();
				let tag_permissions = tag_permissions.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					for item in &items {
						let created = session
							.sync_get_database_item_with_transaction(
								transaction,
								item,
								&tag_owners,
								&tag_permissions,
								&mut batch,
							)
							.await?;
						if created
							&& let Some(arg) = session.sync_get_create_temporary_grant(
								&Self::sync_get_database_item_id(item)?,
							)? {
							batch.items.push(tangram_index::batch::Item::PutGrant(arg));
						}
					}
					for item in &items {
						let tg::sync::PutItemMessage::Tag(message) = item else {
							continue;
						};
						let Some(owner) = tag_owners.get(&message.id).cloned().flatten() else {
							continue;
						};
						let item = if let Ok(object) = message.item.clone().try_into() {
							tangram_index::batch::Item::PutOwnerObject(
								tangram_index::storage::put::ObjectArg {
									object,
									owner,
									touched_at,
								},
							)
						} else if let Ok(process) = message.item.clone().try_into() {
							tangram_index::batch::Item::PutOwnerProcess(
								tangram_index::storage::put::ProcessArg {
									owner,
									process,
									touched_at,
								},
							)
						} else {
							return Err(tg::error!("invalid tag item").into());
						};
						batch.items.push(item);
					}
					session
						.server
						.enqueue_database_outbox_with_transaction(transaction, &batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await?;

		Ok(())
	}

	async fn sync_get_database_authorize(
		&self,
		items: &[tg::sync::PutItemMessage],
	) -> tg::Result<()> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		for item in items {
			let id = Self::sync_get_database_item_id(item)?;
			let specifier = Self::sync_get_database_item_specifier(item)?;
			let mut connection = self
				.server
				.database
				.connection()
				.await
				.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
			let transaction = connection
				.transaction()
				.await
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
			let by_id = Self::try_get_specifier_for_id_with_transaction(&transaction, &id).await?;
			let by_specifier =
				Self::try_get_id_for_specifier_with_transaction(&transaction, specifier).await?;
			Self::sync_get_database_validate_id_and_specifier(
				&id,
				specifier,
				by_id.as_ref(),
				by_specifier.as_ref(),
			)?;
			drop(transaction);
			let permission = Self::write_permission_for_resource(&id)?;
			let resource = tg::grant::Resource::Specifier(specifier.clone());
			let authorized = self.authorize(resource, permission).await?;
			if authorized.is_some_and(|permissions| !permissions.contains(permission)) {
				return Err(tg::error!("unauthorized"));
			}
		}

		Ok(())
	}

	async fn sync_get_database_update_tag_item_permissions(
		&self,
		graph: &Arc<Mutex<Graph>>,
		items: &[tg::sync::PutItemMessage],
	) -> tg::Result<()> {
		let mut objects = BTreeSet::new();
		let mut processes = BTreeSet::new();
		for item in items {
			let tg::sync::PutItemMessage::Tag(message) = item else {
				continue;
			};
			if let Ok(id) = tg::object::Id::try_from(message.item.clone()) {
				objects.insert(id);
			} else if let Ok(id) = tg::process::Id::try_from(message.item.clone()) {
				processes.insert(id);
			} else {
				return Err(tg::error!("invalid tag item"));
			}
		}

		let object_permissions = tg::grant::permission::Set::from_permission(
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Node),
		);
		self.sync_get_authorize(
			graph,
			objects.into_iter().map(tg::Id::from),
			object_permissions,
		)
		.await?;

		let mut process_permissions =
			tg::grant::permission::Set::Process(tg::grant::permission::process::Set::empty());
		for permission in [
			tg::grant::permission::process::Permission::Node,
			tg::grant::permission::process::Permission::NodeCommand,
			tg::grant::permission::process::Permission::NodeError,
			tg::grant::permission::process::Permission::NodeLog,
			tg::grant::permission::process::Permission::NodeOutput,
		] {
			process_permissions.insert(tg::grant::permission::Set::from_permission(
				tg::grant::Permission::Process(permission),
			));
		}
		self.sync_get_authorize(
			graph,
			processes.into_iter().map(tg::Id::from),
			process_permissions,
		)
		.await?;

		Ok(())
	}

	fn sync_get_database_tag_permissions(
		&self,
		graph: &Arc<Mutex<Graph>>,
		items: &[tg::sync::PutItemMessage],
	) -> tg::Result<BTreeMap<tg::tag::Id, Vec<tg::grant::Permission>>> {
		let mut outputs = BTreeMap::new();
		let mut graph = graph.lock().unwrap();
		for item in items {
			let tg::sync::PutItemMessage::Tag(message) = item else {
				continue;
			};
			let (aspects, permissions) =
				if let Ok(id) = tg::object::Id::try_from(message.item.clone()) {
					let aspects = vec![tg::grant::Permission::Object(
						tg::grant::permission::object::Permission::Node,
					)];
					let required = tg::grant::permission::Set::from_permission(aspects[0]);
					let authorization = graph.get_object_local_authorization(&id, required);
					(aspects, authorization.permissions)
				} else if let Ok(id) = tg::process::Id::try_from(message.item.clone()) {
					let aspects = [
						tg::grant::permission::process::Permission::Node,
						tg::grant::permission::process::Permission::NodeCommand,
						tg::grant::permission::process::Permission::NodeError,
						tg::grant::permission::process::Permission::NodeLog,
						tg::grant::permission::process::Permission::NodeOutput,
					]
					.into_iter()
					.map(tg::grant::Permission::Process)
					.collect::<Vec<_>>();
					let mut required = tg::grant::permission::Set::Process(
						tg::grant::permission::process::Set::empty(),
					);
					for aspect in &aspects {
						required.insert(tg::grant::permission::Set::from_permission(*aspect));
					}
					let authorization = graph.get_process_local_authorization(&id, required);
					(aspects, authorization.permissions)
				} else {
					return Err(tg::error!("invalid tag item"));
				};
			let permissions = if matches!(self.context.principal, tg::Principal::Root) {
				aspects
					.into_iter()
					.map(tg::grant::Permission::subtree)
					.collect()
			} else {
				aspects
					.into_iter()
					.filter_map(|aspect| {
						[aspect.subtree(), aspect]
							.into_iter()
							.find(|permission| permissions.contains(*permission))
					})
					.collect()
			};
			outputs.insert(message.id.clone(), permissions);
		}

		Ok(outputs)
	}

	async fn sync_get_database_item_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		item: &tg::sync::PutItemMessage,
		tag_owners: &BTreeMap<tg::tag::Id, Option<tangram_index::storage::Owner>>,
		tag_permissions: &BTreeMap<tg::tag::Id, Vec<tg::grant::Permission>>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<bool> {
		match item {
			tg::sync::PutItemMessage::Group(message) => {
				let created = Self::sync_get_database_validate_item_with_transaction(
					transaction,
					&message.id.clone().into(),
					&message.name,
					message.parent.as_ref(),
					&message.specifier,
				)
				.await?;
				let p = transaction.p();
				let statement = formatdoc!(
					"
						insert into groups (id, name, parent)
						values ({p}1, {p}2, {p}3)
						on conflict (id) do update
						set name = excluded.name, parent = excluded.parent;
					"
				);
				transaction
					.execute(
						statement.into(),
						db::params![
							message.id.to_string(),
							message.name.clone(),
							message.parent.as_ref().map(ToString::to_string)
						],
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				batch.items.push(tangram_index::batch::Item::PutGroup(
					tangram_index::group::put::Arg {
						id: message.id.clone(),
						parent: message.parent.clone(),
						specifier: message.specifier.clone(),
					},
				));

				Ok(created)
			},
			tg::sync::PutItemMessage::Object(_)
			| tg::sync::PutItemMessage::Process(_)
			| tg::sync::PutItemMessage::Sandbox(_) => Err(tg::error!("invalid sync item kind")),
			tg::sync::PutItemMessage::Organization(message) => {
				let created = Self::sync_get_database_validate_item_with_transaction(
					transaction,
					&message.id.clone().into(),
					&message.name,
					None,
					&message.specifier,
				)
				.await?;
				let p = transaction.p();
				let statement = formatdoc!(
					"
						insert into organizations (id, name)
						values ({p}1, {p}2)
						on conflict (id) do update
						set name = excluded.name;
					"
				);
				transaction
					.execute(
						statement.into(),
						db::params![message.id.to_string(), message.name.clone()],
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				batch
					.items
					.push(tangram_index::batch::Item::PutOrganization(
						tangram_index::organization::put::Arg {
							billing: None,
							id: message.id.clone(),
							specifier: message.specifier.clone(),
						},
					));

				Ok(created)
			},
			tg::sync::PutItemMessage::Tag(message) => {
				let created = Self::sync_get_database_validate_item_with_transaction(
					transaction,
					&message.id.clone().into(),
					&message.name,
					message.parent.as_ref(),
					&message.specifier,
				)
				.await?;
				let item = if let Ok(id) = tg::object::Id::try_from(message.item.clone()) {
					tg::Either::Left(id)
				} else if let Ok(id) = tg::process::Id::try_from(message.item.clone()) {
					tg::Either::Right(id)
				} else {
					return Err(tg::error!("invalid tag item"));
				};
				let item_string = item.to_string();
				let permissions = tag_permissions
					.get(&message.id)
					.ok_or_else(|| tg::error!("missing the tag permissions"))?;
				let permissions = serde_json::to_string(permissions)
					.map_err(|error| tg::error!(!error, "failed to serialize the permissions"))?;
				let p = transaction.p();
				let statement = formatdoc!(
					"
						insert into tags (id, name, parent, item, permissions)
						values ({p}1, {p}2, {p}3, {p}4, {p}5)
						on conflict (id) do update
						set name = excluded.name, parent = excluded.parent, item = excluded.item,
							permissions = case when tags.item = excluded.item then tags.permissions else excluded.permissions end;
					"
				);
				transaction
					.execute(
						statement.into(),
						db::params![
							message.id.to_string(),
							message.name.clone(),
							message.parent.as_ref().map(ToString::to_string),
							item_string,
							permissions
						],
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				#[derive(db::row::Deserialize)]
				struct Row {
					permissions: String,
				}
				let statement = formatdoc!(
					"
						select permissions
						from tags
						where id = {p}1;
					"
				);
				let row = transaction
					.query_one_into::<Row>(statement.into(), db::params![message.id.to_string()])
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				let permissions = serde_json::from_str(&row.permissions)
					.map_err(|error| tg::error!(!error, "failed to deserialize the permissions"))?;
				batch.items.push(tangram_index::batch::Item::PutTag(
					tangram_index::tag::put::Arg {
						id: message.id.clone(),
						item,
						name: message.name.clone(),
						owner: tag_owners.get(&message.id).cloned().flatten(),
						parent: message.parent.clone(),
						permissions,
						specifier: message.specifier.clone(),
					},
				));

				Ok(created)
			},
			tg::sync::PutItemMessage::User(message) => {
				let created = Self::sync_get_database_validate_item_with_transaction(
					transaction,
					&message.id.clone().into(),
					&message.name,
					None,
					&message.specifier,
				)
				.await?;
				let p = transaction.p();
				let statement = formatdoc!(
					"
						insert into users (id, name)
						values ({p}1, {p}2)
						on conflict (id) do update
						set name = excluded.name;
					"
				);
				transaction
					.execute(
						statement.into(),
						db::params![message.id.to_string(), message.name.clone()],
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				let statement = format!(r#"delete from user_emails where "user" = {p}1;"#);
				transaction
					.execute(statement.into(), db::params![message.id.to_string()])
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				for email in &message.emails {
					let statement = formatdoc!(
						r#"
							insert into user_emails ("user", email)
							values ({p}1, {p}2);
						"#
					);
					transaction
						.execute(
							statement.into(),
							db::params![message.id.to_string(), email.clone()],
						)
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				}
				batch.items.push(tangram_index::batch::Item::PutUser(
					tangram_index::user::put::Arg {
						billing: None,
						id: message.id.clone(),
						specifier: message.specifier.clone(),
					},
				));

				Ok(created)
			},
		}
	}

	async fn sync_get_database_validate_item_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::Id,
		name: &str,
		parent: Option<&tg::Id>,
		specifier: &tg::Specifier,
	) -> tg::Result<bool> {
		// Validate the ID and specifier.
		let by_id = Self::try_get_specifier_for_id_with_transaction(transaction, id).await?;
		let by_specifier =
			Self::try_get_id_for_specifier_with_transaction(transaction, specifier).await?;
		Self::sync_get_database_validate_id_and_specifier(
			id,
			specifier,
			by_id.as_ref(),
			by_specifier.as_ref(),
		)?;
		let created = by_id.is_none();

		// Validate the name and parent.
		if name != specifier.name() {
			return Err(tg::error!("the name does not match the specifier"));
		}
		if matches!(id.kind(), tg::id::Kind::Organization | tg::id::Kind::User)
			&& specifier.components().count() != 1
		{
			return Err(tg::error!(
				"a user or organization specifier must contain one component"
			));
		}
		let actual_parent = if let Some(parent_specifier) = specifier.parent() {
			let parent =
				Self::try_get_id_for_specifier_with_transaction(transaction, &parent_specifier)
					.await?
					.ok_or_else(|| tg::error!("the parent does not exist"))?;
			if parent.kind() == tg::id::Kind::Tag {
				return Err(tg::error!("a tag cannot be a parent"));
			}
			Some(parent)
		} else {
			None
		};
		if parent != actual_parent.as_ref() {
			return Err(tg::error!("the parent does not match the specifier"));
		}

		// Create the specifier.
		if created {
			Self::insert_specifier_with_transaction(transaction, id, specifier).await?;
		}

		Ok(created)
	}

	fn sync_get_database_item_depth(item: &tg::sync::PutItemMessage) -> usize {
		match item {
			tg::sync::PutItemMessage::Group(message) => message.specifier.components().count(),
			tg::sync::PutItemMessage::Object(_)
			| tg::sync::PutItemMessage::Process(_)
			| tg::sync::PutItemMessage::Sandbox(_) => usize::MAX,
			tg::sync::PutItemMessage::Organization(message) => {
				message.specifier.components().count()
			},
			tg::sync::PutItemMessage::Tag(message) => message.specifier.components().count(),
			tg::sync::PutItemMessage::User(message) => message.specifier.components().count(),
		}
	}

	fn sync_get_database_item_id(item: &tg::sync::PutItemMessage) -> tg::Result<tg::Id> {
		let id = match item {
			tg::sync::PutItemMessage::Group(message) => message.id.clone().into(),
			tg::sync::PutItemMessage::Object(_) | tg::sync::PutItemMessage::Process(_) => {
				return Err(tg::error!("invalid sync item kind"));
			},
			tg::sync::PutItemMessage::Organization(message) => message.id.clone().into(),
			tg::sync::PutItemMessage::Sandbox(_) => {
				return Err(tg::error!("invalid sync item kind"));
			},
			tg::sync::PutItemMessage::Tag(message) => message.id.clone().into(),
			tg::sync::PutItemMessage::User(message) => message.id.clone().into(),
		};

		Ok(id)
	}

	fn sync_get_database_item_specifier(
		item: &tg::sync::PutItemMessage,
	) -> tg::Result<&tg::Specifier> {
		let specifier = match item {
			tg::sync::PutItemMessage::Group(message) => &message.specifier,
			tg::sync::PutItemMessage::Object(_)
			| tg::sync::PutItemMessage::Process(_)
			| tg::sync::PutItemMessage::Sandbox(_) => {
				return Err(tg::error!("invalid sync item kind"));
			},
			tg::sync::PutItemMessage::Organization(message) => &message.specifier,
			tg::sync::PutItemMessage::Tag(message) => &message.specifier,
			tg::sync::PutItemMessage::User(message) => &message.specifier,
		};

		Ok(specifier)
	}

	fn sync_get_database_validate_id_and_specifier(
		id: &tg::Id,
		specifier: &tg::Specifier,
		by_id: Option<&tg::Specifier>,
		by_specifier: Option<&tg::Id>,
	) -> tg::Result<()> {
		if by_id.is_some_and(|candidate| candidate != specifier) {
			return Err(tg::error!("the id is already in use"));
		}
		if by_specifier.is_some_and(|candidate| candidate != id) {
			return Err(tg::error!("the specifier is already in use"));
		}

		Ok(())
	}
}
