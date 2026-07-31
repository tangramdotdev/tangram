use {
	crate::{Session, sync::get::State},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

#[derive(Clone)]
pub enum Item {
	Group(tg::sync::PutItemGroupMessage),
	Organization(tg::sync::PutItemOrganizationMessage),
	Tag(tg::sync::PutItemTagMessage),
	User(tg::sync::PutItemUserMessage),
}

impl Item {
	#[must_use]
	fn id(&self) -> tg::Id {
		match self {
			Self::Group(message) => message.id.clone().into(),
			Self::Organization(message) => message.id.clone().into(),
			Self::Tag(message) => message.id.clone().into(),
			Self::User(message) => message.id.clone().into(),
		}
	}

	#[must_use]
	fn name(&self) -> &str {
		match self {
			Self::Group(message) => &message.name,
			Self::Organization(message) => &message.name,
			Self::Tag(message) => &message.name,
			Self::User(message) => &message.name,
		}
	}

	#[must_use]
	fn parent(&self) -> Option<&tg::Id> {
		match self {
			Self::Group(message) => message.parent.as_ref(),
			Self::Organization(_) | Self::User(_) => None,
			Self::Tag(message) => message.parent.as_ref(),
		}
	}

	#[must_use]
	fn specifier(&self) -> &tg::Specifier {
		match self {
			Self::Group(message) => &message.specifier,
			Self::Organization(message) => &message.specifier,
			Self::Tag(message) => &message.specifier,
			Self::User(message) => &message.specifier,
		}
	}
}

impl Session {
	pub(super) async fn sync_get_database(
		&self,
		state: Arc<State>,
		mut receiver: tokio::sync::mpsc::Receiver<Item>,
	) -> tg::Result<()> {
		while let Some(item) = receiver.recv().await {
			self.sync_get_database_item(&state, item).await?;
		}

		Ok(())
	}

	async fn sync_get_database_item(&self, state: &State, item: Item) -> tg::Result<()> {
		// Authorize the write.
		self.sync_get_database_authorize(&item).await?;

		// Apply the item and enqueue the index batch atomically.
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let item = item.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					let created = session
						.sync_get_database_item_with_transaction(transaction, &item, &mut batch)
						.await?;
					if created
						&& let Some(arg) = session.sync_get_create_temporary_grant(&item.id())?
					{
						batch.items.push(tangram_index::batch::Item::PutGrant(arg));
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

		// Update the graph.
		let id = item.id();
		state.graph.lock().unwrap().update_item_local_applied(&id);
		state.progress.increment_transferred_item(&id);
		if state.graph.lock().unwrap().end_local(&state.arg) {
			state.queue.close();
		}

		Ok(())
	}

	async fn sync_get_database_authorize(&self, item: &Item) -> tg::Result<()> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let id = item.id();
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
		let by_id = Self::try_get_specifier_by_id_with_transaction(&transaction, &id).await?;
		let by_specifier =
			Self::try_get_specifier_with_transaction(&transaction, item.specifier()).await?;
		Self::sync_get_database_validate_id_and_specifier(
			&id,
			item.specifier(),
			by_id.as_ref(),
			by_specifier.as_ref(),
		)?;
		drop(transaction);
		if by_id.is_some() {
			let permission = Self::write_permission_for_resource(&id)?;
			let authorized = self
				.authorize(tg::grant::Resource::Id(id), permission)
				.await?;
			if !authorized.is_some_and(|permissions| permissions.contains(permission)) {
				return Err(tg::error!("unauthorized"));
			}
		} else if let Some(permission) = self
			.write_permission_for_specifier(item.specifier())
			.await?
		{
			let resource = tg::grant::Resource::Specifier(item.specifier().clone());
			let authorized = self.authorize(resource, permission).await?;
			if authorized.is_some_and(|permissions| !permissions.contains(permission)) {
				return Err(tg::error!("unauthorized"));
			}
		}

		Ok(())
	}

	async fn sync_get_database_item_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		item: &Item,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<bool> {
		// Validate the ID and specifier.
		let id = item.id();
		let specifier = item.specifier();
		let by_id = Self::try_get_specifier_by_id_with_transaction(transaction, &id).await?;
		let by_specifier = Self::try_get_specifier_with_transaction(transaction, specifier).await?;
		Self::sync_get_database_validate_id_and_specifier(
			&id,
			specifier,
			by_id.as_ref(),
			by_specifier.as_ref(),
		)?;
		let created = by_id.is_none();

		// Validate the specifier and parent.
		if item.name() != specifier.name() {
			return Err(tg::error!("the name does not match the specifier"));
		}
		if matches!(item, Item::Organization(_) | Item::User(_))
			&& specifier.components().count() != 1
		{
			return Err(tg::error!(
				"a user or organization specifier must contain one component"
			));
		}
		let parent = if let Some(parent_specifier) = specifier.parent() {
			let parent = Self::try_get_specifier_with_transaction(transaction, &parent_specifier)
				.await?
				.ok_or_else(|| tg::error!("the parent does not exist"))?;
			if parent.kind() == tg::id::Kind::Tag {
				return Err(tg::error!("a tag cannot be a parent"));
			}
			Some(parent.id)
		} else {
			None
		};
		if item.parent() != parent.as_ref() {
			return Err(tg::error!("the parent does not match the specifier"));
		}

		// Create the specifier.
		if created {
			Self::create_specifier_with_transaction(transaction, &id, parent.as_ref(), specifier)
				.await?;
		}

		// Upsert the concrete item and create the index mutation.
		match item {
			Item::Group(message) => {
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
			},
			Item::Organization(message) => {
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
							id: message.id.clone(),
							specifier: message.specifier.clone(),
						},
					));
			},
			Item::Tag(message) => {
				let item = if let Ok(id) = tg::object::Id::try_from(message.item.clone()) {
					tg::Either::Left(id)
				} else if let Ok(id) = tg::process::Id::try_from(message.item.clone()) {
					tg::Either::Right(id)
				} else {
					return Err(tg::error!("invalid tag item"));
				};
				let item_string = item.to_string();
				let permissions = serde_json::to_string(&message.permissions)
					.map_err(|error| tg::error!(!error, "failed to serialize the permissions"))?;
				let p = transaction.p();
				let statement = formatdoc!(
					"
						insert into tags (id, name, parent, item, permissions)
						values ({p}1, {p}2, {p}3, {p}4, {p}5)
						on conflict (id) do update
						set name = excluded.name, parent = excluded.parent, item = excluded.item,
							permissions = excluded.permissions;
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
				batch.items.push(tangram_index::batch::Item::PutTag(
					tangram_index::tag::put::Arg {
						id: message.id.clone(),
						item,
						name: message.name.clone(),
						parent: message.parent.clone(),
						permissions: message.permissions.clone(),
						specifier: message.specifier.clone(),
					},
				));
			},
			Item::User(message) => {
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
				batch.items.push(tangram_index::batch::Item::PutUser(
					tangram_index::user::put::Arg {
						id: message.id.clone(),
						specifier: message.specifier.clone(),
					},
				));
			},
		}

		Ok(created)
	}

	fn sync_get_database_validate_id_and_specifier(
		id: &tg::Id,
		specifier: &tg::Specifier,
		by_id: Option<&crate::specifier::Item>,
		by_specifier: Option<&crate::specifier::Item>,
	) -> tg::Result<()> {
		if by_id.is_some_and(|item| item.specifier != *specifier) {
			return Err(tg::error!("the id is already in use"));
		}
		if by_specifier.is_some_and(|item| item.id != *id) {
			return Err(tg::error!("the specifier is already in use"));
		}

		Ok(())
	}
}
