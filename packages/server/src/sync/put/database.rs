use {
	crate::{Session, sync::put::State},
	indoc::formatdoc,
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_index::prelude::*,
};

pub struct Item {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::Id,
	pub send: bool,
	pub token: Option<tg::grant::Token>,
}

struct Output {
	children: Vec<tg::Referent<tg::Id>>,
	message: Option<tg::sync::PutItemMessage>,
}

impl Session {
	pub(super) async fn sync_put_database(
		&self,
		state: Arc<State>,
		mut receiver: tokio::sync::mpsc::Receiver<Item>,
	) -> tg::Result<()> {
		while let Some(item) = receiver.recv().await {
			self.sync_put_database_item(&state, item).await?;
		}

		Ok(())
	}

	async fn sync_put_database_item(&self, state: &State, item: Item) -> tg::Result<()> {
		// Authorize the item.
		let permission = Self::sync_put_database_read_permission(&item.id)?;
		let resource = tg::grant::Resource::Id(item.id.clone());
		let resource = tg::Referent::with_item_and_token(resource, item.token.clone());
		let authorized = self
			.authorize(resource, permission)
			.await?
			.is_some_and(|permissions| permissions.contains(permission));
		let visible = if item.id.kind() == tg::id::Kind::Tag {
			self.server
				.index
				.visible(std::slice::from_ref(&item.id), &self.context.principal)
				.await?
				.pop()
				.unwrap()
		} else {
			false
		};
		if !authorized && !visible {
			if item.send {
				self.sync_put_database_missing(state, &item.id).await;
			}
			if item.descendants {
				state
					.graph
					.lock()
					.unwrap()
					.finish_item_remote_descendants(&item.id, &[]);
			}
			state.queue.close_if_end();
			return Ok(());
		}

		// Read the item.
		let output = self.sync_put_database_read(state, &item).await?;
		let Some(output) = output else {
			if item.send {
				self.sync_put_database_missing(state, &item.id).await;
			}
			if item.descendants {
				state
					.graph
					.lock()
					.unwrap()
					.finish_item_remote_descendants(&item.id, &[]);
			}
			state.queue.close_if_end();
			return Ok(());
		};

		// Complete the item in the graph.
		if item.send {
			state
				.graph
				.lock()
				.unwrap()
				.finish_database_item_remote_found(&item.id);
		}
		// Send the item.
		if let Some(message) = output.message {
			crate::checkpoint!(
				self.server,
				"sync.put.database.item.send",
				descendants = item.descendants,
				id = %item.id,
			)
			.await;
			let message = tg::sync::PutMessage::Item(message);
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|error| tg::error!(!error, "failed to send the item"))?;
		}
		crate::checkpoint!(
			self.server,
			"sync.put.database.item",
			descendants = item.descendants,
			id = %item.id,
		)
		.await;

		// Update the graph and enqueue the children.
		if item.descendants {
			let children = output
				.children
				.iter()
				.map(|child| child.item.clone())
				.collect::<Vec<_>>();
			for child in output.children {
				state
					.queue
					.enqueue(item.eager, child.item, child.options.token)?;
			}
			state
				.graph
				.lock()
				.unwrap()
				.finish_item_remote_descendants(&item.id, &children);
		}
		state.queue.close_if_end();

		Ok(())
	}

	async fn sync_put_database_read(
		&self,
		state: &State,
		item: &Item,
	) -> tg::Result<Option<Output>> {
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
		let Some(specifier) =
			Self::try_get_specifier_for_id_with_transaction(&transaction, &item.id).await?
		else {
			return Ok(None);
		};
		let children = if item.descendants {
			self.sync_put_database_read_children(state, &transaction, &item.id)
				.await?
		} else {
			Vec::new()
		};
		let message = if item.send {
			Some(match item.id.kind() {
				tg::id::Kind::Group => {
					let id = item.id.clone().try_into()?;
					let group = Self::try_get_group_with_transaction(&transaction, &id)
						.await?
						.ok_or_else(|| tg::error!("failed to find the group"))?;
					tg::sync::PutItemMessage::Group(tg::sync::PutItemGroupMessage {
						id,
						name: group.name,
						parent: group.parent,
						specifier,
					})
				},
				tg::id::Kind::Organization => {
					let id = item.id.clone().try_into()?;
					let organization =
						Self::try_get_organization_with_transaction(&transaction, &id)
							.await?
							.ok_or_else(|| tg::error!("failed to find the organization"))?;
					tg::sync::PutItemMessage::Organization(tg::sync::PutItemOrganizationMessage {
						id,
						name: organization.name,
						specifier,
					})
				},
				tg::id::Kind::Tag => {
					let id = item.id.clone().try_into()?;
					let data = Self::get_tag_data_with_transaction(&transaction, &id).await?;
					let id = data.id;
					let item = match data.item {
						tg::tag::data::Item::Object(id) => id.into(),
						tg::tag::data::Item::Process(id) => id.into(),
					};
					tg::sync::PutItemMessage::Tag(tg::sync::PutItemTagMessage {
						id,
						item,
						name: data.name,
						parent: data.parent,
						specifier: data.specifier,
					})
				},
				tg::id::Kind::User => {
					let id = item.id.clone().try_into()?;
					let user = Self::try_get_user_with_transaction(&transaction, &id)
						.await?
						.ok_or_else(|| tg::error!("failed to find the user"))?;
					tg::sync::PutItemMessage::User(tg::sync::PutItemUserMessage {
						emails: user.emails,
						id,
						name: user.name,
						specifier,
					})
				},
				_ => return Err(tg::error!(id = %item.id, "invalid database item kind")),
			})
		} else {
			None
		};
		let output = Output { children, message };

		Ok(Some(output))
	}

	async fn sync_put_database_read_children(
		&self,
		state: &State,
		transaction: &crate::database::Transaction<'_>,
		id: &tg::Id,
	) -> tg::Result<Vec<tg::Referent<tg::Id>>> {
		let enabled = match id.kind() {
			tg::id::Kind::Group => state.arg.group_children,
			tg::id::Kind::Organization => state.arg.organization_children,
			tg::id::Kind::Tag => state.arg.tag_items,
			tg::id::Kind::User => state.arg.user_children,
			_ => false,
		};
		if !enabled {
			return Ok(Vec::new());
		}
		if id.kind() == tg::id::Kind::Tag {
			let tag = id.clone().try_into()?;
			let item = Self::get_tag_data_with_transaction(transaction, &tag)
				.await?
				.item;
			let id = match item {
				tg::tag::data::Item::Object(id) => id.into(),
				tg::tag::data::Item::Process(id) => id.into(),
			};
			let token = self
				.create_tag_item_token_with_transaction(transaction, &tag, &id)
				.await?;
			let item = tg::Referent::with_item_and_token(id, token);

			return Ok(vec![item]);
		}
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select id from groups where parent = {p}1
				union all
				select id from tags where parent = {p}1
				order by id;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let children = rows
			.into_iter()
			.map(|row| tg::Referent::with_item(row.id))
			.collect();

		Ok(children)
	}

	async fn sync_put_database_missing(&self, state: &State, id: &tg::Id) {
		let selectors = state
			.graph
			.lock()
			.unwrap()
			.finish_database_item_remote_missing(id);
		for selector in selectors {
			let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
				selector,
				token: None,
			});
			state.sender.send(Ok(message)).await.ok();
		}
		state.queue.close_if_end();
	}

	fn sync_put_database_read_permission(id: &tg::Id) -> tg::Result<tg::grant::Permission> {
		let permission = match id.kind() {
			tg::id::Kind::Group => {
				tg::grant::Permission::Group(tg::grant::permission::group::Permission::Read)
			},
			tg::id::Kind::Organization => tg::grant::Permission::Organization(
				tg::grant::permission::organization::Permission::Read,
			),
			tg::id::Kind::Tag => {
				tg::grant::Permission::Tag(tg::grant::permission::tag::Permission::Read)
			},
			tg::id::Kind::User => {
				tg::grant::Permission::User(tg::grant::permission::user::Permission::Read)
			},
			_ => return Err(tg::error!(%id, "invalid database item kind")),
		};

		Ok(permission)
	}
}
