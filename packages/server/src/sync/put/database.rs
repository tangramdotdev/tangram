use {
	crate::{Session, sync::put::State},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_index::prelude::*,
};

#[derive(Clone)]
pub struct Node {
	pub descendants: bool,
	pub eager: bool,
	pub id: tg::Id,
	pub send: bool,
	pub token: Option<tg::authorization::Token>,
}

struct Output {
	children: Vec<tg::Referent<tg::Id>>,
	message: Option<tg::sync::PutNodeMessage>,
}

impl Session {
	pub(super) async fn sync_put_database(
		&self,
		state: Arc<State>,
		mut receiver: tokio::sync::mpsc::Receiver<Node>,
	) -> tg::Result<()> {
		while let Some(node) = receiver.recv().await {
			self.sync_put_database_node(&state, node).await?;
		}

		Ok(())
	}

	async fn sync_put_database_node(&self, state: &State, node: Node) -> tg::Result<()> {
		// Authorize the node.
		let permission = Self::sync_put_database_read_permission(&node.id)?;
		let resource = tg::Selector::Id(node.id.clone());
		let resource = tg::Referent::with_node_and_token(resource, node.token.clone());
		let authorized = self
			.authorize(resource, permission)
			.await?
			.is_some_and(|permissions| permissions.contains(permission));
		let visible = if node.id.kind() == tg::id::Kind::Tag {
			self.server
				.index
				.visible(std::slice::from_ref(&node.id), &self.context.principal)
				.await?
				.pop()
				.unwrap()
		} else {
			false
		};
		if !authorized && !visible {
			if node.send {
				self.sync_put_database_missing(state, &node.id).await;
			}
			if node.descendants {
				state
					.graph
					.lock()
					.unwrap()
					.finish_node_remote_descendants(&node.id, &[]);
			}
			state.queue.finish_node();
			return Ok(());
		}

		// Read the node.
		let output = self.sync_put_database_read(state, &node).await?;
		let Some(output) = output else {
			if node.send {
				self.sync_put_database_missing(state, &node.id).await;
			}
			if node.descendants {
				state
					.graph
					.lock()
					.unwrap()
					.finish_node_remote_descendants(&node.id, &[]);
			}
			state.queue.finish_node();
			return Ok(());
		};

		// Complete the node in the graph.
		if node.send {
			state
				.graph
				.lock()
				.unwrap()
				.finish_database_node_remote_found(&node.id);
		}
		// Send the node.
		if let Some(message) = output.message {
			crate::checkpoint!(
				self.server,
				"sync.put.database.node.send",
				descendants = node.descendants,
				id = %node.id,
			)
			.await;
			let message = tg::sync::PutMessage::Node(message);
			state
				.sender
				.send(Ok(message))
				.await
				.map_err(|error| tg::error!(!error, "failed to send the node"))?;
		}
		crate::checkpoint!(
			self.server,
			"sync.put.database.node",
			descendants = node.descendants,
			id = %node.id,
		)
		.await;

		// Update the graph and enqueue the children.
		if node.descendants {
			let children = output
				.children
				.iter()
				.map(|child| child.node.clone())
				.collect::<Vec<_>>();
			for child in output.children {
				state.queue.enqueue(
					node.eager,
					child.node,
					child.options.tokens.local().cloned(),
				)?;
			}
			state
				.graph
				.lock()
				.unwrap()
				.finish_node_remote_descendants(&node.id, &children);
		}
		state.queue.finish_node();

		Ok(())
	}

	async fn sync_put_database_read(
		&self,
		state: &State,
		node: &Node,
	) -> tg::Result<Option<Output>> {
		let children_enabled = match node.id.kind() {
			tg::id::Kind::Group => state.arg.group_children,
			tg::id::Kind::Organization => state.arg.organization_children,
			tg::id::Kind::Tag => state.arg.tag_targets,
			tg::id::Kind::User => state.arg.user_children,
			_ => false,
		};
		let node = node.clone();
		let session = self.clone();
		self.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let node = node.clone();
				let session = session.clone();
				async move {
					session
						.sync_put_database_read_with_transaction(
							transaction,
							&node,
							children_enabled,
						)
						.await
				}
				.boxed()
			})
			.await
	}

	async fn sync_put_database_read_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		node: &Node,
		children_enabled: bool,
	) -> tg::Result<ControlFlow<Option<Output>, crate::database::Error>> {
		let specifier =
			match Self::try_get_specifier_for_id_with_transaction(transaction, &node.id).await? {
				ControlFlow::Break(specifier) => specifier,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let Some(specifier) = specifier else {
			return Ok(ControlFlow::Break(None));
		};
		let children = if node.descendants {
			match self
				.sync_put_database_read_children_with_transaction(
					transaction,
					&node.id,
					children_enabled,
				)
				.await?
			{
				ControlFlow::Break(children) => children,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		} else {
			Vec::new()
		};
		let message = if node.send {
			Some(match node.id.kind() {
				tg::id::Kind::Group => {
					let id = node.id.clone().try_into()?;
					let group = match Self::try_get_group_with_transaction(transaction, &id).await?
					{
						ControlFlow::Break(group) => group,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					}
					.ok_or_else(|| tg::error!("failed to find the group"))?;
					tg::sync::PutNodeMessage::Group(tg::sync::PutNodeGroupMessage {
						id,
						name: group.name,
						parent: group.parent,
						specifier,
					})
				},
				tg::id::Kind::Organization => {
					let id = node.id.clone().try_into()?;
					let organization = match Self::try_get_organization_with_transaction(
						transaction,
						&id,
					)
					.await?
					{
						ControlFlow::Break(organization) => organization,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					}
					.ok_or_else(|| tg::error!("failed to find the organization"))?;
					tg::sync::PutNodeMessage::Organization(tg::sync::PutNodeOrganizationMessage {
						id,
						name: organization.name,
						specifier,
					})
				},
				tg::id::Kind::Tag => {
					let id = node.id.clone().try_into()?;
					let data = match Self::get_tag_data_with_transaction(transaction, &id).await? {
						ControlFlow::Break(data) => data,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					let id = data.id;
					let target = match data.target {
						tg::tag::data::Target::Object(id) => id.into(),
						tg::tag::data::Target::Process(id) => id.into(),
					};
					tg::sync::PutNodeMessage::Tag(tg::sync::PutNodeTagMessage {
						id,
						name: data.name,
						parent: data.parent,
						specifier: data.specifier,
						target,
						token: None,
					})
				},
				tg::id::Kind::User => {
					let id = node.id.clone().try_into()?;
					let user = match Self::try_get_user_with_transaction(transaction, &id).await? {
						ControlFlow::Break(user) => user,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					}
					.ok_or_else(|| tg::error!("failed to find the user"))?;
					tg::sync::PutNodeMessage::User(tg::sync::PutNodeUserMessage {
						emails: user.emails,
						id,
						name: user.name,
						specifier,
					})
				},
				_ => return Err(tg::error!(id = %node.id, "invalid database node kind")),
			})
		} else {
			None
		};
		let output = Output { children, message };

		Ok(ControlFlow::Break(Some(output)))
	}

	async fn sync_put_database_read_children_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		id: &tg::Id,
		enabled: bool,
	) -> tg::Result<ControlFlow<Vec<tg::Referent<tg::Id>>, crate::database::Error>> {
		if !enabled {
			return Ok(ControlFlow::Break(Vec::new()));
		}
		if id.kind() == tg::id::Kind::Tag {
			let tag = id.clone().try_into()?;
			let data = match Self::get_tag_data_with_transaction(transaction, &tag).await? {
				ControlFlow::Break(data) => data,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let target = data.target;
			let id = match target {
				tg::tag::data::Target::Object(id) => id.into(),
				tg::tag::data::Target::Process(id) => id.into(),
			};
			let token = match self
				.create_tag_target_token_with_transaction(transaction, &tag, &id)
				.await?
			{
				ControlFlow::Break(token) => token,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			let node = tg::Referent::with_node_and_token(id, token);

			return Ok(ControlFlow::Break(vec![node]));
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
		let result = transaction
			.query_all_into::<Row>(statement.into(), db::params![id.to_string()])
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let children = rows
			.into_iter()
			.map(|row| tg::Referent::with_node(row.id))
			.collect();

		Ok(ControlFlow::Break(children))
	}

	async fn sync_put_database_missing(&self, state: &State, id: &tg::Id) {
		let selectors = state
			.graph
			.lock()
			.unwrap()
			.finish_database_node_remote_missing(id);
		for selector in selectors {
			let message = tg::sync::PutMessage::Missing(tg::sync::PutMissingMessage {
				selector,
				token: None,
			});
			state.sender.send(Ok(message)).await.ok();
		}
		state.queue.close_if_end();
	}

	fn sync_put_database_read_permission(id: &tg::Id) -> tg::Result<tg::authorization::Permission> {
		let permission = match id.kind() {
			tg::id::Kind::Group => tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Read,
			),
			tg::id::Kind::Organization => tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Read,
			),
			tg::id::Kind::Tag => tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Read,
			),
			tg::id::Kind::User => tg::authorization::Permission::User(
				tg::authorization::permission::user::Permission::Read,
			),
			_ => return Err(tg::error!(%id, "invalid database node kind")),
		};

		Ok(permission)
	}
}
