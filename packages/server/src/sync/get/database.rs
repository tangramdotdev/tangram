use {
	crate::{Session, database::Transaction, sync::graph::Graph},
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _},
	indoc::formatdoc,
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

const SYNC_GET_DATABASE_BATCH_SIZE: usize = 128;

#[derive(Default)]
struct Namespace {
	ids: BTreeMap<tg::Id, tg::Specifier>,
	specifiers: BTreeMap<tg::Specifier, tg::Id>,
}

impl Namespace {
	fn insert(&mut self, id: tg::Id, specifier: tg::Specifier) {
		self.ids.insert(id.clone(), specifier.clone());
		self.specifiers.insert(specifier, id);
	}
}

impl Session {
	pub(super) async fn sync_get_database(&self, graph: &Arc<Mutex<Graph>>) -> tg::Result<()> {
		// Get the staged nodes.
		let mut nodes = {
			let graph = graph.lock().unwrap();
			graph
				.local_messages()
				.into_iter()
				.filter(|node| !matches!(node, tg::sync::PutNodeMessage::Sandbox(_)))
				.collect::<Vec<_>>()
		};
		if nodes.is_empty() {
			return Ok(());
		}

		// Send the database nodes to the primary region.
		if !self.server.is_primary_region() {
			self.sync_get_database_update_tag_target_permissions(graph, &nodes)
				.await?;
			let tag_permissions = self.sync_get_database_tag_permissions(graph, &nodes)?;
			self.sync_get_database_add_tag_target_tokens(&mut nodes, &tag_permissions)?;
			self.sync_get_database_to_primary_region(nodes).await?;

			return Ok(());
		}

		// Authorize the writes.
		self.sync_get_database_authorize(&nodes).await?;

		// Update the tag target permissions in the graph.
		self.sync_get_database_update_tag_target_permissions(graph, &nodes)
			.await?;
		let tag_permissions = self.sync_get_database_tag_permissions(graph, &nodes)?;
		let touched_at = self.server.clock.unix_timestamp()?;

		// Sort the nodes so that parents are written before their children.
		nodes.sort_by_key(Self::sync_get_database_node_depth);

		// Write all of the nodes and enqueue their index mutations atomically.
		let session = self.clone();
		let _invalidated_specifiers = self
			.server
			.database
			.run(|transaction| {
				let nodes = nodes.clone();
				let session = session.clone();
				let tag_permissions = tag_permissions.clone();
				async move {
					session
						.sync_get_database_with_transaction(
							transaction,
							&nodes,
							&tag_permissions,
							touched_at,
						)
						.await
				}
				.boxed()
			})
			.await?;
		self.server
			.spawn_publish_database_outbox_notification_task();
		self.checkout_index_barrier().await?;

		Ok(())
	}

	fn sync_get_database_add_tag_target_tokens(
		&self,
		nodes: &mut [tg::sync::PutNodeMessage],
		tag_permissions: &BTreeMap<tg::tag::Id, Vec<tg::authorization::Permission>>,
	) -> tg::Result<()> {
		for node in nodes {
			let tg::sync::PutNodeMessage::Tag(message) = node else {
				continue;
			};
			let permissions = tag_permissions
				.get(&message.id)
				.cloned()
				.ok_or_else(|| tg::error!("missing tag target permissions"))?;
			let token = self
				.create_tag_target_token_with_permissions(&message.target, permissions)?
				.ok_or_else(|| tg::error!("authorization token signing is not configured"))?;
			message.token = Some(token);
		}

		Ok(())
	}

	async fn sync_get_database_to_primary_region(
		&self,
		mut nodes: Vec<tg::sync::PutNodeMessage>,
	) -> tg::Result<()> {
		// Start a sync to the primary region.
		let primary_region_arg = tg::sync::Arg {
			ancestors: tg::node::AncestorsPull::Missing,
			..Default::default()
		};
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		nodes.sort_by_key(Self::sync_get_database_node_depth);
		let messages = nodes
			.into_iter()
			.map(|node| {
				Ok::<_, tg::Error>(tg::sync::Message::Put(tg::sync::PutMessage::Node(node)))
			})
			.chain([
				Ok(tg::sync::Message::Put(tg::sync::PutMessage::End)),
				Ok(tg::sync::Message::End),
			]);
		let input = futures::stream::iter(messages).boxed();
		let output = client
			.sync(primary_region_arg, input)
			.await
			.map_err(|error| tg::error!(!error, "failed to start the primary region sync"))?;
		let mut output = std::pin::pin!(output);
		let mut get_end_received = false;
		let mut end_received = false;
		while let Some(message) = output.try_next().await? {
			match message {
				tg::sync::Message::Get(tg::sync::GetMessage::End) => {
					get_end_received = true;
				},
				tg::sync::Message::Get(tg::sync::GetMessage::Progress(_))
				| tg::sync::Message::Put(
					tg::sync::PutMessage::End | tg::sync::PutMessage::Progress(_),
				) => {},
				tg::sync::Message::End => {
					end_received = true;
					break;
				},
				tg::sync::Message::Get(
					tg::sync::GetMessage::Node(_) | tg::sync::GetMessage::Stored(_),
				)
				| tg::sync::Message::Put(
					tg::sync::PutMessage::Missing(_) | tg::sync::PutMessage::Node(_),
				) => {
					return Err(tg::error!("unexpected primary region sync message"));
				},
			}
		}
		if !get_end_received || !end_received {
			return Err(tg::error!(
				"the primary region sync ended before completion"
			));
		}

		Ok(())
	}

	async fn sync_get_database_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
		tag_permissions: &BTreeMap<tg::tag::Id, Vec<tg::authorization::Permission>>,
		touched_at: i64,
	) -> tg::Result<ControlFlow<BTreeSet<tg::Specifier>, crate::database::Error>> {
		let mut batch = tangram_index::batch::Arg::default();
		let mut tag_accounts = BTreeMap::new();
		let invalidated_specifiers = nodes
			.iter()
			.filter_map(|node| match node {
				tg::sync::PutNodeMessage::Tag(message) => Some(message.specifier.clone()),
				_ => None,
			})
			.collect::<BTreeSet<_>>();
		let mut namespace =
			match Self::sync_get_database_namespace_with_transaction(transaction, nodes).await? {
				ControlFlow::Break(namespace) => namespace,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		for node in nodes {
			if let tg::sync::PutNodeMessage::Tag(message) = node {
				let account = match self
					.usage_account_for_specifier_with_transaction(transaction, &message.specifier)
					.await?
				{
					ControlFlow::Break(account) => account,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				tag_accounts.insert(message.id.clone(), account);
			}
			let created = match self
				.sync_get_database_node_with_transaction(
					transaction,
					node,
					&mut namespace,
					&tag_accounts,
					tag_permissions,
					&mut batch,
				)
				.await?
			{
				ControlFlow::Break(created) => created,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			if created
				&& let Some(arg) =
					self.sync_get_create_temporary_grant(&Self::sync_get_database_node_id(node)?)?
			{
				batch.items.push(tangram_index::batch::Item::PutGrant(arg));
			}
		}
		for node in nodes {
			let tg::sync::PutNodeMessage::Tag(message) = node else {
				continue;
			};
			let Some(account) = tag_accounts.get(&message.id).cloned().flatten() else {
				continue;
			};
			let item = if let Ok(object) = message.target.clone().try_into() {
				tangram_index::batch::Item::PutAccountObject(
					tangram_index::usage::storage::put::ObjectArg {
						account,
						object,
						touched_at,
					},
				)
			} else if let Ok(process) = message.target.clone().try_into() {
				tangram_index::batch::Item::PutAccountProcess(
					tangram_index::usage::storage::put::ProcessArg {
						account,
						process,
						touched_at,
					},
				)
			} else {
				return Err(tg::error!("invalid tag target"));
			};
			batch.items.push(item);
		}
		match self
			.server
			.enqueue_database_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(invalidated_specifiers))
	}

	async fn sync_get_database_authorize(
		&self,
		nodes: &[tg::sync::PutNodeMessage],
	) -> tg::Result<()> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}

		// Read existing IDs and specifiers from the database.
		let ids = nodes
			.iter()
			.map(Self::sync_get_database_node_id)
			.collect::<tg::Result<Vec<_>>>()?;
		let specifiers = nodes
			.iter()
			.map(|node| Self::sync_get_database_node_specifier(node).cloned())
			.collect::<tg::Result<Vec<_>>>()?;
		let nodes = nodes.to_vec();
		let connection_options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			..Default::default()
		};
		let existing_named_nodes = self
			.server
			.database
			.run_with_options(connection_options, |transaction| {
				let nodes = nodes.clone();
				async move {
					Self::sync_get_database_existing_named_nodes_with_transaction(
						transaction,
						&nodes,
					)
					.await
				}
				.boxed()
			})
			.await?;

		// Validate the conflicts and collect all required authorizations.
		let mut authorization = BTreeMap::new();
		for (id, specifier) in std::iter::zip(ids, specifiers) {
			let by_id = existing_named_nodes.ids.get(&id);
			let by_specifier = existing_named_nodes.specifiers.get(&specifier);
			Self::sync_get_database_validate_id_and_specifier(
				&id,
				&specifier,
				by_id,
				by_specifier,
			)?;
			if by_specifier.is_some() {
				let permission = Self::write_permission_for_resource(&id)?;
				let permissions = tg::authorization::permission::Set::from_permission(permission);
				let resource = tg::Selector::<tg::Id>::Specifier(specifier.clone());
				authorization.insert(resource, permissions);
			}
		}

		// Authorize the writes.
		let authorization = authorization.into_iter().collect::<Vec<_>>();
		for authorization in authorization.chunks(SYNC_GET_DATABASE_BATCH_SIZE) {
			let args = authorization
				.iter()
				.map(|(resource, permissions)| (resource.clone(), *permissions))
				.collect::<Vec<_>>();
			let outputs = self.authorize_batch(args).await?;
			for ((_, permissions), output) in std::iter::zip(authorization, outputs) {
				let authorized = output.is_some_and(|output| output.contains(*permissions));
				if !authorized {
					return Err(tg::error!("unauthorized"));
				}
			}
		}

		Ok(())
	}

	async fn sync_get_database_existing_named_nodes_with_transaction(
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
	) -> tg::Result<ControlFlow<Namespace, crate::database::Error>> {
		let existing_named_nodes =
			match Self::sync_get_database_namespace_with_transaction(transaction, nodes).await? {
				ControlFlow::Break(existing_named_nodes) => existing_named_nodes,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};

		Ok(ControlFlow::Break(existing_named_nodes))
	}

	async fn sync_get_database_update_tag_target_permissions(
		&self,
		graph: &Arc<Mutex<Graph>>,
		nodes: &[tg::sync::PutNodeMessage],
	) -> tg::Result<()> {
		let mut objects = BTreeSet::new();
		let mut processes = BTreeSet::new();
		for node in nodes {
			let tg::sync::PutNodeMessage::Tag(message) = node else {
				continue;
			};
			if message.token.is_some() {
				self.sync_get_database_tag_permissions_from_token(message)?;
				continue;
			}
			if let Ok(id) = tg::object::Id::try_from(message.target.clone()) {
				objects.insert(id);
			} else if let Ok(id) = tg::process::Id::try_from(message.target.clone()) {
				processes.insert(id);
			} else {
				return Err(tg::error!("invalid tag target"));
			}
		}

		let object_permissions = tg::authorization::permission::Set::from_permission(
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			),
		);
		self.sync_get_authorize(
			graph,
			objects.into_iter().map(tg::Id::from),
			object_permissions,
		)
		.await?;

		let mut process_permissions = tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::empty(),
		);
		for permission in [
			tg::authorization::permission::process::Permission::Node,
			tg::authorization::permission::process::Permission::NodeCommand,
			tg::authorization::permission::process::Permission::NodeError,
			tg::authorization::permission::process::Permission::NodeLog,
			tg::authorization::permission::process::Permission::NodeOutput,
		] {
			process_permissions.insert(tg::authorization::permission::Set::from_permission(
				tg::authorization::Permission::Process(permission),
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
		nodes: &[tg::sync::PutNodeMessage],
	) -> tg::Result<BTreeMap<tg::tag::Id, Vec<tg::authorization::Permission>>> {
		let mut outputs = BTreeMap::new();
		let mut graph = graph.lock().unwrap();
		for node in nodes {
			let tg::sync::PutNodeMessage::Tag(message) = node else {
				continue;
			};
			if let Some(permissions) = self.sync_get_database_tag_permissions_from_token(message)? {
				outputs.insert(message.id.clone(), permissions);
				continue;
			}
			let (aspects, permissions) = if let Ok(id) =
				tg::object::Id::try_from(message.target.clone())
			{
				let aspects = vec![tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Node,
				)];
				let required = tg::authorization::permission::Set::from_permission(aspects[0]);
				let authorization = graph.get_object_local_authorization(&id, required);
				(aspects, authorization.permissions)
			} else if let Ok(id) = tg::process::Id::try_from(message.target.clone()) {
				let aspects = [
					tg::authorization::permission::process::Permission::Node,
					tg::authorization::permission::process::Permission::NodeCommand,
					tg::authorization::permission::process::Permission::NodeError,
					tg::authorization::permission::process::Permission::NodeLog,
					tg::authorization::permission::process::Permission::NodeOutput,
				]
				.into_iter()
				.map(tg::authorization::Permission::Process)
				.collect::<Vec<_>>();
				let mut required = tg::authorization::permission::Set::Process(
					tg::authorization::permission::process::Set::empty(),
				);
				for aspect in &aspects {
					required.insert(tg::authorization::permission::Set::from_permission(*aspect));
				}
				let authorization = graph.get_process_local_authorization(&id, required);
				(aspects, authorization.permissions)
			} else {
				return Err(tg::error!("invalid tag target"));
			};
			let permissions = if matches!(self.context.principal, tg::Principal::Root) {
				aspects
					.into_iter()
					.map(tg::authorization::Permission::subtree)
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

	fn sync_get_database_tag_permissions_from_token(
		&self,
		message: &tg::sync::PutNodeTagMessage,
	) -> tg::Result<Option<Vec<tg::authorization::Permission>>> {
		let Some(token) = &message.token else {
			return Ok(None);
		};
		if token.body.resource != message.target || !self.verify_token(token) {
			return Err(tg::error!("invalid tag target token"));
		}
		let valid =
			if message.target.kind().is_object() {
				token.body.permissions.iter().all(|permission| {
					matches!(permission, tg::authorization::Permission::Object(_))
				})
			} else if message.target.kind() == tg::id::Kind::Process {
				token.body.permissions.iter().all(|permission| {
					matches!(
						permission,
						tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::Node
								| tg::authorization::permission::process::Permission::NodeCommand
								| tg::authorization::permission::process::Permission::NodeError
								| tg::authorization::permission::process::Permission::NodeLog
								| tg::authorization::permission::process::Permission::NodeOutput
								| tg::authorization::permission::process::Permission::Subtree
								| tg::authorization::permission::process::Permission::SubtreeCommand
								| tg::authorization::permission::process::Permission::SubtreeError
								| tg::authorization::permission::process::Permission::SubtreeLog
								| tg::authorization::permission::process::Permission::SubtreeOutput
						)
					)
				})
			} else {
				false
			};
		if !valid {
			return Err(tg::error!("invalid tag target token permissions"));
		}
		let permissions = token.body.permissions.clone();

		Ok(Some(permissions))
	}

	async fn sync_get_database_namespace_with_transaction(
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
	) -> tg::Result<ControlFlow<Namespace, crate::database::Error>> {
		// Collect the relevant IDs and specifiers.
		let mut ids = BTreeSet::new();
		let mut specifiers = BTreeSet::new();
		for node in nodes {
			let id = Self::sync_get_database_node_id(node)?;
			let specifier = Self::sync_get_database_node_specifier(node)?;
			ids.insert(id.to_string());
			specifiers.insert(specifier.to_string());
			if let Some(parent) = specifier.parent() {
				specifiers.insert(parent.to_string());
			}
		}

		// Load the namespace in batches.
		let mut namespace = Namespace::default();
		let ids = ids.into_iter().collect::<Vec<_>>();
		match Self::sync_get_database_load_namespace_column_with_transaction(
			transaction,
			&mut namespace,
			"id",
			&ids,
		)
		.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let specifiers = specifiers.into_iter().collect::<Vec<_>>();
		match Self::sync_get_database_load_namespace_column_with_transaction(
			transaction,
			&mut namespace,
			"specifier",
			&specifiers,
		)
		.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(namespace))
	}

	async fn sync_get_database_load_namespace_column_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &mut Namespace,
		column: &str,
		values: &[String],
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}

		for values in values.chunks(SYNC_GET_DATABASE_BATCH_SIZE) {
			let p = transaction.p();
			let placeholders = (1..=values.len())
				.map(|index| format!("{p}{index}"))
				.collect::<Vec<_>>()
				.join(", ");
			let statement = formatdoc!(
				"
					select id, specifier
					from specifiers
					where {column} in ({placeholders});
				"
			);
			let params = values.iter().cloned().map(db::Value::from).collect();
			let result = transaction
				.query_all_into::<Row>(statement.into(), params)
				.await;
			let rows = crate::database::retry!(result, "failed to execute the statement");
			for row in rows {
				namespace.insert(row.id, row.specifier);
			}
		}

		Ok(ControlFlow::Break(()))
	}

	async fn sync_get_database_node_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		node: &tg::sync::PutNodeMessage,
		namespace: &mut Namespace,
		tag_accounts: &BTreeMap<tg::tag::Id, Option<tg::usage::Account>>,
		tag_permissions: &BTreeMap<tg::tag::Id, Vec<tg::authorization::Permission>>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<bool, crate::database::Error>> {
		match node {
			tg::sync::PutNodeMessage::Group(message) => {
				let created = match Self::sync_get_database_validate_node_with_transaction(
					transaction,
					namespace,
					&message.id.clone().into(),
					&message.name,
					message.parent.as_ref(),
					&message.specifier,
				)
				.await?
				{
					ControlFlow::Break(created) => created,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let p = transaction.p();
				let statement = formatdoc!(
					"
						insert into groups (id, name, parent)
						values ({p}1, {p}2, {p}3)
						on conflict (id) do update
						set name = excluded.name, parent = excluded.parent;
					"
				);
				let result = transaction
					.execute(
						statement.into(),
						db::params![
							message.id.to_string(),
							message.name.clone(),
							message.parent.as_ref().map(ToString::to_string)
						],
					)
					.await;
				crate::database::retry!(result, "failed to execute the statement");
				batch.items.push(tangram_index::batch::Item::PutGroup(
					tangram_index::group::put::Arg {
						id: message.id.clone(),
						parent: message.parent.clone(),
						specifier: message.specifier.clone(),
					},
				));

				Ok(ControlFlow::Break(created))
			},
			tg::sync::PutNodeMessage::Object(_)
			| tg::sync::PutNodeMessage::Process(_)
			| tg::sync::PutNodeMessage::Sandbox(_) => Err(tg::error!("invalid sync node kind")),
			tg::sync::PutNodeMessage::Organization(message) => {
				let created = match Self::sync_get_database_validate_node_with_transaction(
					transaction,
					namespace,
					&message.id.clone().into(),
					&message.name,
					None,
					&message.specifier,
				)
				.await?
				{
					ControlFlow::Break(created) => created,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let p = transaction.p();
				let statement = formatdoc!(
					"
						insert into organizations (id, name)
						values ({p}1, {p}2)
						on conflict (id) do update
						set name = excluded.name;
					"
				);
				let result = transaction
					.execute(
						statement.into(),
						db::params![message.id.to_string(), message.name.clone()],
					)
					.await;
				crate::database::retry!(result, "failed to execute the statement");
				batch
					.items
					.push(tangram_index::batch::Item::PutOrganization(
						tangram_index::organization::put::Arg {
							billing: None,
							id: message.id.clone(),
							specifier: message.specifier.clone(),
						},
					));

				Ok(ControlFlow::Break(created))
			},
			tg::sync::PutNodeMessage::Tag(message) => {
				let created = match Self::sync_get_database_validate_node_with_transaction(
					transaction,
					namespace,
					&message.id.clone().into(),
					&message.name,
					message.parent.as_ref(),
					&message.specifier,
				)
				.await?
				{
					ControlFlow::Break(created) => created,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let target = if let Ok(id) = tg::object::Id::try_from(message.target.clone()) {
					tg::Either::Left(id)
				} else if let Ok(id) = tg::process::Id::try_from(message.target.clone()) {
					tg::Either::Right(id)
				} else {
					return Err(tg::error!("invalid tag target"));
				};
				let target_string = target.to_string();
				let permissions = tag_permissions
					.get(&message.id)
					.ok_or_else(|| tg::error!("missing the tag permissions"))?;
				let permissions = serde_json::to_string(permissions)
					.map_err(|error| tg::error!(!error, "failed to serialize the permissions"))?;
				let p = transaction.p();
				let statement = formatdoc!(
					"
						insert into tags (id, name, parent, target, permissions)
						values ({p}1, {p}2, {p}3, {p}4, {p}5)
						on conflict (id) do update
						set name = excluded.name, parent = excluded.parent, target = excluded.target,
							permissions = case when tags.target = excluded.target then tags.permissions else excluded.permissions end;
					"
				);
				let result = transaction
					.execute(
						statement.into(),
						db::params![
							message.id.to_string(),
							message.name.clone(),
							message.parent.as_ref().map(ToString::to_string),
							target_string,
							permissions
						],
					)
					.await;
				crate::database::retry!(result, "failed to execute the statement");
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
				let result = transaction
					.query_one_into::<Row>(statement.into(), db::params![message.id.to_string()])
					.await;
				let row = crate::database::retry!(result, "failed to execute the statement");
				let permissions = serde_json::from_str(&row.permissions)
					.map_err(|error| tg::error!(!error, "failed to deserialize the permissions"))?;
				batch.items.push(tangram_index::batch::Item::PutTag(
					tangram_index::tag::put::Arg {
						account: tag_accounts.get(&message.id).cloned().flatten(),
						id: message.id.clone(),
						name: message.name.clone(),
						parent: message.parent.clone(),
						permissions,
						specifier: message.specifier.clone(),
						target,
					},
				));

				Ok(ControlFlow::Break(created))
			},
			tg::sync::PutNodeMessage::User(message) => {
				let created = match Self::sync_get_database_validate_node_with_transaction(
					transaction,
					namespace,
					&message.id.clone().into(),
					&message.name,
					None,
					&message.specifier,
				)
				.await?
				{
					ControlFlow::Break(created) => created,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				let p = transaction.p();
				let statement = formatdoc!(
					"
						insert into users (id, name)
						values ({p}1, {p}2)
						on conflict (id) do update
						set name = excluded.name;
					"
				);
				let result = transaction
					.execute(
						statement.into(),
						db::params![message.id.to_string(), message.name.clone()],
					)
					.await;
				crate::database::retry!(result, "failed to execute the statement");
				let statement = format!(r#"delete from user_emails where "user" = {p}1;"#);
				let result = transaction
					.execute(statement.into(), db::params![message.id.to_string()])
					.await;
				crate::database::retry!(result, "failed to execute the statement");
				for email in &message.emails {
					let statement = formatdoc!(
						r#"
							insert into user_emails ("user", email)
							values ({p}1, {p}2);
						"#
					);
					let result = transaction
						.execute(
							statement.into(),
							db::params![message.id.to_string(), email.clone()],
						)
						.await;
					crate::database::retry!(result, "failed to execute the statement");
				}
				batch.items.push(tangram_index::batch::Item::PutUser(
					tangram_index::user::put::Arg {
						billing: None,
						id: message.id.clone(),
						specifier: message.specifier.clone(),
					},
				));

				Ok(ControlFlow::Break(created))
			},
		}
	}

	async fn sync_get_database_validate_node_with_transaction(
		transaction: &Transaction<'_>,
		namespace: &mut Namespace,
		id: &tg::Id,
		name: &str,
		parent: Option<&tg::Id>,
		specifier: &tg::Specifier,
	) -> tg::Result<ControlFlow<bool, crate::database::Error>> {
		// Validate the ID and specifier.
		let by_id = namespace.ids.get(id);
		let by_specifier = namespace.specifiers.get(specifier);
		Self::sync_get_database_validate_id_and_specifier(id, specifier, by_id, by_specifier)?;
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
			let parent = namespace
				.specifiers
				.get(&parent_specifier)
				.cloned()
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
			match Self::insert_specifier_with_transaction(transaction, id, specifier).await? {
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			namespace.insert(id.clone(), specifier.clone());
		}

		Ok(ControlFlow::Break(created))
	}

	fn sync_get_database_node_depth(node: &tg::sync::PutNodeMessage) -> usize {
		match node {
			tg::sync::PutNodeMessage::Group(message) => message.specifier.components().count(),
			tg::sync::PutNodeMessage::Object(_)
			| tg::sync::PutNodeMessage::Process(_)
			| tg::sync::PutNodeMessage::Sandbox(_) => usize::MAX,
			tg::sync::PutNodeMessage::Organization(message) => {
				message.specifier.components().count()
			},
			tg::sync::PutNodeMessage::Tag(message) => message.specifier.components().count(),
			tg::sync::PutNodeMessage::User(message) => message.specifier.components().count(),
		}
	}

	fn sync_get_database_node_id(node: &tg::sync::PutNodeMessage) -> tg::Result<tg::Id> {
		let id = match node {
			tg::sync::PutNodeMessage::Group(message) => message.id.clone().into(),
			tg::sync::PutNodeMessage::Object(_) | tg::sync::PutNodeMessage::Process(_) => {
				return Err(tg::error!("invalid sync node kind"));
			},
			tg::sync::PutNodeMessage::Organization(message) => message.id.clone().into(),
			tg::sync::PutNodeMessage::Sandbox(_) => {
				return Err(tg::error!("invalid sync node kind"));
			},
			tg::sync::PutNodeMessage::Tag(message) => message.id.clone().into(),
			tg::sync::PutNodeMessage::User(message) => message.id.clone().into(),
		};

		Ok(id)
	}

	fn sync_get_database_node_specifier(
		node: &tg::sync::PutNodeMessage,
	) -> tg::Result<&tg::Specifier> {
		let specifier = match node {
			tg::sync::PutNodeMessage::Group(message) => &message.specifier,
			tg::sync::PutNodeMessage::Object(_)
			| tg::sync::PutNodeMessage::Process(_)
			| tg::sync::PutNodeMessage::Sandbox(_) => {
				return Err(tg::error!("invalid sync node kind"));
			},
			tg::sync::PutNodeMessage::Organization(message) => &message.specifier,
			tg::sync::PutNodeMessage::Tag(message) => &message.specifier,
			tg::sync::PutNodeMessage::User(message) => &message.specifier,
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
