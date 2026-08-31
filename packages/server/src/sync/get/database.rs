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

#[cfg(test)]
mod tests;

#[derive(Clone, Default, Eq, PartialEq)]
struct Specifiers {
	ids_by_specifier: BTreeMap<tg::Specifier, tg::Id>,
	specifiers_by_id: BTreeMap<tg::Id, tg::Specifier>,
}

impl Specifiers {
	fn insert(&mut self, id: tg::Id, specifier: tg::Specifier) {
		self.ids_by_specifier.insert(specifier.clone(), id.clone());
		self.specifiers_by_id.insert(id, specifier);
	}

	fn remove_id(&mut self, id: &tg::Id) {
		if let Some(specifier) = self.specifiers_by_id.remove(id) {
			self.ids_by_specifier.remove(&specifier);
		}
	}
}

impl Session {
	pub(super) async fn sync_get_database(
		&self,
		graph: &Arc<Mutex<Graph>>,
		force: bool,
	) -> tg::Result<()> {
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
			self.sync_get_database_to_primary_region(nodes, force)
				.await?;

			return Ok(());
		}

		// Update the tag target permissions in the graph.
		self.sync_get_database_update_tag_target_permissions(graph, &nodes)
			.await?;
		let tag_permissions = self.sync_get_database_tag_permissions(graph, &nodes)?;
		let touched_at = self.server.clock.unix_timestamp()?;

		// Sort the nodes so that parents are written before their children.
		nodes.sort_by_key(Self::sync_get_database_node_depth);

		let options = tangram_futures::retry::Options::default();
		let session = self.clone();
		let _invalidated_specifiers = tangram_futures::retry(&options, || {
			let nodes = nodes.clone();
			let session = session.clone();
			let tag_permissions = tag_permissions.clone();
			async move {
				match session
					.sync_get_database_attempt(&nodes, force, &tag_permissions, touched_at)
					.await?
				{
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(()) => Ok(ControlFlow::Continue(tg::error!(
						"the named node ids kept changing while authorizing the write"
					))),
				}
			}
		})
		.await?;
		self.server
			.spawn_publish_database_index_outbox_notification_task();
		self.checkout_index_barrier().await?;

		Ok(())
	}

	async fn sync_get_database_attempt(
		&self,
		nodes: &[tg::sync::PutNodeMessage],
		force: bool,
		tag_permissions: &BTreeMap<tg::tag::Id, Vec<tg::authorization::Permission>>,
		touched_at: i64,
	) -> tg::Result<ControlFlow<BTreeSet<tg::Specifier>>> {
		let authorized_state = self.sync_get_database_authorize(nodes, force).await?;
		crate::checkpoint!(self.server, "sync.get.database.authorized").await;
		let session = self.clone();
		let output = self
			.server
			.database
			.run(|transaction| {
				let authorized_state = authorized_state.clone();
				let nodes = nodes.to_vec();
				let session = session.clone();
				let tag_permissions = tag_permissions.clone();
				async move {
					session
						.sync_get_database_with_transaction(
							transaction,
							&nodes,
							force,
							&authorized_state,
							&tag_permissions,
							touched_at,
						)
						.await
				}
				.boxed()
			})
			.await?;

		Ok(output)
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
		force: bool,
	) -> tg::Result<()> {
		// Start a sync to the primary region.
		let primary_region_arg = tg::sync::Arg {
			ancestors: tg::node::AncestorsPull::Missing,
			force,
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
				Ok(tg::sync::Message::Get(tg::sync::GetMessage::End)),
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
					tg::sync::GetMessage::Node(_) | tg::sync::GetMessage::Available(_),
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
		force: bool,
		authorized_state: &(
			Specifiers,
			BTreeMap<tg::Id, tg::Specifier>,
			BTreeMap<tg::tag::Id, tg::Id>,
		),
		tag_permissions: &BTreeMap<tg::tag::Id, Vec<tg::authorization::Permission>>,
		touched_at: i64,
	) -> tg::Result<ControlFlow<ControlFlow<BTreeSet<tg::Specifier>>, crate::database::Error>> {
		let (specifiers, replacement_roots, tag_targets) = authorized_state;
		let batch_size = self.server.config.sync.get.database.batch_size;
		let invalidated_specifiers = nodes
			.iter()
			.filter_map(|node| match node {
				tg::sync::PutNodeMessage::Tag(message) => Some(message.specifier.clone()),
				_ => None,
			})
			.collect::<BTreeSet<_>>();
		let (mut stored_specifiers, actual_tag_targets) =
			match Self::sync_get_database_specifiers_and_tag_targets_with_transaction(
				transaction,
				nodes,
				batch_size,
			)
			.await?
			{
				ControlFlow::Break(output) => output,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		if &stored_specifiers != specifiers || &actual_tag_targets != tag_targets {
			return Ok(ControlFlow::Break(ControlFlow::Continue(())));
		}

		// Recheck and delete the authorized replacement subtrees.
		let actual_replacement_roots = if force {
			Self::sync_get_database_replacement_roots(&stored_specifiers, nodes)?
		} else {
			BTreeMap::new()
		};
		if &actual_replacement_roots != replacement_roots {
			return Ok(ControlFlow::Break(ControlFlow::Continue(())));
		}
		let mut batch = tangram_index::batch::Arg::default();
		if !replacement_roots.is_empty() {
			let roots = replacement_roots.keys().cloned().collect::<Vec<_>>();
			let replaced_ids_and_specifiers = match Self::collect_named_subtrees_with_transaction(
				transaction,
				&roots,
				batch_size,
			)
			.await?
			{
				ControlFlow::Break(ids_and_specifiers) => ids_and_specifiers,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			match self
				.delete_named_nodes_with_transaction(
					transaction,
					&replaced_ids_and_specifiers,
					&mut batch,
					batch_size,
				)
				.await?
			{
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			for (id, _) in replaced_ids_and_specifiers {
				stored_specifiers.remove_id(&id);
			}
		}

		// Validate the incoming specifiers and insert their new entries.
		let created = Self::sync_get_database_validate_nodes(&mut stored_specifiers, nodes)?;
		let specifiers = nodes
			.iter()
			.map(|node| {
				let id = Self::sync_get_database_node_id(node)?;
				let specifier = Self::sync_get_database_node_specifier(node)?;
				Ok((id, specifier.clone()))
			})
			.collect::<tg::Result<Vec<_>>>()?
			.into_iter()
			.filter(|(id, _)| created.contains(id))
			.collect::<Vec<_>>();
		match Self::sync_get_database_insert_specifiers_with_transaction(
			transaction,
			&specifiers,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		// Pipeline the independent named-node writes and accounting lookup.
		let (groups, organizations, tag_accounts, tags, users) = futures::join!(
			Self::sync_get_database_put_groups_with_transaction(transaction, nodes, batch_size),
			Self::sync_get_database_put_organizations_with_transaction(
				transaction,
				nodes,
				batch_size,
			),
			self.sync_get_database_tag_accounts_with_transaction(
				transaction,
				nodes,
				&stored_specifiers,
			),
			Self::sync_get_database_put_tags_with_transaction(
				transaction,
				nodes,
				tag_permissions,
				batch_size,
			),
			Self::sync_get_database_put_users_with_transaction(transaction, nodes, batch_size),
		);
		match groups? {
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		match organizations? {
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let tag_accounts = match tag_accounts? {
			ControlFlow::Break(tag_accounts) => tag_accounts,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let tag_permissions = match tags? {
			ControlFlow::Break(tag_permissions) => tag_permissions,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		match users? {
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		// Create the index batch.
		let put_batch = self.sync_get_database_batch(
			nodes,
			&created,
			&tag_accounts,
			&tag_permissions,
			touched_at,
		)?;
		batch.items.extend(put_batch.items);
		match self
			.server
			.enqueue_database_index_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(ControlFlow::Break(
			invalidated_specifiers,
		)))
	}

	async fn sync_get_database_authorize(
		&self,
		nodes: &[tg::sync::PutNodeMessage],
		force: bool,
	) -> tg::Result<(
		Specifiers,
		BTreeMap<tg::Id, tg::Specifier>,
		BTreeMap<tg::tag::Id, tg::Id>,
	)> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}

		// Read the existing IDs, specifiers, and tag targets from the database.
		let batch_size = self.server.config.sync.get.database.batch_size;
		let nodes = nodes.to_vec();
		let connection_options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			..Default::default()
		};
		let (existing_specifiers, tag_targets) = self
			.server
			.database
			.run_with_options(connection_options, |transaction| {
				let nodes = nodes.clone();
				async move {
					Self::sync_get_database_specifiers_and_tag_targets_with_transaction(
						transaction,
						&nodes,
						batch_size,
					)
					.await
				}
				.boxed()
			})
			.await?;

		// Validate the conflicts and collect all required authorizations.
		let replacement_roots = if force {
			Self::sync_get_database_replacement_roots(&existing_specifiers, &nodes)?
		} else {
			BTreeMap::new()
		};
		let mut authorizations = Vec::new();
		for node in &nodes {
			let id = Self::sync_get_database_node_id(node)?;
			let specifier = Self::sync_get_database_node_specifier(node)?;
			let by_id = existing_specifiers.specifiers_by_id.get(&id);
			let by_specifier = existing_specifiers.ids_by_specifier.get(specifier);
			if !force {
				Self::sync_get_database_validate_id_and_specifier(
					&id,
					specifier,
					by_id,
					by_specifier,
				)?;
				if let tg::sync::PutNodeMessage::Tag(message) = node
					&& by_id == Some(specifier)
					&& tag_targets
						.get(&message.id)
						.is_some_and(|target| target != &message.target)
				{
					return Err(tg::error!("the tag already has a different target"));
				}
			}
			let cross_kind_replacement = by_specifier
				.is_some_and(|existing| existing != &id && existing.kind() != id.kind());
			if cross_kind_replacement {
				if let Some(parent) = specifier.parent()
					&& let Some(parent) = existing_specifiers.ids_by_specifier.get(&parent)
				{
					let permission = Self::write_permission_for_resource(parent)?;
					let permissions =
						tg::authorization::permission::Set::from_permission(permission);
					let resource = tg::Selector::<tg::Id>::Id(parent.clone());
					authorizations.push((resource, false, permissions));
				}
				continue;
			}
			let allow_unclaimed = by_specifier.is_none()
				&& specifier.ancestors().all(|specifier| {
					!existing_specifiers
						.ids_by_specifier
						.contains_key(&specifier)
				});
			let permission = Self::write_permission_for_resource(&id)?;
			let permissions = tg::authorization::permission::Set::from_permission(permission);
			let resource = tg::Selector::<tg::Id>::Specifier(specifier.clone());
			authorizations.push((resource, allow_unclaimed, permissions));
		}
		for id in replacement_roots.keys() {
			let permission = Self::delete_permission_for_named_node(id)?;
			let permissions = tg::authorization::permission::Set::from_permission(permission);
			let resource = tg::Selector::<tg::Id>::Id(id.clone());
			authorizations.push((resource, false, permissions));
		}

		// Authorize the writes.
		for authorizations in authorizations.chunks(batch_size) {
			let args = authorizations
				.iter()
				.map(|(resource, _, permissions)| (resource.clone(), *permissions))
				.collect::<Vec<_>>();
			let outputs = self.authorize_batch(args).await?;
			for ((_, allow_unclaimed, permissions), output) in
				std::iter::zip(authorizations, outputs)
			{
				let authorized = match output {
					Some(output) => output.contains(*permissions),
					None => *allow_unclaimed,
				};
				if !authorized {
					return Err(tg::error!("unauthorized"));
				}
			}
		}

		Ok((existing_specifiers, replacement_roots, tag_targets))
	}

	async fn sync_get_database_specifiers_and_tag_targets_with_transaction(
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
		batch_size: usize,
	) -> tg::Result<ControlFlow<(Specifiers, BTreeMap<tg::tag::Id, tg::Id>), crate::database::Error>>
	{
		let specifiers = match Self::sync_get_database_specifiers_with_transaction(
			transaction,
			nodes,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(specifiers) => specifiers,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let tag_targets = match Self::sync_get_database_tag_targets_with_transaction(
			transaction,
			nodes,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(tag_targets) => tag_targets,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};

		Ok(ControlFlow::Break((specifiers, tag_targets)))
	}

	async fn sync_get_database_tag_targets_with_transaction(
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
		batch_size: usize,
	) -> tg::Result<ControlFlow<BTreeMap<tg::tag::Id, tg::Id>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::tag::Id,
			#[tangram_database(as = "db::value::FromStr")]
			target: tg::Id,
		}

		let ids = nodes
			.iter()
			.filter_map(|node| {
				let tg::sync::PutNodeMessage::Tag(message) = node else {
					return None;
				};

				Some(message.id.clone())
			})
			.collect::<BTreeSet<_>>()
			.into_iter()
			.collect::<Vec<_>>();
		let mut tag_targets = BTreeMap::new();
		for ids in ids.chunks(batch_size) {
			let p = transaction.p();
			let placeholders = (1..=ids.len())
				.map(|index| format!("{p}{index}"))
				.collect::<Vec<_>>()
				.join(", ");
			let statement = format!("select id, target from tags where id in ({placeholders});");
			let params = ids
				.iter()
				.map(ToString::to_string)
				.map(db::Value::from)
				.collect();
			let result = transaction
				.query_all_into::<Row>(statement.into(), params)
				.await;
			let rows = crate::database::retry!(result, "failed to read the tag targets");
			for row in rows {
				tag_targets.insert(row.id, row.target);
			}
		}

		Ok(ControlFlow::Break(tag_targets))
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

	async fn sync_get_database_specifiers_with_transaction(
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
		batch_size: usize,
	) -> tg::Result<ControlFlow<Specifiers, crate::database::Error>> {
		// Collect the relevant IDs and specifiers.
		let mut ids = BTreeSet::new();
		let mut specifiers = BTreeSet::new();
		for node in nodes {
			let id = Self::sync_get_database_node_id(node)?;
			let specifier = Self::sync_get_database_node_specifier(node)?;
			ids.insert(id.to_string());
			specifiers.insert(specifier.to_string());
			specifiers.extend(specifier.ancestors().map(|specifier| specifier.to_string()));
		}

		// Load the specifiers in batches.
		let mut stored_specifiers = Specifiers::default();
		let ids = ids.into_iter().collect::<Vec<_>>();
		match Self::sync_get_database_load_specifiers_column_with_transaction(
			transaction,
			&mut stored_specifiers,
			"id",
			&ids,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		let specifiers = specifiers.into_iter().collect::<Vec<_>>();
		match Self::sync_get_database_load_specifiers_column_with_transaction(
			transaction,
			&mut stored_specifiers,
			"specifier",
			&specifiers,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(stored_specifiers))
	}

	async fn sync_get_database_load_specifiers_column_with_transaction(
		transaction: &Transaction<'_>,
		stored_specifiers: &mut Specifiers,
		column: &str,
		values: &[String],
		batch_size: usize,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}

		for values in values.chunks(batch_size) {
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
				stored_specifiers.insert(row.id, row.specifier);
			}
		}

		Ok(ControlFlow::Break(()))
	}

	fn sync_get_database_validate_nodes(
		stored_specifiers: &mut Specifiers,
		nodes: &[tg::sync::PutNodeMessage],
	) -> tg::Result<BTreeSet<tg::Id>> {
		let mut created = BTreeSet::new();
		for node in nodes {
			let (id, name, parent, specifier) = match node {
				tg::sync::PutNodeMessage::Group(message) => (
					message.id.clone().into(),
					message.name.as_str(),
					message.parent.as_ref(),
					&message.specifier,
				),
				tg::sync::PutNodeMessage::Object(_)
				| tg::sync::PutNodeMessage::Process(_)
				| tg::sync::PutNodeMessage::Sandbox(_) => {
					return Err(tg::error!("invalid sync node kind"));
				},
				tg::sync::PutNodeMessage::Organization(message) => (
					message.id.clone().into(),
					message.name.as_str(),
					None,
					&message.specifier,
				),
				tg::sync::PutNodeMessage::Tag(message) => (
					message.id.clone().into(),
					message.name.as_str(),
					message.parent.as_ref(),
					&message.specifier,
				),
				tg::sync::PutNodeMessage::User(message) => (
					message.id.clone().into(),
					message.name.as_str(),
					None,
					&message.specifier,
				),
			};
			if Self::sync_get_database_validate_node(
				stored_specifiers,
				&id,
				name,
				parent,
				specifier,
			)? {
				created.insert(id);
			}
		}

		Ok(created)
	}

	fn sync_get_database_validate_node(
		stored_specifiers: &mut Specifiers,
		id: &tg::Id,
		name: &str,
		parent: Option<&tg::Id>,
		specifier: &tg::Specifier,
	) -> tg::Result<bool> {
		// Validate the ID and specifier.
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let by_id = stored_specifiers.specifiers_by_id.get(id);
		let by_specifier = stored_specifiers.ids_by_specifier.get(specifier);
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
			let parent = stored_specifiers
				.ids_by_specifier
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

		// Record the new specifier.
		if created {
			stored_specifiers.insert(id.clone(), specifier.clone());
		}

		Ok(created)
	}

	async fn sync_get_database_insert_specifiers_with_transaction(
		transaction: &Transaction<'_>,
		specifiers: &[(tg::Id, tg::Specifier)],
		batch_size: usize,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		for specifiers in specifiers.chunks(batch_size) {
			let p = transaction.p();
			let values = Self::sync_get_database_placeholders(p, specifiers.len(), 2);
			let statement = formatdoc!(
				"
					insert into specifiers (id, specifier)
					values {values};
				"
			);
			let params = specifiers
				.iter()
				.flat_map(|(id, specifier)| db::params![id.to_string(), specifier.to_string()])
				.collect();
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to insert the specifiers");
		}

		Ok(ControlFlow::Break(()))
	}

	async fn sync_get_database_put_groups_with_transaction(
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
		batch_size: usize,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let groups = nodes
			.iter()
			.filter_map(|node| {
				let tg::sync::PutNodeMessage::Group(message) = node else {
					return None;
				};

				Some(message)
			})
			.collect::<Vec<_>>();
		for groups in groups.chunks(batch_size) {
			let p = transaction.p();
			let values = Self::sync_get_database_placeholders(p, groups.len(), 3);
			let statement = formatdoc!(
				"
					insert into groups (id, name, parent)
					values {values}
					on conflict (id) do update
					set name = excluded.name, parent = excluded.parent;
				"
			);
			let params = groups
				.iter()
				.flat_map(|message| {
					db::params![
						message.id.to_string(),
						message.name.clone(),
						message.parent.as_ref().map(ToString::to_string)
					]
				})
				.collect();
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to put the groups");
		}

		Ok(ControlFlow::Break(()))
	}

	async fn sync_get_database_put_organizations_with_transaction(
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
		batch_size: usize,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let organizations = nodes
			.iter()
			.filter_map(|node| {
				let tg::sync::PutNodeMessage::Organization(message) = node else {
					return None;
				};

				Some(message)
			})
			.collect::<Vec<_>>();
		for organizations in organizations.chunks(batch_size) {
			let p = transaction.p();
			let values = Self::sync_get_database_placeholders(p, organizations.len(), 2);
			let statement = formatdoc!(
				"
					insert into organizations (id, name)
					values {values}
					on conflict (id) do update
					set name = excluded.name;
				"
			);
			let params = organizations
				.iter()
				.flat_map(|message| db::params![message.id.to_string(), message.name.clone()])
				.collect();
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to put the organizations");
		}

		Ok(ControlFlow::Break(()))
	}

	async fn sync_get_database_put_tags_with_transaction(
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
		tag_permissions: &BTreeMap<tg::tag::Id, Vec<tg::authorization::Permission>>,
		batch_size: usize,
	) -> tg::Result<
		ControlFlow<
			BTreeMap<tg::tag::Id, Vec<tg::authorization::Permission>>,
			crate::database::Error,
		>,
	> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::tag::Id,
			permissions: String,
		}

		let tags = nodes
			.iter()
			.filter_map(|node| {
				let tg::sync::PutNodeMessage::Tag(message) = node else {
					return None;
				};

				Some(message)
			})
			.collect::<Vec<_>>();
		let mut outputs = BTreeMap::new();
		for tags in tags.chunks(batch_size) {
			let p = transaction.p();
			let values = Self::sync_get_database_placeholders(p, tags.len(), 5);
			let statement = formatdoc!(
				"
					insert into tags (id, name, parent, target, permissions)
					values {values}
					on conflict (id) do update
					set name = excluded.name, parent = excluded.parent, target = excluded.target,
						permissions = case when tags.target = excluded.target then tags.permissions else excluded.permissions end
					returning id, permissions;
				"
			);
			let mut params = Vec::with_capacity(tags.len() * 5);
			for message in tags {
				let target = Self::sync_get_database_tag_target(&message.target)?;
				let permissions = tag_permissions
					.get(&message.id)
					.ok_or_else(|| tg::error!("missing the tag permissions"))?;
				let permissions = serde_json::to_string(permissions)
					.map_err(|error| tg::error!(!error, "failed to serialize the permissions"))?;
				params.extend(db::params![
					message.id.to_string(),
					message.name.clone(),
					message.parent.as_ref().map(ToString::to_string),
					target.to_string(),
					permissions
				]);
			}
			let result = transaction
				.query_all_into::<Row>(statement.into(), params)
				.await;
			let rows = crate::database::retry!(result, "failed to put the tags");
			for row in rows {
				let permissions = serde_json::from_str(&row.permissions).map_err(|error| {
					tg::error!(!error, "failed to deserialize the tag permissions")
				})?;
				outputs.insert(row.id, permissions);
			}
		}

		Ok(ControlFlow::Break(outputs))
	}

	async fn sync_get_database_put_users_with_transaction(
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
		batch_size: usize,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let users = nodes
			.iter()
			.filter_map(|node| {
				let tg::sync::PutNodeMessage::User(message) = node else {
					return None;
				};

				Some(message)
			})
			.collect::<Vec<_>>();
		for users in users.chunks(batch_size) {
			let p = transaction.p();
			let values = Self::sync_get_database_placeholders(p, users.len(), 2);
			let statement = formatdoc!(
				"
					insert into users (id, name)
					values {values}
					on conflict (id) do update
					set name = excluded.name;
				"
			);
			let params = users
				.iter()
				.flat_map(|message| db::params![message.id.to_string(), message.name.clone()])
				.collect();
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to put the users");
		}
		for users in users.chunks(batch_size) {
			let p = transaction.p();
			let placeholders = Self::sync_get_database_placeholders(p, users.len(), 1);
			let statement = format!(r#"delete from user_emails where "user" in ({placeholders});"#);
			let params = users
				.iter()
				.map(|message| db::Value::from(message.id.to_string()))
				.collect();
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to delete the user emails");
		}
		let emails = users
			.iter()
			.flat_map(|message| {
				message
					.emails
					.iter()
					.map(|email| (message.id.to_string(), email.clone()))
			})
			.collect::<Vec<_>>();
		for emails in emails.chunks(batch_size) {
			let p = transaction.p();
			let values = Self::sync_get_database_placeholders(p, emails.len(), 2);
			let statement = formatdoc!(
				r#"
					insert into user_emails ("user", email)
					values {values};
				"#
			);
			let params = emails
				.iter()
				.flat_map(|(user, email)| db::params![user.clone(), email.clone()])
				.collect();
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to insert the user emails");
		}

		Ok(ControlFlow::Break(()))
	}

	async fn sync_get_database_tag_accounts_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
		stored_specifiers: &Specifiers,
	) -> tg::Result<
		ControlFlow<BTreeMap<tg::tag::Id, Option<tg::usage::Account>>, crate::database::Error>,
	> {
		let mut accounts = BTreeMap::new();
		if !self.server.config.usage.enabled {
			return Ok(ControlFlow::Break(accounts));
		}
		let tags = nodes
			.iter()
			.filter_map(|node| {
				let tg::sync::PutNodeMessage::Tag(message) = node else {
					return None;
				};

				Some(message)
			})
			.collect::<Vec<_>>();
		let roots = tags
			.iter()
			.map(|message| {
				message
					.specifier
					.prefixes()
					.next()
					.expect("a specifier should have a component")
			})
			.collect::<BTreeSet<_>>();
		let fallback_required = roots.iter().any(|root| {
			!matches!(
				stored_specifiers
					.ids_by_specifier
					.get(root)
					.map(tg::Id::kind),
				Some(tg::id::Kind::Organization | tg::id::Kind::User)
			)
		});
		let fallback = if fallback_required {
			match self
				.usage_account_with_transaction(transaction, &self.context.principal)
				.await?
			{
				ControlFlow::Break(account) => account,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		} else {
			None
		};
		let root_accounts = roots
			.into_iter()
			.map(|root| {
				let account = match stored_specifiers.ids_by_specifier.get(&root) {
					Some(id) if id.kind() == tg::id::Kind::Organization => {
						Some(tg::usage::Account::Organization(id.clone().try_into()?))
					},
					Some(id) if id.kind() == tg::id::Kind::User => {
						Some(tg::usage::Account::User(id.clone().try_into()?))
					},
					Some(_) | None => fallback.clone(),
				};

				Ok((root, account))
			})
			.collect::<tg::Result<BTreeMap<_, _>>>()?;
		for message in tags {
			let root = message
				.specifier
				.prefixes()
				.next()
				.expect("a specifier should have a component");
			let account = root_accounts
				.get(&root)
				.cloned()
				.ok_or_else(|| tg::error!("missing the tag account"))?;
			accounts.insert(message.id.clone(), account);
		}

		Ok(ControlFlow::Break(accounts))
	}

	fn sync_get_database_batch(
		&self,
		nodes: &[tg::sync::PutNodeMessage],
		created: &BTreeSet<tg::Id>,
		tag_accounts: &BTreeMap<tg::tag::Id, Option<tg::usage::Account>>,
		tag_permissions: &BTreeMap<tg::tag::Id, Vec<tg::authorization::Permission>>,
		touched_at: i64,
	) -> tg::Result<tangram_index::batch::Arg> {
		let mut batch = tangram_index::batch::Arg::default();
		for node in nodes {
			let id = Self::sync_get_database_node_id(node)?;
			let item = match node {
				tg::sync::PutNodeMessage::Group(message) => {
					tangram_index::batch::Item::PutGroup(tangram_index::group::put::Arg {
						id: message.id.clone(),
						parent: message.parent.clone(),
						specifier: message.specifier.clone(),
					})
				},
				tg::sync::PutNodeMessage::Object(_)
				| tg::sync::PutNodeMessage::Process(_)
				| tg::sync::PutNodeMessage::Sandbox(_) => {
					return Err(tg::error!("invalid sync node kind"));
				},
				tg::sync::PutNodeMessage::Organization(message) => {
					tangram_index::batch::Item::PutOrganization(
						tangram_index::organization::put::Arg {
							billing: None,
							id: message.id.clone(),
							specifier: message.specifier.clone(),
						},
					)
				},
				tg::sync::PutNodeMessage::Tag(message) => {
					let permissions = tag_permissions
						.get(&message.id)
						.cloned()
						.ok_or_else(|| tg::error!("missing the tag permissions"))?;
					let target = Self::sync_get_database_tag_target(&message.target)?;
					tangram_index::batch::Item::PutTag(tangram_index::tag::put::Arg {
						account: tag_accounts.get(&message.id).cloned().flatten(),
						id: message.id.clone(),
						name: message.name.clone(),
						parent: message.parent.clone(),
						permissions,
						specifier: message.specifier.clone(),
						target,
					})
				},
				tg::sync::PutNodeMessage::User(message) => {
					tangram_index::batch::Item::PutUser(tangram_index::user::put::Arg {
						billing: None,
						id: message.id.clone(),
						specifier: message.specifier.clone(),
					})
				},
			};
			batch.items.push(item);
			if created.contains(&id)
				&& let Some(arg) = self.sync_get_create_implicit_grant(&id)?
			{
				batch.items.push(tangram_index::batch::Item::PutGrant(arg));
			}
		}
		for message in nodes.iter().filter_map(|node| {
			let tg::sync::PutNodeMessage::Tag(message) = node else {
				return None;
			};

			Some(message)
		}) {
			let Some(account) = tag_accounts.get(&message.id).cloned().flatten() else {
				continue;
			};
			let permissions = tag_permissions
				.get(&message.id)
				.ok_or_else(|| tg::error!("missing the tag permissions"))?;
			if !Self::tag_target_permissions_grant_access(permissions) {
				continue;
			}
			let target = Self::sync_get_database_tag_target(&message.target)?;
			let item = match target {
				tg::Either::Left(object) => tangram_index::batch::Item::PutAccountObject(
					tangram_index::usage::storage::put::ObjectArg {
						account,
						object,
						touched_at,
					},
				),
				tg::Either::Right(process) => tangram_index::batch::Item::PutAccountProcess(
					tangram_index::usage::storage::put::ProcessArg {
						account,
						process,
						touched_at,
					},
				),
			};
			batch.items.push(item);
		}

		Ok(batch)
	}

	fn sync_get_database_tag_target(
		target: &tg::Id,
	) -> tg::Result<tg::Either<tg::object::Id, tg::process::Id>> {
		let target = if let Ok(id) = tg::object::Id::try_from(target.clone()) {
			tg::Either::Left(id)
		} else if let Ok(id) = tg::process::Id::try_from(target.clone()) {
			tg::Either::Right(id)
		} else {
			return Err(tg::error!("invalid tag target"));
		};

		Ok(target)
	}

	fn sync_get_database_placeholders(p: &str, rows: usize, columns: usize) -> String {
		(0..rows)
			.map(|row| {
				let offset = row * columns;
				let values = (1..=columns)
					.map(|column| format!("{p}{}", offset + column))
					.collect::<Vec<_>>()
					.join(", ");
				format!("({values})")
			})
			.collect::<Vec<_>>()
			.join(", ")
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

	fn sync_get_database_replacement_roots(
		stored_specifiers: &Specifiers,
		nodes: &[tg::sync::PutNodeMessage],
	) -> tg::Result<BTreeMap<tg::Id, tg::Specifier>> {
		let mut candidates = BTreeMap::new();
		for node in nodes {
			let id = Self::sync_get_database_node_id(node)?;
			let specifier = Self::sync_get_database_node_specifier(node)?;
			if let Some(existing_specifier) = stored_specifiers.specifiers_by_id.get(&id)
				&& existing_specifier != specifier
			{
				candidates.insert(id.clone(), existing_specifier.clone());
			}
			if let Some(existing_id) = stored_specifiers.ids_by_specifier.get(specifier)
				&& existing_id != &id
			{
				candidates.insert(existing_id.clone(), specifier.clone());
			}
		}
		let mut candidates = candidates.into_iter().collect::<Vec<_>>();
		candidates.sort_by(|(_, a), (_, b)| {
			a.components()
				.count()
				.cmp(&b.components().count())
				.then_with(|| a.cmp(b))
		});
		let mut replacement_roots = BTreeMap::new();
		for (id, specifier) in candidates {
			let covered = replacement_roots.values().any(|root| {
				root == &specifier || specifier.ancestors().any(|ancestor| &ancestor == root)
			});
			if !covered {
				replacement_roots.insert(id, specifier);
			}
		}

		Ok(replacement_roots)
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
