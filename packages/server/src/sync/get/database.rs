use {
	crate::{Session, database::Transaction, sync::graph::Graph},
	futures::{FutureExt as _, TryStreamExt as _},
	indoc::formatdoc,
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::stream::TryExt as _,
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

	fn remove_ids(&mut self, ids: &[tg::Id]) {
		for id in ids {
			let Some(specifier) = self.ids.remove(id) else {
				continue;
			};
			self.specifiers.remove(&specifier);
		}
	}
}

impl Session {
	pub(super) async fn sync_get_database(&self, graph: &Arc<Mutex<Graph>>) -> tg::Result<()> {
		// Get the staged nodes.
		let (mut nodes, replacement_ids) = {
			let graph = graph.lock().unwrap();
			let nodes = graph
				.local_messages()
				.into_iter()
				.filter(|node| !matches!(node, tg::sync::PutNodeMessage::Sandbox(_)))
				.collect::<Vec<_>>();
			let replacement_ids = graph.local_replacements().clone();
			(nodes, replacement_ids)
		};
		if nodes.is_empty() {
			return Ok(());
		}

		// Authorize the writes.
		self.sync_get_database_authorize(&nodes, &replacement_ids)
			.await?;

		// Finalize the tag node permissions in the graph.
		self.sync_get_database_update_tag_target_permissions(graph, &nodes)
			.await?;
		let tag_permissions = self.sync_get_database_tag_permissions(graph, &nodes)?;
		let touched_at = self.server.clock.unix_timestamp()?;

		// Sort the nodes so that parents are written before their children.
		nodes.sort_by_key(Self::sync_get_database_node_depth);

		// Write all of the nodes and enqueue their index mutations atomically.
		let session = self.clone();
		let invalidated_specifiers = self
			.server
			.run_database_outbox_transaction(|transaction, database_outbox_partition| {
				let nodes = nodes.clone();
				let replacement_ids = replacement_ids.clone();
				let session = session.clone();
				let tag_permissions = tag_permissions.clone();
				async move {
					session
						.sync_get_database_with_transaction(
							transaction,
							&nodes,
							&replacement_ids,
							&tag_permissions,
							touched_at,
							database_outbox_partition,
						)
						.await
				}
				.boxed()
			})
			.await?;
		let invalidated_specifiers = invalidated_specifiers.into_iter().collect::<Vec<_>>();
		self.invalidate_tag_cache_entries(&invalidated_specifiers)
			.await?;

		Ok(())
	}

	async fn sync_get_database_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
		replacement_ids: &std::collections::HashSet<tg::Id, fnv::FnvBuildHasher>,
		tag_permissions: &BTreeMap<tg::tag::Id, Vec<tg::authorization::Permission>>,
		touched_at: i64,
		database_outbox_partition: u64,
	) -> tg::Result<ControlFlow<BTreeSet<tg::Specifier>, crate::database::Error>> {
		let mut batch = tangram_index::batch::Arg::default();
		let mut tag_accounts = BTreeMap::new();
		let mut invalidated_specifiers = nodes
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
		let replaced_specifiers = match self
			.sync_get_database_replace_nodes_with_transaction(
				transaction,
				nodes,
				&mut namespace,
				replacement_ids,
				&mut batch,
			)
			.await?
		{
			ControlFlow::Break(specifiers) => specifiers,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		invalidated_specifiers.extend(replaced_specifiers);
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
			.enqueue_database_outbox_with_transaction(
				transaction,
				database_outbox_partition,
				&batch,
			)
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
		replacement_ids: &std::collections::HashSet<tg::Id, fnv::FnvBuildHasher>,
	) -> tg::Result<()> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}

		// Inspect the local namespace after waiting for pending mutations when replacement is possible.
		let ids = nodes
			.iter()
			.map(Self::sync_get_database_node_id)
			.collect::<tg::Result<Vec<_>>>()?;
		let specifiers = nodes
			.iter()
			.map(|node| Self::sync_get_database_node_specifier(node).cloned())
			.collect::<tg::Result<Vec<_>>>()?;
		let output = if replacement_ids.is_empty() {
			self.try_get_nodes_from_index(&ids, &specifiers).await?
		} else {
			self.index()
				.await
				.map_err(|error| tg::error!(!error, "failed to index"))?
				.try_last()
				.await
				.map_err(|error| tg::error!(!error, "failed to index"))?;
			self.try_get_nodes_once_from_index(&ids, &specifiers)
				.await?
		};

		// Validate the conflicts and collect all required authorizations.
		let mut authorization = BTreeMap::new();
		let mut parent_write_specifiers = BTreeSet::new();
		let mut replacement_roots = BTreeMap::new();
		for (((id, specifier), by_id), by_specifier) in std::iter::zip(
			std::iter::zip(std::iter::zip(ids, specifiers), output.specifiers),
			output.ids,
		) {
			let replace = replacement_ids.contains(&id);
			if !replace {
				Self::sync_get_database_validate_id_and_specifier(
					&id,
					&specifier,
					by_id.as_ref(),
					by_specifier.as_ref(),
				)?;
			}
			let cross_kind_replacement = replace
				&& by_specifier
					.as_ref()
					.is_some_and(|candidate| candidate.kind() != id.kind());
			if cross_kind_replacement {
				if let Some(parent) = specifier.parent() {
					parent_write_specifiers.insert(parent);
				}
			} else {
				let permission = Self::write_permission_for_resource(&id)?;
				let permissions = tg::authorization::permission::Set::from_permission(permission);
				let resource = tg::Selector::Specifier(specifier.clone());
				let allow_missing = by_specifier.is_none();
				authorization.insert(resource, (allow_missing, permissions));
			}
			if replace {
				let roots = Self::sync_get_database_replacement_roots(
					&id,
					&specifier,
					by_id.as_ref(),
					by_specifier.as_ref(),
				);
				replacement_roots.extend(roots);
			}
		}
		let parent_write_specifiers = parent_write_specifiers.into_iter().collect::<Vec<_>>();
		if !parent_write_specifiers.is_empty() {
			let output = self
				.try_get_nodes_once_from_index(&[], &parent_write_specifiers)
				.await?;
			for (specifier, id) in std::iter::zip(parent_write_specifiers, output.ids) {
				let id = id.ok_or_else(|| tg::error!(%specifier, "failed to find the parent"))?;
				let permission = Self::write_permission_for_resource(&id)?;
				let permissions = tg::authorization::permission::Set::from_permission(permission);
				let resource = tg::Selector::Specifier(specifier);
				authorization.insert(resource, (false, permissions));
			}
		}

		// Authorize recursive deletion at each minimal conflicting root.
		let roots = Self::sync_get_database_minimize_replacement_roots(replacement_roots);
		for root in roots {
			let permission = Self::delete_permission_for_resource(&root)?;
			let permissions = tg::authorization::permission::Set::from_permission(permission);
			let resource = tg::Selector::Id(root);
			authorization.insert(resource, (false, permissions));
		}

		// Authorize the writes and recursive deletions.
		let authorization = authorization.into_iter().collect::<Vec<_>>();
		for authorization in authorization.chunks(SYNC_GET_DATABASE_BATCH_SIZE) {
			let args = authorization
				.iter()
				.map(|(resource, (_, permissions))| (resource.clone(), *permissions))
				.collect::<Vec<_>>();
			let outputs = self.authorize_batch(args).await?;
			for ((_, (allow_missing, permissions)), output) in
				std::iter::zip(authorization, outputs)
			{
				let authorized = match output {
					None => *allow_missing,
					Some(output) => output.contains(*permissions),
				};
				if !authorized {
					return Err(tg::error!("unauthorized"));
				}
			}
		}

		Ok(())
	}

	fn sync_get_database_minimize_replacement_roots(
		roots: BTreeMap<tg::Id, tg::Specifier>,
	) -> BTreeSet<tg::Id> {
		let mut roots = roots
			.into_iter()
			.map(|(id, specifier)| (specifier, id))
			.collect::<Vec<_>>();
		roots.sort();
		let mut minimized = BTreeSet::new();
		let mut root = None;
		for (specifier, id) in roots {
			let covered = root
				.as_ref()
				.is_some_and(|root| Self::sync_get_database_specifier_has_prefix(&specifier, root));
			if covered {
				continue;
			}
			root = Some(specifier);
			minimized.insert(id);
		}

		minimized
	}

	fn sync_get_database_specifier_has_prefix(
		specifier: &tg::Specifier,
		prefix: &tg::Specifier,
	) -> bool {
		let mut components = specifier.components();

		prefix
			.components()
			.all(|component| components.next() == Some(component))
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

	async fn sync_get_database_replace_nodes_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		nodes: &[tg::sync::PutNodeMessage],
		namespace: &mut Namespace,
		replacement_ids: &std::collections::HashSet<tg::Id, fnv::FnvBuildHasher>,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<Vec<tg::Specifier>, crate::database::Error>> {
		// Find the current conflicts.
		let mut roots = BTreeMap::new();
		for node in nodes {
			let id = Self::sync_get_database_node_id(node)?;
			if !replacement_ids.contains(&id) {
				continue;
			}
			let specifier = Self::sync_get_database_node_specifier(node)?;
			let node_roots = Self::sync_get_database_replacement_roots(
				&id,
				specifier,
				namespace.ids.get(&id),
				namespace.specifiers.get(specifier),
			);
			roots.extend(node_roots);
		}

		// Traverse and delete the current conflicting subtrees.
		let roots = Self::sync_get_database_minimize_replacement_roots(roots);
		let nodes =
			match Self::sync_get_database_collect_subtrees_with_transaction(transaction, roots)
				.await?
			{
				ControlFlow::Break(nodes) => nodes,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let ids = Self::sync_get_database_sort_deletions(nodes);
		let specifiers = ids
			.iter()
			.filter(|id| matches!(id.kind(), tg::id::Kind::Tag))
			.filter_map(|id| namespace.ids.get(id).cloned())
			.collect();
		for ids in ids.chunks(SYNC_GET_DATABASE_BATCH_SIZE) {
			match self
				.sync_get_database_delete_nodes_with_transaction(transaction, ids, batch)
				.await?
			{
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		}
		namespace.remove_ids(&ids);

		Ok(ControlFlow::Break(specifiers))
	}

	fn sync_get_database_replacement_roots(
		id: &tg::Id,
		specifier: &tg::Specifier,
		by_id: Option<&tg::Specifier>,
		by_specifier: Option<&tg::Id>,
	) -> BTreeMap<tg::Id, tg::Specifier> {
		let mut roots = BTreeMap::new();
		if let Some(candidate) = by_id.filter(|candidate| *candidate != specifier) {
			roots.insert(id.clone(), candidate.clone());
		}
		if let Some(candidate) = by_specifier.filter(|candidate| *candidate != id) {
			roots.insert(candidate.clone(), specifier.clone());
		}

		roots
	}

	async fn sync_get_database_collect_subtrees_with_transaction(
		transaction: &Transaction<'_>,
		roots: BTreeSet<tg::Id>,
	) -> tg::Result<ControlFlow<BTreeMap<tg::Id, usize>, crate::database::Error>> {
		if roots.is_empty() {
			return Ok(ControlFlow::Break(BTreeMap::new()));
		}

		#[derive(db::row::Deserialize)]
		struct Row {
			depth: i64,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
		}

		let roots = roots.into_iter().collect::<Vec<_>>();
		let mut nodes = BTreeMap::<tg::Id, usize>::new();
		for roots in roots.chunks(SYNC_GET_DATABASE_BATCH_SIZE) {
			let p = transaction.p();
			let values = (1..=roots.len())
				.map(|index| format!("(cast({p}{index} as text))"))
				.collect::<Vec<_>>()
				.join(", ");
			let recursion = if p == "$" {
				"
					select children.id, subtree.depth + 1
					from subtree
					cross join lateral (
						select id from groups where parent = subtree.id
						union all
						select id from tags where parent = subtree.id
					) children
				"
			} else {
				"
					select groups.id, subtree.depth + 1
					from subtree
					join groups on groups.parent = subtree.id
					union all
					select tags.id, subtree.depth + 1
					from subtree
					join tags on tags.parent = subtree.id
				"
			};
			let statement = formatdoc!(
				"
					with recursive
					roots(id) as (values {values}),
					subtree(id, depth) as (
						select id, 0 from roots
						union all
						{recursion}
					)
					select id, depth from subtree;
				"
			);
			let params = roots
				.iter()
				.map(ToString::to_string)
				.map(db::Value::from)
				.collect();
			let result = transaction
				.query_into::<Row>(statement.into(), params)
				.await;
			let rows =
				crate::database::retry!(result, "failed to collect the replacement subtrees");
			let mut rows = std::pin::pin!(rows);
			loop {
				let result = rows.try_next().await;
				let row = crate::database::retry!(result, "failed to read a replacement subtree");
				let Some(row) = row else {
					break;
				};
				let depth = usize::try_from(row.depth)
					.map_err(|error| tg::error!(!error, "invalid replacement subtree depth"))?;
				nodes
					.entry(row.id)
					.and_modify(|existing| *existing = (*existing).max(depth))
					.or_insert(depth);
			}
		}

		Ok(ControlFlow::Break(nodes))
	}

	fn sync_get_database_sort_deletions(nodes: BTreeMap<tg::Id, usize>) -> Vec<tg::Id> {
		let mut nodes = nodes.into_iter().collect::<Vec<_>>();
		nodes.sort_by(|(id_a, depth_a), (id_b, depth_b)| {
			depth_b.cmp(depth_a).then_with(|| id_a.cmp(id_b))
		});

		nodes.into_iter().map(|(id, _)| id).collect()
	}

	async fn sync_get_database_delete_nodes_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		ids: &[tg::Id],
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		if ids.is_empty() {
			return Ok(ControlFlow::Break(()));
		}

		#[derive(db::row::Deserialize)]
		struct GroupMemberRow {
			#[tangram_database(as = "db::value::FromStr")]
			group: tg::group::Id,
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::group::Member,
		}

		#[derive(db::row::Deserialize)]
		struct OrganizationMemberRow {
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::organization::Member,
			#[tangram_database(as = "db::value::FromStr")]
			organization: tg::organization::Id,
		}

		// Partition the nodes by kind.
		let mut groups = Vec::new();
		let mut organizations = Vec::new();
		let mut tags = Vec::new();
		let mut users = Vec::new();
		for id in ids {
			match id.kind() {
				tg::id::Kind::Group => groups.push(id.clone()),
				tg::id::Kind::Organization => organizations.push(id.clone()),
				tg::id::Kind::Tag => tags.push(id.clone()),
				tg::id::Kind::User => users.push(id.clone()),
				_ => return Err(tg::error!(%id, "invalid database node kind")),
			}
		}

		// Read the memberships for the index batch.
		let p = transaction.p();
		let placeholders = (1..=ids.len())
			.map(|index| format!("{p}{index}"))
			.collect::<Vec<_>>()
			.join(", ");
		let params = ids
			.iter()
			.map(ToString::to_string)
			.map(db::Value::from)
			.collect::<Vec<_>>();
		let statement = format!(
			r#"
				select "group", member
				from group_members
				where "group" in ({placeholders}) or member in ({placeholders});
			"#
		);
		let result = transaction
			.query_all_into::<GroupMemberRow>(statement.into(), params.clone())
			.await;
		let group_members = crate::database::retry!(result, "failed to execute the statement");
		for row in group_members {
			batch
				.items
				.push(tangram_index::batch::Item::DeleteGroupMember(
					tangram_index::group::member::delete::Arg {
						group: row.group,
						member: row.member,
					},
				));
		}
		let statement = format!(
			"
				select organization, member
				from organization_members
				where organization in ({placeholders}) or member in ({placeholders});
			"
		);
		let result = transaction
			.query_all_into::<OrganizationMemberRow>(statement.into(), params.clone())
			.await;
		let organization_members =
			crate::database::retry!(result, "failed to execute the statement");
		for row in organization_members {
			batch
				.items
				.push(tangram_index::batch::Item::DeleteOrganizationMember(
					tangram_index::organization::member::delete::Arg {
						member: row.member,
						organization: row.organization,
					},
				));
		}

		// Delete the relationships and grants.
		for statement in [
			format!(
				r#"delete from group_members where "group" in ({placeholders}) or member in ({placeholders});"#
			),
			format!(
				"delete from organization_members where organization in ({placeholders}) or member in ({placeholders});"
			),
			format!("update runners set owner = null where owner in ({placeholders});"),
		] {
			let result = transaction.execute(statement.into(), params.clone()).await;
			crate::database::retry!(result, "failed to execute the statement");
		}
		match self
			.delete_node_grants_batch_with_transaction(transaction, ids, batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		// Delete user relationships.
		if !users.is_empty() {
			for (column, table) in [
				(r#""user""#, "github_identities"),
				(r#""user""#, "user_emails"),
				(r#""user""#, "user_identities"),
				(r#""user""#, "user_tokens"),
			] {
				match Self::sync_get_database_delete_ids_from_table_with_transaction(
					transaction,
					table,
					column,
					&users,
				)
				.await?
				{
					ControlFlow::Break(()) => (),
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				}
			}
			let p = transaction.p();
			let placeholders = (1..=users.len())
				.map(|index| format!("{p}{index}"))
				.collect::<Vec<_>>()
				.join(", ");
			let statement =
				format!(r#"update logins set "user" = null where "user" in ({placeholders});"#);
			let params = users
				.iter()
				.map(ToString::to_string)
				.map(db::Value::from)
				.collect();
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to execute the statement");
		}

		// Delete the nodes from the database and index.
		for id in ids {
			let node = match id.kind() {
				tg::id::Kind::Group => {
					tangram_index::batch::Item::DeleteGroup(id.clone().try_into()?)
				},
				tg::id::Kind::Organization => {
					tangram_index::batch::Item::DeleteOrganization(id.clone().try_into()?)
				},
				tg::id::Kind::Tag => tangram_index::batch::Item::DeleteTag(id.clone().try_into()?),
				tg::id::Kind::User => {
					tangram_index::batch::Item::DeleteUser(id.clone().try_into()?)
				},
				_ => unreachable!(),
			};
			batch.items.push(node);
		}
		for (ids, table) in [
			(groups.as_slice(), "groups"),
			(organizations.as_slice(), "organizations"),
			(tags.as_slice(), "tags"),
			(users.as_slice(), "users"),
		] {
			match Self::sync_get_database_delete_ids_from_table_with_transaction(
				transaction,
				table,
				"id",
				ids,
			)
			.await?
			{
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		}
		match Self::sync_get_database_delete_ids_from_table_with_transaction(
			transaction,
			"specifiers",
			"id",
			ids,
		)
		.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(()))
	}

	async fn sync_get_database_delete_ids_from_table_with_transaction(
		transaction: &Transaction<'_>,
		table: &str,
		column: &str,
		ids: &[tg::Id],
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		if ids.is_empty() {
			return Ok(ControlFlow::Break(()));
		}
		let p = transaction.p();
		let placeholders = (1..=ids.len())
			.map(|index| format!("{p}{index}"))
			.collect::<Vec<_>>()
			.join(", ");
		let statement = format!("delete from {table} where {column} in ({placeholders});");
		let params = ids
			.iter()
			.map(ToString::to_string)
			.map(db::Value::from)
			.collect();
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

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
