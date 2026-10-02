use {
	super::{Indexer, RETRY_OPTIONS, partition},
	futures::{FutureExt as _, future},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_index::{
		Index as _,
		permission::capture::{Entry, enqueue},
	},
};

impl Indexer {
	pub(super) async fn permission_capture_task(
		&self,
		config: &crate::config::IndexerPermissionCapture,
		partition_start: u64,
		partition_end: u64,
		stopper: &Stopper,
	) -> tg::Result<()> {
		if config.concurrency == 0 || config.batch_size == 0 || config.poll_interval.is_zero() {
			return Err(tg::error!("invalid permission capture task configuration"));
		}
		if partition_start == partition_end {
			stopper.wait().await;
			return Ok(());
		}
		let futures =
			partition::ranges(partition_start, partition_end, config.concurrency).map(|range| {
				self.permission_capture_partition_task(config, range.start, range.end, stopper)
			});
		future::try_join_all(futures).await?;

		Ok(())
	}

	async fn permission_capture_partition_task(
		&self,
		config: &crate::config::IndexerPermissionCapture,
		partition_start: u64,
		partition_end: u64,
		stopper: &Stopper,
	) -> tg::Result<()> {
		loop {
			if stopper.stopped() {
				return Ok(());
			}
			let result = tokio::select! {
				() = stopper.wait() => return Ok(()),
				result = self.server.index.permission_capture_batch(
					config.batch_size,
					partition_start,
					partition_end,
				) => result,
			};
			let entries = match result {
				Ok(entries) => entries,
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to read permission capture entries");
					tokio::select! {
						() = stopper.wait() => return Ok(()),
						() = tokio::time::sleep(RETRY_OPTIONS.max_delay) => {},
					}
					continue;
				},
			};
			if entries.is_empty() {
				tokio::select! {
					() = stopper.wait() => return Ok(()),
					() = tokio::time::sleep(config.poll_interval) => {},
				}
				continue;
			}
			let mut failed = false;
			for entry in entries {
				let result = tokio::select! {
					() = stopper.wait() => return Ok(()),
					result = async {
						self.permission_capture_entry(&entry).boxed().await?;
						self.server.index.complete_permission_capture(&entry).await?;
						Ok::<_, tg::Error>(())
					} => result,
				};

				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to capture permissions");
					failed = true;
				}
			}
			if failed {
				tokio::select! {
					() = stopper.wait() => return Ok(()),
					() = tokio::time::sleep(RETRY_OPTIONS.max_delay) => {},
				}
			}
		}
	}

	async fn permission_capture_entry(&self, entry: &Entry) -> tg::Result<()> {
		let arg = &entry.arg;
		if let Some(version) = &arg.version {
			let id = tg::tag::Id::try_from(arg.resource.clone())?;
			let mut tags = self.server.index.try_get_tags(&[id]).await?;
			if tags
				.pop()
				.unwrap()
				.is_none_or(|tag| tag.version != *version)
			{
				return Ok(());
			}
		}
		for root in &arg.roots {
			self.permission_capture_checkpoint_with_resource(
				"permission_capture.started",
				arg,
				&root.node,
			)
			.await;
		}
		let mut context = self.server.context.clone();
		context.principal = arg.principal.clone();
		context.token = None;
		let session = self.server.session(&context);

		let mut pending = BTreeMap::new();
		let mut stack = Vec::new();
		let mut visited = BTreeMap::new();
		for root in arg.roots.iter().rev().cloned() {
			push_root(&mut pending, &mut stack, &visited, root);
		}
		let mut error = None;
		while let Some(id) = stack.pop() {
			let root = pending.remove(&id).unwrap();
			visited.insert(root.node.clone(), root.clone());
			let requested = requested_permissions(&root.node)?;
			let output = match session.authorize(root.clone(), requested).boxed().await {
				Ok(output) => output,
				Err(source) => {
					error.get_or_insert(source);
					continue;
				},
			};
			self.capture_permissions(arg, &root.node, output.permissions)
				.await?;
			if output.outcome == crate::authorization::Outcome::Exhausted {
				error.get_or_insert_with(|| {
					tangram_index::verify::search_exhausted_error(
						"the permission capture search exhausted",
					)
				});
				continue;
			}
			if covers_subtree(&root.node, output.permissions) || output.permissions.is_empty() {
				continue;
			}
			let children = match self
				.try_get_permission_capture_children(&root.node)
				.boxed()
				.await
			{
				Ok(Some(children)) => children,
				Ok(None) => continue,
				Err(source) => {
					error.get_or_insert(source);
					continue;
				},
			};
			for mut child in children.into_iter().rev() {
				child.options.tokens.inherit(&root.options.tokens);
				push_root(&mut pending, &mut stack, &visited, child);
			}
		}
		if let Some(error) = error {
			return Err(error);
		}

		Ok(())
	}

	async fn capture_permissions(
		&self,
		arg: &enqueue::Arg,
		resource: &tg::Id,
		permissions: tg::authorization::permission::Set,
	) -> tg::Result<()> {
		self.permission_capture_checkpoint_with_resource(
			"permission_capture.advance",
			arg,
			resource,
		)
		.await;
		if !permissions.is_empty() {
			self.permission_capture_checkpoint_with_resource(
				"permission_capture.write",
				arg,
				resource,
			)
			.await;
			let (creator, subject) = match arg.resource.kind() {
				tg::id::Kind::Process => {
					let process = tg::process::Id::try_from(arg.resource.clone())?;
					(
						Some(tg::Principal::Process(process.clone())),
						tg::authorization::Subject::Process(process),
					)
				},
				tg::id::Kind::Tag => {
					let tag = tg::tag::Id::try_from(arg.resource.clone())?;
					(None, tg::authorization::Subject::Tag(tag))
				},
				_ => return Err(tg::error!("invalid permission capture resource")),
			};
			let permission = tangram_index::permission::put::Arg {
				created_at: self.server.clock.unix_timestamp()?,
				creator,
				permissions,
				resource: resource.clone(),
				source: tangram_index::permission::Source::Direct { expires_at: None },
				subject,
				time_to_touch: None,
				version: arg.version.clone(),
			};
			self.server.index.put_permissions(&[permission]).await?;
			self.permission_capture_checkpoint_with_resource(
				"permission_capture.written",
				arg,
				resource,
			)
			.await;
		}
		self.permission_capture_checkpoint_with_resource(
			"permission_capture.advanced",
			arg,
			resource,
		)
		.await;
		Ok(())
	}

	async fn try_get_permission_capture_children(
		&self,
		resource: &tg::Id,
	) -> tg::Result<Option<Vec<tg::Referent<tg::Id>>>> {
		if resource.kind().is_object() {
			let id = tg::object::Id::try_from(resource.clone())?;
			let children = self.server.index.try_get_object_children(&id).await?;
			return Ok(children.map(|children| {
				children
					.into_iter()
					.map(|id| tg::Referent::with_node(id.into()))
					.collect()
			}));
		}
		let id = tg::process::Id::try_from(resource.clone())?;
		let children = self
			.server
			.index
			.try_get_process_children_and_objects(&id)
			.await?;
		Ok(children
			.filter(|children| children.complete)
			.map(|children| children.nodes))
	}

	async fn permission_capture_checkpoint_with_resource(
		&self,
		name: &str,
		arg: &enqueue::Arg,
		resource: &tg::Id,
	) {
		if let Some(version) = &arg.version {
			crate::checkpoint!(self.server, name, tag = %arg.resource, %version, %resource).await;
		} else {
			crate::checkpoint!(self.server, name, process = %arg.resource, %resource).await;
		}
	}
}

fn push_root(
	pending: &mut BTreeMap<tg::Id, tg::Referent<tg::Id>>,
	stack: &mut Vec<tg::Id>,
	visited: &BTreeMap<tg::Id, tg::Referent<tg::Id>>,
	mut root: tg::Referent<tg::Id>,
) {
	if let Some(previous) = visited.get(&root.node) {
		let mut merged = previous.clone();
		merged.options.tokens.inherit(&root.options.tokens);
		if merged.options.tokens == previous.options.tokens {
			return;
		}
		root = merged;
	}
	if let Some(previous) = pending.get_mut(&root.node) {
		previous.options.tokens.inherit(&root.options.tokens);
	} else {
		stack.push(root.node.clone());
		pending.insert(root.node.clone(), root);
	}
}

fn requested_permissions(resource: &tg::Id) -> tg::Result<tg::authorization::permission::Set> {
	let mut permissions = node_permission(resource)?;
	if tg::object::Id::try_from(resource.clone()).is_ok() {
		permissions.insert(
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)
			.into(),
		);
	} else {
		let mut requested = tg::authorization::permission::process::Set::all();
		requested.remove(tg::authorization::permission::process::Set::PARENT);
		permissions.insert(tg::authorization::permission::Set::Process(requested));
	}
	Ok(permissions)
}

fn node_permission(resource: &tg::Id) -> tg::Result<tg::authorization::permission::Set> {
	if tg::object::Id::try_from(resource.clone()).is_ok() {
		Ok(tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		)
		.into())
	} else if tg::process::Id::try_from(resource.clone()).is_ok() {
		Ok(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		)
		.into())
	} else {
		Err(tg::error!(
			"a permission capture resource must be an object or process"
		))
	}
}

fn covers_subtree(resource: &tg::Id, permissions: tg::authorization::permission::Set) -> bool {
	if tg::object::Id::try_from(resource.clone()).is_ok() {
		permissions.contains(tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		))
	} else {
		let requested = [
			tg::authorization::permission::process::Permission::Subtree,
			tg::authorization::permission::process::Permission::SubtreeCommandObjects,
			tg::authorization::permission::process::Permission::SubtreeErrorObjects,
			tg::authorization::permission::process::Permission::SubtreeLogObjects,
			tg::authorization::permission::process::Permission::SubtreeOutputObjects,
		];
		requested.into_iter().all(|permission| {
			permissions.contains(tg::authorization::Permission::Process(permission))
		})
	}
}
