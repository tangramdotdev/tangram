use {
	crate::Index,
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
	},
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_process_object_permissions_with_transaction(
		verify_concurrency: usize,
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		arg: &tangram_index::process::object::permission::Arg,
		partition_totals: crate::PartitionTotals,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		arg.validate()?;
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let mut requested = tg::authorization::permission::object::Set::empty();
		requested.insert(tg::authorization::permission::object::Set::NODE);
		requested.insert(tg::authorization::permission::object::Set::SUBTREE);
		let requested = tg::authorization::permission::Set::Object(requested);
		let mut root_permissions: BTreeMap<_, tg::authorization::permission::Set> = BTreeMap::new();
		let mut objects = BTreeSet::new();
		for root in &arg.roots {
			if let Some(permissions) = root.permissions {
				root_permissions
					.entry(root.object.clone())
					.and_modify(|root_permissions| root_permissions.insert(permissions))
					.or_insert(permissions);
			}
			objects.insert(root.object.clone());
		}
		let mut permissions = BTreeMap::new();
		let mut traversed = BTreeSet::new();
		let verification_fact_cache = tangram_index::verify::facts::Cache::new();

		// Walk the verified portion of the locally indexed object graph.
		while !objects.is_empty() {
			// Use the supplied subtree permissions before searching the index.
			objects.retain(|object| {
				if !root_permissions
					.get(object)
					.is_some_and(|permissions| permissions.contains(subtree))
				{
					return true;
				}
				permissions.insert(
					object.clone(),
					tg::authorization::permission::object::Permission::Subtree,
				);
				false
			});
			if objects.is_empty() {
				break;
			}

			let verify_args = objects
				.iter()
				.cloned()
				.map(|object| tangram_index::verify::Arg {
					storage: tg::storage::Set::Object(tg::object::storage::Set::empty()),
					subject: None,
					requested,
					required: node.into(),
					resource: tg::Selector::Id(object.into()),
					tokens: Vec::new(),
				})
				.collect::<Vec<_>>();
			let verifications = crate::propagate!(
				Self::verify_batch_with_transaction(
					verification_fact_cache.clone(),
					verify_concurrency,
					arg.verify,
					txn,
					subspace,
					&verify_args,
					&arg.principal,
				)
				.await
			);
			let mut verified = Vec::new();
			for (object, outcome) in std::iter::zip(objects, verifications) {
				if outcome.outcome == tangram_index::verify::Outcome::Exhausted {
					return Err(tangram_index::verify::search_exhausted_error(
						"the process object permission verification search exhausted",
					));
				}
				let verification = Some(outcome);

				let proven_permissions = root_permissions
					.get(&object)
					.copied()
					.unwrap_or_else(|| requested.empty_like());
				let permission = if verification
					.as_ref()
					.is_some_and(|verification| verification.permissions.contains(subtree))
				{
					tg::authorization::permission::object::Permission::Subtree
				} else if proven_permissions.contains(node)
					|| verification
						.as_ref()
						.is_some_and(|verification| verification.permissions.contains(node))
				{
					tg::authorization::permission::object::Permission::Node
				} else {
					continue;
				};
				permissions
					.entry(object.clone())
					.and_modify(|current| {
						if permission == tg::authorization::permission::object::Permission::Subtree
						{
							*current = permission;
						}
					})
					.or_insert(permission);
				if permission == tg::authorization::permission::object::Permission::Subtree
					|| !traversed.insert(object.clone())
				{
					continue;
				}
				verified.push(object);
			}
			let results = futures::future::try_join_all(verified.iter().map(|object| {
				Self::try_get_object_children_with_transaction(txn, subspace, object)
			}))
			.await?;
			let mut children = BTreeSet::new();
			for result in results {
				let object_children = match result {
					ControlFlow::Break(object_children) => object_children,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				if let Some(object_children) = object_children {
					children.extend(object_children);
				}
			}
			objects = children;
		}

		// Put the permissions.
		let creator = Some(arg.principal.clone());
		let subject = tg::authorization::Subject::Process(arg.process.clone());
		let permission_args = permissions
			.into_iter()
			.map(
				|(resource, permission)| tangram_index::permission::put::Arg {
					created_at: arg.created_at,
					creator: creator.clone(),
					permissions: tg::authorization::Permission::Object(permission).into(),
					resource: resource.into(),
					source: tangram_index::permission::Source::Direct {
						expires_at: arg.expires_at,
					},
					subject: subject.clone(),
					time_to_touch: arg.time_to_touch,
				},
			)
			.collect::<Vec<_>>();
		crate::propagate!(
			Self::put_permissions_with_transaction(
				txn,
				subspace,
				&permission_args,
				partition_totals
			)
			.await
		);

		Ok(ControlFlow::Break(()))
	}
}
