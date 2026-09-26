use {
	crate::fdb::Index,
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
	},
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) async fn put_process_object_permissions_with_transaction(
		authorize_concurrency: usize,
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::process::object::permission::Arg,
		partition_totals: crate::fdb::PartitionTotals,
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
		let authorization_fact_cache = crate::authorize::facts::Cache::new();

		// Walk the authorized portion of the locally indexed object graph.
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

			let authorize_args = objects
				.iter()
				.cloned()
				.map(|object| crate::authorize::Arg {
					requested,
					required: node.into(),
					resource: tg::Selector::Id(object.into()),
					tokens: Vec::new(),
				})
				.collect::<Vec<_>>();
			let authorizations = crate::fdb::propagate!(
				Self::authorize_batch_with_transaction(
					authorization_fact_cache.clone(),
					authorize_concurrency,
					arg.authorize,
					txn,
					subspace,
					&authorize_args,
					&arg.principal,
				)
				.await
			);
			let mut authorized = Vec::new();
			for (object, outcome) in std::iter::zip(objects, authorizations) {
				let authorization = match outcome {
					crate::authorize::Outcome::Authorized(output)
					| crate::authorize::Outcome::Denied(Some(output)) => Some(output),
					crate::authorize::Outcome::Denied(None) => None,
					crate::authorize::Outcome::Exhausted => {
						return Err(crate::authorize::search_exhausted_error(
							"the process object permission authorization search exhausted",
						));
					},
				};
				let proven_permissions = root_permissions
					.get(&object)
					.copied()
					.unwrap_or_else(|| requested.empty_like());
				let permission = if authorization
					.as_ref()
					.is_some_and(|authorization| authorization.permissions.contains(subtree))
				{
					tg::authorization::permission::object::Permission::Subtree
				} else if proven_permissions.contains(node)
					|| authorization
						.as_ref()
						.is_some_and(|authorization| authorization.permissions.contains(node))
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
				authorized.push(object);
			}
			let results = futures::future::try_join_all(authorized.iter().map(|object| {
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
			.map(|(resource, permission)| crate::permission::put::Arg {
				created_at: arg.created_at,
				creator: creator.clone(),
				permissions: tg::authorization::Permission::Object(permission).into(),
				resource: resource.into(),
				source: crate::permission::Source::Direct {
					expires_at: arg.expires_at,
				},
				subject: subject.clone(),
				time_to_touch: arg.time_to_touch,
			})
			.collect::<Vec<_>>();
		crate::fdb::propagate!(
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
