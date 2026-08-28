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
	pub(crate) async fn put_process_object_grants_with_transaction(
		authorize_concurrency: usize,
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &crate::process::object::grant::Arg,
		partition_total: u64,
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
		let mut grants = BTreeMap::new();
		let mut traversed = BTreeSet::new();

		// Walk the authorized portion of the locally indexed object graph.
		while !objects.is_empty() {
			let authorize_args = objects
				.iter()
				.cloned()
				.map(|object| crate::authorize::Arg {
					required: node.into(),
					requested,
					resource: tg::Selector::Id(object.into()),
					token: None,
				})
				.collect::<Vec<_>>();
			let authorizations = crate::fdb::propagate!(
				Self::authorize_batch_with_transaction(
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
							"the process object grant authorization search exhausted",
						));
					},
				};
				let root_permissions = root_permissions
					.get(&object)
					.copied()
					.unwrap_or_else(|| requested.empty_like());
				let permission = if root_permissions.contains(subtree)
					|| authorization
						.as_ref()
						.is_some_and(|authorization| authorization.permissions.contains(subtree))
				{
					tg::authorization::permission::object::Permission::Subtree
				} else if root_permissions.contains(node)
					|| authorization
						.as_ref()
						.is_some_and(|authorization| authorization.permissions.contains(node))
				{
					tg::authorization::permission::object::Permission::Node
				} else {
					continue;
				};
				grants
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

		// Put the grants.
		let creator = Some(arg.principal.clone());
		let subject = tg::authorization::Subject::Process(arg.process.clone());
		let grant_args = grants
			.into_iter()
			.map(|(resource, permission)| crate::grant::put::Arg {
				created_at: arg.created_at,
				creator: creator.clone(),
				implicit: Some(arg.expires_at),
				permissions: tg::authorization::Permission::Object(permission).into(),
				resource: resource.into(),
				subject: subject.clone(),
				time_to_touch: arg.time_to_touch,
			})
			.collect::<Vec<_>>();
		crate::fdb::propagate!(
			Self::put_grants_with_transaction(txn, subspace, &grant_args, partition_total,).await
		);

		Ok(ControlFlow::Break(()))
	}
}
