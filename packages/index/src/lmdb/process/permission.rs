use {
	crate::lmdb::{Db, Index},
	foundationdb_tuple as fdbt, heed as lmdb,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_process_object_permissions_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::process::object::permission::Arg,
	) -> tg::Result<()> {
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
			let authorizations = Self::authorize_batch_with_transaction(
				authorization_fact_cache.clone(),
				arg.authorize,
				db,
				subspace,
				transaction,
				&authorize_args,
				&arg.principal,
			)?;
			let mut children = BTreeSet::new();
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
				let Some(object_children) = Self::try_get_object_children_with_transaction(
					db,
					subspace,
					transaction,
					&object,
				)?
				else {
					continue;
				};
				children.extend(object_children);
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
		Self::put_permissions_with_transaction(db, subspace, transaction, &permission_args)?;

		Ok(())
	}
}
