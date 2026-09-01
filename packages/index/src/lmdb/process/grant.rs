use {
	crate::lmdb::{Db, Index},
	foundationdb_tuple as fdbt, heed as lmdb,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
};

impl Index {
	pub(crate) fn put_process_object_grants_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &crate::process::object::grant::Arg,
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
		let mut grants = BTreeMap::new();
		let mut traversed = BTreeSet::new();
		let authorization_fact_cache = crate::authorize::facts::Cache::new();

		// Walk the authorized portion of the locally indexed object graph.
		while !objects.is_empty() {
			let authorize_args = objects
				.iter()
				.cloned()
				.map(|object| crate::authorize::Arg {
					requested,
					required: node.into(),
					resource: tg::Selector::Id(object.into()),
					token: None,
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
		Self::put_grants_with_transaction(db, subspace, transaction, &grant_args)?;

		Ok(())
	}
}
