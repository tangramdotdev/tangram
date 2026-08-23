use {
	crate::Session,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn create_process_object_grant_args(
		&self,
		process: &tg::process::Id,
		roots: impl IntoIterator<Item = tg::Referent<tg::object::Id>>,
		created_at: i64,
		expires_at: Option<i64>,
	) -> tg::Result<Vec<tangram_index::grant::put::Arg>> {
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
		let mut grants = BTreeMap::new();
		let mut objects = roots.into_iter().collect::<Vec<_>>();
		let mut traversed = BTreeSet::new();

		// Walk the authorized portion of the locally indexed object graph.
		while !objects.is_empty() {
			let authorizations = self
				.authorize_batch(objects.iter().cloned().map(|object| (object, requested)))
				.await?;
			let mut authorized = BTreeMap::new();
			for (object, authorization) in std::iter::zip(objects, authorizations) {
				let Some(authorization) = authorization else {
					continue;
				};
				let permission = if authorization.contains(subtree) {
					tg::authorization::permission::object::Permission::Subtree
				} else if authorization.contains(node) {
					tg::authorization::permission::object::Permission::Node
				} else {
					continue;
				};
				authorized
					.entry(object.node)
					.and_modify(|current| {
						if permission == tg::authorization::permission::object::Permission::Subtree
						{
							*current = permission;
						}
					})
					.or_insert(permission);
			}

			let mut children = BTreeSet::new();
			for (object, permission) in authorized {
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
				let Some(object_children) =
					self.server.index.try_get_object_children(&object).await?
				else {
					continue;
				};
				children.extend(object_children);
			}
			objects = children.into_iter().map(tg::Referent::with_node).collect();
		}

		// Create the grant arguments.
		let creator = Some(self.context.principal.clone());
		let subject = tg::authorization::Subject::Process(process.clone());
		let time_to_touch = expires_at.map(|_| self.server.config.object.grant_time_to_touch);
		let args = grants
			.into_iter()
			.map(|(resource, permission)| tangram_index::grant::put::Arg {
				created_at,
				creator: creator.clone(),
				implicit: Some(expires_at),
				permissions: tg::authorization::Permission::Object(permission).into(),
				resource: resource.into(),
				subject: subject.clone(),
				time_to_touch,
			})
			.collect();

		Ok(args)
	}
}
