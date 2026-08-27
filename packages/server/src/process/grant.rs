use {
	crate::Session, tangram_client::prelude::*, tangram_futures::stream::TryExt as _,
	tangram_index::Index as _,
};

impl Session {
	pub(crate) async fn create_process_object_grant_arg(
		&self,
		process: &tg::process::Id,
		roots: impl IntoIterator<Item = tg::Referent<tg::object::Id>>,
		created_at: i64,
		expires_at: Option<i64>,
	) -> tg::Result<tangram_index::process::object::grant::Arg> {
		self.create_process_object_grant_arg_with_root_permissions(
			process,
			roots,
			created_at,
			expires_at,
			tg::authorization::permission::object::Set::empty(),
		)
		.await
	}

	pub(crate) async fn create_process_object_grant_arg_with_root_permissions(
		&self,
		process: &tg::process::Id,
		roots: impl IntoIterator<Item = tg::Referent<tg::object::Id>>,
		created_at: i64,
		expires_at: Option<i64>,
		root_permissions: tg::authorization::permission::object::Set,
	) -> tg::Result<tangram_index::process::object::grant::Arg> {
		let node = tg::authorization::permission::object::Permission::Node;
		let subtree = tg::authorization::permission::object::Permission::Subtree;
		let subtree_permission = tg::authorization::Permission::Object(subtree);
		let mut authorize_args = Vec::new();
		let roots = roots
			.into_iter()
			.map(|root| {
				let mut permissions = root_permissions;
				let resource = tg::Id::from(root.node.clone());
				let token = root
					.options
					.tokens
					.local()
					.filter(|token| {
						token.body.resource == resource && self.verify_local_token(token)
					})
					.map(|token| token.body.clone());
				if let Some(token) = &token {
					if token.grants(subtree_permission) {
						permissions.insert(tg::authorization::permission::object::Set::SUBTREE);
					} else if token.grants(tg::authorization::Permission::Object(node)) {
						permissions.insert(tg::authorization::permission::object::Set::NODE);
					}
				}
				let permissions = (!permissions.is_empty())
					.then_some(tg::authorization::permission::Set::Object(permissions));
				if !permissions.is_some_and(|permissions| permissions.contains(subtree_permission))
				{
					authorize_args.push(tangram_index::authorize::Arg {
						permissions: subtree_permission.into(),
						resource: tg::Selector::Id(resource),
						token,
					});
				}
				tangram_index::process::object::grant::Root {
					object: root.node,
					permissions,
				}
			})
			.collect::<Vec<_>>();

		// Attempt to authorize from the current index before refreshing it.
		let authorizations = self
			.server
			.index
			.authorize_batch(
				&authorize_args,
				crate::authorization_search_config(&self.server.config.authorization.initial),
				&self.context.principal,
			)
			.await;
		let needs_index = match authorizations {
			Ok(outcomes) => outcomes.iter().any(|outcome| {
				outcome
					.output()
					.is_none_or(|output| !output.permissions.contains(subtree_permission))
			}),
			Err(_) => true,
		};
		if needs_index {
			self.index()
				.await
				.map_err(|error| tg::error!(!error, "failed to index process objects"))?
				.try_last()
				.await
				.map_err(|error| tg::error!(!error, "failed to index process objects"))?;
		}

		let principal = self.context.principal.clone();
		let process = process.clone();
		let time_to_touch = expires_at.map(|_| self.server.config.object.grant_time_to_touch);
		let arg = tangram_index::process::object::grant::Arg {
			created_at,
			expires_at,
			principal,
			process,
			roots,
			time_to_touch,
		};

		Ok(arg)
	}
}
