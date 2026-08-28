use {
	crate::Session, futures::FutureExt as _, tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _, tangram_index::Index as _,
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
		let mut index_args = Vec::new();
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
					.cloned();
				if let Some(token) = &token {
					if token.body.grants(subtree_permission) {
						permissions.insert(tg::authorization::permission::object::Set::SUBTREE);
					} else if token
						.body
						.grants(tg::authorization::Permission::Object(node))
					{
						permissions.insert(tg::authorization::permission::object::Set::NODE);
					}
				}
				let permissions = (!permissions.is_empty())
					.then_some(tg::authorization::permission::Set::Object(permissions));
				if !permissions.is_some_and(|permissions| permissions.contains(subtree_permission))
				{
					let permissions = subtree_permission.into();
					let resource = tg::Selector::Id(resource);
					let token = token.map(|token| token.body);
					index_args.push(tangram_index::authorize::Arg {
						required: permissions,
						requested: permissions,
						resource,
						token,
					});
				}
				tangram_index::process::object::grant::Root {
					object: root.node,
					permissions,
				}
			})
			.collect::<Vec<_>>();

		// Resolve authorization from the current index or wait for indexing.
		if !index_args.is_empty() {
			let required = vec![subtree_permission.into(); index_args.len()];
			self.prepare_process_object_grant_authorization(index_args, required)
				.boxed()
				.await?;
		}

		let authorize =
			crate::authorization_search_config(&self.server.config.authorization.final_);
		let principal = self.context.principal.clone();
		let process = process.clone();
		let time_to_touch = expires_at.map(|_| self.server.config.object.grant_time_to_touch);
		let arg = tangram_index::process::object::grant::Arg {
			authorize,
			created_at,
			expires_at,
			principal,
			process,
			roots,
			time_to_touch,
		};

		Ok(arg)
	}

	async fn prepare_process_object_grant_authorization(
		&self,
		args: Vec<tangram_index::authorize::Arg>,
		required: Vec<tg::authorization::permission::Set>,
	) -> tg::Result<()> {
		let authorization = &self.server.config.authorization;
		let delay = authorization.index.delay;
		let initial_config = crate::authorization_search_config(&authorization.initial);
		let grants_required = |outcomes: &[tangram_index::authorize::Outcome]| {
			outcomes.len() == required.len()
				&& std::iter::zip(outcomes, &required).all(|(outcome, required)| {
					outcome
						.output()
						.is_some_and(|output| output.permissions.contains(*required))
				})
		};
		let initial =
			self.server
				.index
				.authorize_batch(&args, initial_config, &self.context.principal);
		tokio::pin!(initial);
		let initial_result = match delay {
			Some(delay) => tokio::select! {
				result = &mut initial => Some(result),
				() = tokio::time::sleep(delay) => None,
			},
			None => Some((&mut initial).await),
		};
		let index_wait = async {
			self.index()
				.await
				.map_err(|error| tg::error!(!error, "failed to index process objects"))?
				.try_last()
				.await
				.map_err(|error| tg::error!(!error, "failed to index process objects"))?;

			Ok(())
		};
		match initial_result {
			Some(Ok(outcomes)) if grants_required(&outcomes) => {},
			Some(Ok(_)) => index_wait.await?,
			Some(Err(error)) => return Err(error),
			None => {
				tokio::pin!(index_wait);
				tokio::select! {
					result = &mut initial => match result {
						Ok(outcomes) if grants_required(&outcomes) => {},
						Ok(_) => index_wait.await?,
						Err(error) => return Err(error),
					},
					result = &mut index_wait => match result {
						Ok(()) => {},
						Err(error) => match initial.await {
							Ok(outcomes) if grants_required(&outcomes) => {},
							Ok(_) | Err(_) => return Err(error),
						},
					},
				}
			},
		}

		Ok(())
	}
}
