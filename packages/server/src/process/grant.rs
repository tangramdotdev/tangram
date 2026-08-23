use {crate::Session, tangram_client::prelude::*, tangram_futures::stream::TryExt as _};

impl Session {
	pub(crate) async fn create_process_object_grant_arg(
		&self,
		process: &tg::process::Id,
		roots: impl IntoIterator<Item = tg::Referent<tg::object::Id>>,
		created_at: i64,
		expires_at: Option<i64>,
	) -> tg::Result<tangram_index::process::object::grant::Arg> {
		let node = tg::authorization::permission::object::Permission::Node;
		let subtree = tg::authorization::permission::object::Permission::Subtree;
		let roots = roots
			.into_iter()
			.map(|root| {
				let mut permissions = tg::authorization::permission::object::Set::empty();
				let resource = tg::Id::from(root.node.clone());
				if let Some(token) = root.options.tokens.local()
					&& token.body.resource == resource
					&& self.verify_local_token(token)
				{
					if token
						.body
						.grants(tg::authorization::Permission::Object(subtree))
					{
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
				tangram_index::process::object::grant::Root {
					object: root.node,
					permissions,
				}
			})
			.collect::<Vec<_>>();
		let subtree = tg::authorization::Permission::Object(subtree);
		let needs_index = roots.iter().any(|root| {
			!root
				.permissions
				.is_some_and(|permissions| permissions.contains(subtree))
		});
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
