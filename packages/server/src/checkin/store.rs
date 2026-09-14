use {
	crate::Session,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

impl Session {
	pub(super) async fn try_checkin_store_path(
		&self,
		path: &Path,
	) -> tg::Result<Option<tg::checkin::Output>> {
		let checkout_path = self.server.checkout_path();
		let store_path = self.server.store_path();
		let Ok(path) = path
			.strip_prefix(&checkout_path)
			.or_else(|_| path.strip_prefix(&store_path))
		else {
			return Ok(None);
		};

		// Parse the root artifact and the path within it.
		let mut components = path.components();
		let component = components
			.next()
			.ok_or_else(|| tg::error!("cannot check in the store directory"))?;
		let component = component
			.as_os_str()
			.to_str()
			.ok_or_else(|| tg::error!("the store path component is not valid utf-8"))?;
		let component = tg::store::path::parse_component(component)
			.map_err(|error| tg::error!(!error, "failed to parse the store path component"))?;
		let tg::store::path::Component::Id { id, .. } = component else {
			return Ok(None);
		};
		let path = components.collect::<PathBuf>();
		let output = self
			.checkin_store_path_inner(id, &path)
			.await
			.map_err(|error| tg::error!(!error, "failed to check in the store path"))?;

		Ok(Some(output))
	}

	async fn checkin_store_path_inner(
		&self,
		id: tg::artifact::Id,
		path: &Path,
	) -> tg::Result<tg::checkin::Output> {
		// Recover proofs from the physical checkout and the authenticated origin sandbox.
		let checkout_path = self.server.checkout_path().join(id.to_string());
		let mut tokens = Self::checkin_read_file_tokens(&checkout_path)?;
		let sandbox = self.try_get_checkin_origin_sandbox()?;
		if let Some(sandbox) = &sandbox
			&& let Some(state) = self.server.runner.state().sandboxes().get_by_id(sandbox)
			&& let Some(token) = state.tokens.get(&id)
		{
			tokens.insert_local(token.clone());
		}

		// Authorize the root and bound both returned tokens by the accepted proof.
		let mut referent = tg::Referent::with_node_and_tokens(id.clone(), tokens);
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let authorization = self
			.authorize_object_read(referent.clone(), true)
			.await?
			.filter(|authorization| authorization.permissions.contains(subtree))
			.ok_or_else(|| tg::error!("unauthorized"))?;
		let now = self.server.clock.unix_timestamp()?;
		let time_to_live = i64::try_from(self.server.config.object.grant_time_to_live.as_secs())
			.map_err(|error| tg::error!(!error, "failed to convert the grant time to live"))?;
		let expires_at = now
			.checked_add(time_to_live)
			.ok_or_else(|| tg::error!("the grant expiration overflowed"))?;
		let expires_at = authorization
			.expires_at
			.map_or(expires_at, |expiration| expiration.min(expires_at));
		let root_token = self.create_token(id.clone().into(), vec![subtree], expires_at)?;
		if let Some(token) = &root_token {
			referent.options.tokens.insert_local(token.clone());
		}

		// Resolve through artifact handles carrying the proof and the derived child tokens.
		if !path.as_os_str().is_empty() {
			let directory = tg::Artifact::with_referent(referent)
				.try_unwrap_directory()
				.ok()
				.ok_or_else(|| tg::error!("the root artifact is not a directory"))?;
			let artifact = directory
				.get_with_handle(self, path)
				.await
				.map_err(|error| tg::error!(!error, "failed to resolve the artifact path"))?;
			let node = artifact
				.store_with_handle(self)
				.await
				.map_err(|error| tg::error!(!error, "failed to store the resolved artifact"))?;
			referent = tg::Referent::with_node(node);
			referent.options.id = Some(id.into());
			referent.options.path = Some(path.to_owned());
		}

		// Return exact tokens for the resolved artifact and its containing root.
		referent.options.tokens.clear();
		if referent.options.id.is_some()
			&& let Some(token) =
				self.create_token(referent.node.clone().into(), vec![subtree], expires_at)?
		{
			referent.options.tokens.insert_local(token);
		}
		if let Some(token) = root_token {
			referent.options.tokens.insert_local(token);
		}
		if let Some(sandbox) = &sandbox
			&& let Some(mut state) = self
				.server
				.runner
				.state()
				.sandboxes()
				.get_mut_by_id(sandbox)
		{
			for token in referent.options.tokens.local() {
				let id = token.body.resource.clone().try_into().unwrap();
				if state
					.tokens
					.get(&id)
					.is_none_or(|existing| existing.body.expires_at < token.body.expires_at)
				{
					state.tokens.insert(id, token.clone());
				}
			}
		}
		let output = tg::checkin::Output { artifact: referent };

		Ok(output)
	}

	fn try_get_checkin_origin_sandbox(&self) -> tg::Result<Option<tg::sandbox::Id>> {
		let Some(sandbox) = self
			.server
			.try_get_request_origin_sandbox(self.context.origin)?
		else {
			return Ok(None);
		};
		let authorized = match &self.context.principal {
			tg::Principal::Process(id) => sandbox.processes.get_by_id(id).is_some(),
			tg::Principal::Sandbox(id) => sandbox.id.as_ref() == Some(id),
			_ => false,
		};
		let id = authorized.then(|| sandbox.id.clone()).flatten();

		Ok(id)
	}

	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn checkin_store(
		&self,
		args: Vec<crate::cache::object::put::Arg>,
		progress: &crate::progress::Handle<super::TaskOutput>,
	) -> tg::Result<()> {
		progress.spinner("storing", "storing");
		self.server
			.put_object_batch(args)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the objects"))?;
		progress.finish("storing");
		Ok(())
	}
}
