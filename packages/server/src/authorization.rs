use {
	crate::Session, futures::FutureExt as _, tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _, tangram_index::prelude::*,
};

mod token;
pub(crate) mod trace;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Output {
	pub expires_at: Option<i64>,
	pub permissions: tg::authorization::permission::Set,
}

impl Session {
	pub(crate) fn create_token(
		&self,
		resource: tg::Id,
		permissions: Vec<tg::authorization::Permission>,
		expires_at: i64,
	) -> tg::Result<Option<tg::authorization::Token>> {
		let Some(private_key) = self.server.authorization_tokens.private_key.as_ref() else {
			return Ok(None);
		};
		let body = tg::authorization::Body {
			expires_at,
			permissions,
			resource,
		};
		let token = tg::authorization::Token::sign(body, private_key)?;
		Ok(Some(token))
	}

	#[track_caller]
	pub(crate) fn authorize(
		&self,
		resource: impl IntoAuthorizationResource,
		permissions: impl Into<tg::authorization::permission::Set>,
	) -> impl Future<Output = tg::Result<Option<tg::authorization::permission::Set>>> {
		let future = self.authorize_batch([(resource, permissions.into())]);
		async move {
			let mut outputs = future.await?;
			Ok(outputs.pop().unwrap())
		}
	}

	#[track_caller]
	pub(crate) fn authorize_batch<R, I>(
		&self,
		args: I,
	) -> impl Future<Output = tg::Result<Vec<Option<tg::authorization::permission::Set>>>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		let span = trace::span(self, std::panic::Location::caller(), "authorize_batch");
		trace::run(span, async move {
			let args = args
				.into_iter()
				.map(|(resource, permissions)| (resource, permissions, None));
			let outputs = self.authorize_batch_inner(args, None, false).await?;
			let outputs = outputs
				.into_iter()
				.map(|output| output.map(|output| output.permissions))
				.collect();
			Ok(outputs)
		})
	}

	#[track_caller]
	pub(crate) fn authorize_batch_with_required<R, I>(
		&self,
		args: I,
		required: tg::authorization::permission::Set,
	) -> impl Future<Output = tg::Result<Vec<Option<tg::authorization::permission::Set>>>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = (R, tg::authorization::permission::Set)>,
	{
		let span = trace::span(
			self,
			std::panic::Location::caller(),
			"authorize_batch_with_required",
		);
		trace::run(span, async move {
			let args = args
				.into_iter()
				.map(|(resource, permissions)| (resource, permissions, None));
			let outputs = self
				.authorize_batch_inner(args, Some(required), false)
				.await?;
			let outputs = outputs
				.into_iter()
				.map(|output| output.map(|output| output.permissions))
				.collect();
			Ok(outputs)
		})
	}

	#[track_caller]
	pub(crate) fn authorize_object_read(
		&self,
		resource: impl IntoAuthorizationResource,
		wait_for_subtree: bool,
	) -> impl Future<Output = tg::Result<Option<Output>>> {
		let future = self.authorize_object_read_batch([resource], wait_for_subtree);
		async move {
			let mut outputs = future.await?;
			Ok(outputs.pop().unwrap())
		}
	}

	#[track_caller]
	pub(crate) fn authorize_object_read_batch<R, I>(
		&self,
		resources: I,
		wait_for_subtree: bool,
	) -> impl Future<Output = tg::Result<Vec<Option<Output>>>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<Item = R>,
	{
		let span = trace::span(
			self,
			std::panic::Location::caller(),
			"authorize_object_read_batch",
		);
		trace::run(span, async move {
			// Request the optional subtree permission while requiring the node permission.
			let mut requested = tg::authorization::permission::object::Set::empty();
			requested.insert(tg::authorization::permission::object::Set::NODE);
			requested.insert(tg::authorization::permission::object::Set::SUBTREE);
			let requested = tg::authorization::permission::Set::Object(requested);
			let required = tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			);
			let args = resources
				.into_iter()
				.map(|resource| (resource, requested, None));

			self.authorize_batch_inner(args, Some(required.into()), wait_for_subtree)
				.await
		})
	}

	#[track_caller]
	pub(crate) fn authorize_with_permissions(
		&self,
		resource: impl IntoAuthorizationResource,
		requested: tg::authorization::permission::Set,
		required: tg::authorization::permission::Set,
		proven: tg::authorization::permission::Set,
	) -> impl Future<Output = tg::Result<Option<Output>>> {
		let span = trace::span(
			self,
			std::panic::Location::caller(),
			"authorize_with_permissions",
		);
		trace::run(span, async move {
			let args = [(resource, requested, Some(proven))];
			let mut outputs = self
				.authorize_batch_inner(args, Some(required), false)
				.await?;
			Ok(outputs.pop().unwrap())
		})
	}

	async fn authorize_batch_inner<R, I>(
		&self,
		args: I,
		required: Option<tg::authorization::permission::Set>,
		wait_for_requested_permissions: bool,
	) -> tg::Result<Vec<Option<Output>>>
	where
		R: IntoAuthorizationResource,
		I: IntoIterator<
			Item = (
				R,
				tg::authorization::permission::Set,
				Option<tg::authorization::permission::Set>,
			),
		>,
	{
		tracing::debug!(target: "tangram_authz", wait_for_requested_permissions, "authz.batch");
		let mut outputs = Vec::new();
		let mut index_args = Vec::new();
		let mut index_positions = Vec::new();

		for (position, (resource, permissions, trusted)) in args.into_iter().enumerate() {
			let required = required.unwrap_or(permissions);
			if !permissions.contains(required) {
				return Err(tg::error!(
					"the required permissions must be contained in the requested permissions"
				));
			}
			let (resource, mut tokens) = resource.into_authorization_resource();
			let started = trace::start();
			let token_count = tokens.len();
			tracing::debug!(target: "tangram_authz", position, resource = %resource,
				requested = %permissions, %required, ?trusted, tokens = tokens.len(), "authz.resource");
			for (token_index, token) in tokens.iter().enumerate() {
				tracing::debug!(target: "tangram_authz", position, token_index,
					token_resource = %token.body.resource, permissions = ?token.body.permissions,
					expires_at = token.body.expires_at, key = %token.metadata.key, "authz.token_input");
			}

			// Try exact proofs first, without verifying unrelated ancestor tokens.
			tokens.sort_by_key(|token| std::cmp::Reverse(token.body.expires_at));
			let mut verified = vec![None; tokens.len()];
			if let tg::Selector::Id(id) = &resource {
				let mut proven = permissions.empty_like();
				if let Some(trusted) = trusted {
					for permission in permissions
						.iter()
						.filter(|permission| trusted.iter().any(|proof| proof.implies(*permission)))
					{
						proven.insert(tg::authorization::permission::Set::from_permission(
							permission,
						));
					}
				}
				let mut expires_at = i64::MAX;
				for (index, token) in tokens.iter().enumerate() {
					if &token.body.resource != id
						|| !permissions.iter().any(|permission| {
							!proven.contains(permission) && token.body.grants(permission)
						}) {
						continue;
					}
					let valid = self.verify_token(token);
					verified[index] = Some(valid);
					if !valid {
						continue;
					}
					for permission in permissions
						.iter()
						.filter(|permission| token.body.grants(*permission))
					{
						proven.insert(tg::authorization::permission::Set::from_permission(
							permission,
						));
					}
					expires_at = expires_at.min(token.body.expires_at);
					if proven.contains(permissions) {
						break;
					}
				}
				if proven.contains(permissions)
					|| (proven.contains(required)
						&& (trusted.is_some()
							|| matches!(required, tg::authorization::permission::Set::Process(_)))
						&& !wait_for_requested_permissions
						&& !matches!(self.context.principal, tg::Principal::Root))
				{
					let output = Output {
						expires_at: (expires_at != i64::MAX).then_some(expires_at),
						permissions: proven,
					};
					tracing::debug!(target: "tangram_authz", position, resource = %resource,
						path = "proof", permissions = %proven, elapsed_us = trace::elapsed(started), "authz.resource_result");
					outputs.push(Some(output));
					continue;
				}
			}

			// Authorize the root principal for all resources.
			if matches!(self.context.principal, tg::Principal::Root) {
				let output = Output {
					expires_at: None,
					permissions,
				};
				tracing::debug!(target: "tangram_authz", position, resource = %resource,
					path = "root", %permissions, elapsed_us = trace::elapsed(started), "authz.resource_result");
				outputs.push(Some(output));
				continue;
			}

			// Authorize a sandbox for its own processes.
			if let (
				tg::Selector::Id(id),
				tg::authorization::permission::Set::Process(_),
				tg::Principal::Sandbox(sandbox),
			) = (&resource, permissions, &self.context.principal)
				&& let Ok(process) = tg::process::Id::try_from(id.clone())
				&& match self.server.runner.state().try_get_process_sandbox(&process) {
					Some(process_sandbox) => process_sandbox == *sandbox,
					None => self
						.try_get_process_local_inner(&process, false)
						.boxed()
						.await?
						.is_some_and(|output| output.data.sandbox == *sandbox),
				} {
				let output = Output {
					expires_at: None,
					permissions,
				};
				tracing::debug!(target: "tangram_authz", position, resource = %resource,
					path = "sandbox_process", %permissions, elapsed_us = trace::elapsed(started), "authz.resource_result");
				outputs.push(Some(output));
				continue;
			}

			outputs.push(None);
			let mut tokens: Vec<_> = std::iter::zip(tokens, verified)
				.filter_map(|(token, verified)| {
					verified
						.unwrap_or_else(|| self.verify_token(&token))
						.then_some(token.body)
				})
				.collect();
			let valid_tokens = tokens.len();
			if let Some(permissions) = trusted {
				let tg::Selector::Id(resource) = &resource else {
					return Err(tg::error!("expected an ID for the authorization proof"));
				};
				let proof = tg::authorization::Body {
					expires_at: i64::MAX,
					permissions: permissions.iter().collect(),
					resource: resource.clone(),
				};
				tokens.push(proof);
			}

			let reason = if token_count == 0 && trusted.is_none() {
				"missing_tokens"
			} else if tokens.is_empty() {
				"no_valid_tokens"
			} else {
				"insufficient_proof"
			};
			tracing::debug!(target: "tangram_authz", position, index_position = index_args.len(),
				resource = %resource, reason, valid_tokens, valid_proofs = tokens.len(),
				preparation_us = trace::elapsed(started), "authz.index_required");
			index_positions.push(position);
			index_args.push(tangram_index::authorize::Arg {
				required,
				requested: permissions,
				resource,
				tokens,
			});
		}

		if index_args.is_empty() {
			return Ok(outputs);
		}
		let index_outcomes = self
			.authorize_batch_index(&index_args, wait_for_requested_permissions)
			.boxed()
			.await?;
		for (position, outcome) in std::iter::zip(index_positions, index_outcomes) {
			tracing::debug!(target: "tangram_authz", position, path = "index", ?outcome, "authz.resource_result");
			let output = match outcome {
				tangram_index::authorize::Outcome::Authorized(output) => Some(output),
				tangram_index::authorize::Outcome::Denied(output) => output,
				outcome @ tangram_index::authorize::Outcome::Exhausted => {
					Some(outcome.into_result()?)
				},
			};
			if let Some(output) = output {
				let output = Output {
					expires_at: output.expires_at,
					permissions: output.permissions,
				};
				outputs[position] = Some(output);
			}
		}

		Ok(outputs)
	}

	async fn authorize_batch_index(
		&self,
		index_args: &[tangram_index::authorize::Arg],
		wait_for_requested_permissions: bool,
	) -> tg::Result<Vec<tangram_index::authorize::Outcome>> {
		for arg in index_args {
			let token_resource = arg
				.tokens
				.iter()
				.map(|body| body.resource.to_string())
				.collect::<Vec<_>>()
				.join(",");
			crate::checkpoint!(
				self.server,
				"authorization.index",
				resource = %arg.resource,
				token_resource,
			)
			.await;
		}

		// Run at most one authorization search at a time while indexing catches up.
		let authorization = &self.server.config.authorization;
		let delay = authorization.index.delay;
		let initial_config = crate::authorization_search_config(&authorization.initial);
		let initial_is_sufficient = |outcomes: &[tangram_index::authorize::Outcome]| {
			outcomes.len() == index_args.len()
				&& std::iter::zip(outcomes, index_args).all(|(outcome, arg)| {
					let permissions = if wait_for_requested_permissions {
						arg.requested
					} else {
						arg.required
					};
					outcome
						.output()
						.is_some_and(|output| output.permissions.contains(permissions))
				})
		};
		let initial = trace::stage(
			"initial",
			self.server
				.index
				.authorize_batch(index_args, initial_config, &self.context.principal),
		)
		.boxed();
		tokio::pin!(initial);
		let initial_result = match delay {
			Some(delay) => tokio::select! {
				result = &mut initial => Some(result),
				() = tokio::time::sleep(delay) => None,
			},
			None => Some((&mut initial).await),
		};
		let index_wait = trace::stage(
			"index_wait",
			async {
				self.index()
					.await
					.map_err(|error| tg::error!(!error, "failed to index"))?
					.try_last()
					.await
					.map_err(|error| tg::error!(!error, "failed to index"))?;

				Ok::<_, tg::Error>(())
			}
			.boxed(),
		);
		let final_config = crate::authorization_search_config(&authorization.final_);
		let final_authorization = || async {
			let outcomes = trace::stage(
				"final",
				self.server.index.authorize_batch(
					index_args,
					final_config,
					&self.context.principal,
				),
			)
			.boxed()
			.await?;

			ensure_authorization_search_complete(outcomes)
		};
		let index_outcomes = match initial_result {
			Some(Ok(outcomes)) if initial_is_sufficient(&outcomes) => outcomes,
			Some(Ok(_)) => {
				index_wait.await?;
				final_authorization().await?
			},
			Some(Err(error)) => return Err(error),
			None => {
				tokio::pin!(index_wait);
				tokio::select! {
					result = &mut initial => match result {
						Ok(outcomes) if initial_is_sufficient(&outcomes) => outcomes,
						Ok(_) => {
							index_wait.await?;
							final_authorization().await?
						},
						Err(error) => return Err(error),
					},
					result = &mut index_wait => match result {
						Ok(()) => match initial.await {
							Ok(outcomes) if initial_is_sufficient(&outcomes) => outcomes,
							Ok(_) | Err(_) => final_authorization().await?,
						},
						Err(error) => match initial.await {
							Ok(outcomes) if initial_is_sufficient(&outcomes) => outcomes,
							Ok(_) | Err(_) => return Err(error),
						},
					},
				}
			},
		};

		Ok(index_outcomes)
	}

	#[track_caller]
	pub(crate) fn authorize_owner(
		&self,
		owner: Option<&tg::Principal>,
	) -> impl Future<Output = tg::Result<()>> {
		let span = trace::span(self, std::panic::Location::caller(), "authorize_owner");
		trace::run(span, async move {
			let Some(owner) = owner else {
				return Ok(());
			};
			let authorized = match owner.to_id() {
				Some(id) => {
					let permission = Self::write_permission_for_resource(&id)?;
					self.authorize(tg::Selector::Id(id), permission)
						.await?
						.is_some_and(|permissions| permissions.contains(permission))
				},
				None => matches!(self.context.principal, tg::Principal::Root),
			};
			if !authorized {
				return Err(tg::error!("unauthorized"));
			}
			Ok(())
		})
	}

	#[track_caller]
	pub(crate) fn authorize_token(
		&self,
		resource: &tg::Selector<tg::Id>,
		permissions: tg::authorization::permission::Set,
		token: &tg::authorization::Token,
	) -> bool {
		let span = trace::span(self, std::panic::Location::caller(), "authorize_token");
		let _entered = span.enter();
		let started = trace::start();
		let authorized = (|| {
			if !matches!(resource, tg::Selector::Id(id) if token.body.resource == *id) {
				return false;
			}
			if !self.verify_token(token) {
				return false;
			}
			permissions
				.iter()
				.all(|permission| token.body.grants(permission))
		})();
		tracing::debug!(target: "tangram_authz", resource = %resource, %permissions,
			token_resource = %token.body.resource, token_permissions = ?token.body.permissions,
			expires_at = token.body.expires_at, authorized, elapsed_us = trace::elapsed(started), "authz.token_result");
		authorized
	}

	pub(crate) fn verify_local_token(&self, token: &tg::authorization::Token) -> bool {
		self.server
			.authorization_tokens
			.private_key
			.as_ref()
			.is_some_and(|private_key| private_key.name == token.metadata.key)
			&& self.verify_token(token)
	}

	pub(crate) fn verify_token(&self, token: &tg::authorization::Token) -> bool {
		let started = trace::start();
		let reason = (|| {
			let Ok(now) = self.server.clock.unix_timestamp() else {
				return "clock_error";
			};
			let Some(public_key) = self
				.server
				.authorization_tokens
				.public_keys
				.get(&token.metadata.key)
			else {
				return "unknown_key";
			};
			if token.verify_at(public_key, now).is_err() {
				return if token.body.expires_at <= now {
					"expired_or_invalid"
				} else {
					"invalid"
				};
			}
			"valid"
		})();
		tracing::debug!(target: "tangram_authz", token_resource = %token.body.resource,
			key = %token.metadata.key, reason, elapsed_us = trace::elapsed(started), "authz.token_verify");
		reason == "valid"
	}
}

pub(crate) trait IntoResource {
	fn into_resource(self) -> tg::Selector<tg::Id>;
}

pub(crate) trait IntoAuthorizationResource {
	fn into_authorization_resource(self) -> (tg::Selector<tg::Id>, Vec<tg::authorization::Token>);
}

impl IntoResource for tg::Id {
	fn into_resource(self) -> tg::Selector<tg::Id> {
		tg::Selector::Id(self)
	}
}

impl IntoResource for tg::object::Id {
	fn into_resource(self) -> tg::Selector<tg::Id> {
		tg::Selector::Id(self.into())
	}
}

impl IntoResource for tg::process::Id {
	fn into_resource(self) -> tg::Selector<tg::Id> {
		tg::Selector::Id(self.into())
	}
}

impl IntoResource for tg::sandbox::Id {
	fn into_resource(self) -> tg::Selector<tg::Id> {
		tg::Selector::Id(self.into())
	}
}

impl IntoResource for tg::artifact::Id {
	fn into_resource(self) -> tg::Selector<tg::Id> {
		tg::Selector::Id(tg::object::Id::from(self).into())
	}
}

impl<I> IntoResource for tg::Selector<I>
where
	I: Into<tg::Id>,
{
	fn into_resource(self) -> tg::Selector<tg::Id> {
		match self {
			tg::Selector::Id(id) => tg::Selector::Id(id.into()),
			tg::Selector::Specifier(specifier) => tg::Selector::Specifier(specifier),
		}
	}
}

impl<T> IntoAuthorizationResource for T
where
	T: IntoResource,
{
	fn into_authorization_resource(self) -> (tg::Selector<tg::Id>, Vec<tg::authorization::Token>) {
		(self.into_resource(), Vec::new())
	}
}

impl<T> IntoAuthorizationResource for tg::Referent<T>
where
	T: IntoResource,
{
	fn into_authorization_resource(self) -> (tg::Selector<tg::Id>, Vec<tg::authorization::Token>) {
		(
			self.node.into_resource(),
			self.options.tokens.local_authorization().to_vec(),
		)
	}
}

fn ensure_authorization_search_complete(
	outcomes: Vec<tangram_index::authorize::Outcome>,
) -> tg::Result<Vec<tangram_index::authorize::Outcome>> {
	if outcomes
		.iter()
		.any(|outcome| matches!(outcome, tangram_index::authorize::Outcome::Exhausted))
	{
		tangram_index::authorize::Outcome::Exhausted.into_result()?;
	}

	Ok(outcomes)
}
