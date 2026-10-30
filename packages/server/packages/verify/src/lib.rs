use {
	self::proofs::Proofs,
	futures::{FutureExt as _, StreamExt as _, future::BoxFuture, stream::FuturesUnordered},
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_index::prelude::*,
	tangram_server_core::{
		Session,
		authorization::{Outcome, Output as Authorization},
		verify::{Arg, Output, VerifyBatchArg, empty_storage},
	},
};

mod proofs;
#[cfg(test)]
mod tests;

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

struct Item {
	bodies: Vec<tg::authorization::Body>,
	error: Option<tg::Error>,
	exhausted: bool,
	proofs: Proofs,
	requested: tg::authorization::permission::Set,
	required: tg::authorization::permission::Set,
	resource: tg::Selector<tg::Id>,
	storage: tg::storage::Set,
	stored: tg::storage::Set,
	target: tg::authorization::permission::Set,
	tokens: Vec<tg::authorization::Token>,
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn verify_batch(
		&self,
		session: &Session,
		arg: VerifyBatchArg<'_>,
	) -> tg::Result<Vec<Output>> {
		let VerifyBatchArg {
			args,
			current_sync,
			required,
			search,
			subject,
		} = arg;

		// Create the items.
		let mut items = args
			.into_iter()
			.map(|arg| Item::new(arg, required, subject.as_ref()))
			.collect::<tg::Result<Vec<_>>>()?;

		// Prove the items directly.
		self.verify_direct(session, &mut items, subject.as_ref())
			.await?;

		// Ask the syncs named by the tokens.
		Self::verify_syncs(session, &mut items, current_sync).await?;

		// Search the index as a last resort, or read only the principal's permissions from the index when searching is not allowed.
		if search {
			self.verify_search(session, &mut items, subject.as_ref())
				.await?;
		} else if subject.is_none() {
			Self::verify_permissions(session, &mut items).await?;
		}

		// Return the error of a sync for an item that remains unproven.
		for item in &mut items {
			if !item.proven()
				&& let Some(error) = item.error.take()
			{
				return Err(tg::error!(
					!error,
					"failed to verify the resource through a sync"
				));
			}
		}

		// Create the outputs.
		let requirements = items
			.iter()
			.map(|item| (item.required, item.storage))
			.collect::<Vec<_>>();
		let exhausted = items.iter().map(|item| item.exhausted).collect::<Vec<_>>();
		let stored = items.iter().map(|item| item.stored).collect();
		let outputs = items
			.iter()
			.map(|item| item.proofs.output(item.requested))
			.collect();
		let outputs = Self::verify_outputs(outputs, stored, &requirements, &exhausted);

		Ok(outputs)
	}

	async fn verify_direct(
		&self,
		session: &Session,
		items: &mut [Item],
		subject: Option<&tg::authorization::Subject>,
	) -> tg::Result<()> {
		for item in items.iter_mut() {
			// Prove the permissions of the exact tokens.
			if let tg::Selector::Id(id) = &item.resource {
				let (exact, other) = std::mem::take(&mut item.tokens)
					.into_iter()
					.partition::<Vec<_>, _>(|token| token.body.resource == *id);
				item.tokens = other;
				for token in exact {
					if !session
						.server
						.components
						.authorization
						.verify_token(session, &token)
					{
						continue;
					}
					item.insert_body(&token.body);
					item.bodies.push(token.body);
				}
			}

			// Prove the permissions that the principal has on every resource or on its own resource.
			if subject.is_none()
				&& (matches!(session.context.principal, tg::Principal::Root)
					|| Self::principal_is_resource(session, item)
					|| self.sandbox_owns_process(session, item).await?)
			{
				item.insert_all();
			}
		}

		// Read the storage.
		Self::read_storage(session, items).await?;

		Ok(())
	}

	fn principal_is_resource(session: &Session, item: &Item) -> bool {
		let tg::Selector::Id(id) = &item.resource else {
			return false;
		};
		session.context.principal.to_id().as_ref() == Some(id)
			&& matches!(
				(item.requested, &session.context.principal),
				(
					tg::authorization::permission::Set::Process(_),
					tg::Principal::Process(_)
				) | (
					tg::authorization::permission::Set::Sandbox(_),
					tg::Principal::Sandbox(_)
				) | (
					tg::authorization::permission::Set::User(_),
					tg::Principal::User(_)
				)
			)
	}

	async fn sandbox_owns_process(&self, session: &Session, item: &Item) -> tg::Result<bool> {
		let (
			tg::Selector::Id(id),
			tg::authorization::permission::Set::Process(_),
			tg::Principal::Sandbox(sandbox),
		) = (&item.resource, item.requested, &session.context.principal)
		else {
			return Ok(false);
		};
		let Ok(process) = tg::process::Id::try_from(id.clone()) else {
			return Ok(false);
		};
		if let Some(process_sandbox) = session
			.server
			.components
			.runner
			.state()
			.try_get_process_sandbox(&process)
		{
			return Ok(process_sandbox == *sandbox);
		}
		let output = session
			.server
			.components
			.process
			.try_get_process_local_inner(session, &process, false, tg::process::Source::Auto)
			.await?;
		let owns = output.is_some_and(|output| output.data.sandbox.as_ref() == Some(sandbox));

		Ok(owns)
	}

	async fn verify_syncs(
		session: &Session,
		items: &mut [Item],
		current_sync: Option<&tg::sync::Id>,
	) -> tg::Result<()> {
		// Ask each sync named by a valid token for what each unproven item is missing.
		let mut aborts = vec![Vec::new(); items.len()];
		let mut futures = FuturesUnordered::new();
		for (position, item) in items.iter().enumerate() {
			if item.proven() {
				continue;
			}
			let tg::Selector::Id(id) = &item.resource else {
				continue;
			};
			if !matches!(
				item.requested,
				tg::authorization::permission::Set::Object(_)
					| tg::authorization::permission::Set::Process(_)
			) {
				continue;
			}
			let syncs = item
				.tokens
				.iter()
				.filter_map(|token| {
					session
						.server
						.components
						.authorization
						.try_get_sync_id_from_token(session, token)
				})
				.filter(|sync| current_sync != Some(sync))
				.collect::<BTreeSet<_>>();
			let permissions = item.missing_permissions();
			let storage = if item.stored.contains(item.storage) {
				empty_storage(permissions)
			} else {
				item.storage
			};
			for sync in syncs {
				let arg = tg::sync::service::VerifyArg {
					node: id.clone(),
					permissions,
					storage,
				};
				let (abort, registration) = futures::future::AbortHandle::new_pair();
				aborts[position].push(abort);
				let future = async move {
					session
						.server
						.components
						.sync
						.request_sync_verification(&sync, arg)
						.await
				};
				let future = futures::future::Abortable::new(future, registration)
					.map(move |result| (position, result.ok()));
				futures.push(future.boxed());
			}
		}

		// Apply each answer, and stop asking for an item once it is proven.
		while let Some((position, result)) = futures.next().await {
			let item = &mut items[position];
			match result {
				None | Some(Ok(None)) => {},
				Some(Ok(Some(output))) => {
					item.insert_sync_output(session, &output)?;
				},
				Some(Err(error)) => {
					tracing::trace!(%error, "a sync verification request failed");
					item.error.get_or_insert(error);
				},
			}
			if item.proven() {
				for abort in &aborts[position] {
					abort.abort();
				}
			}
		}

		Ok(())
	}

	async fn verify_permissions(session: &Session, items: &mut [Item]) -> tg::Result<()> {
		// Collect the resources of the items whose permissions are unproven.
		let mut ids = Vec::new();
		let mut positions = Vec::new();
		for (position, item) in items.iter().enumerate() {
			if item.permissions().contains(item.target) {
				continue;
			}
			let tg::Selector::Id(id) = &item.resource else {
				continue;
			};
			ids.push(id.clone());
			positions.push(position);
		}

		// Read the principal's permissions on the resources from the index.
		if !ids.is_empty() {
			let outputs = session
				.server
				.index
				.get_permissions(&ids, &session.context.principal)
				.await?;
			for (position, permissions) in std::iter::zip(positions, outputs) {
				items[position].insert_permissions(&permissions);
			}
		}

		// Read the storage.
		Self::read_storage(session, items).await?;

		Ok(())
	}

	async fn verify_search(
		&self,
		session: &Session,
		items: &mut [Item],
		subject: Option<&tg::authorization::Subject>,
	) -> tg::Result<()> {
		// Collect the bodies of the remaining valid tokens of the unproven items.
		let positions = Self::unproven(items);
		if positions.is_empty() {
			return Ok(());
		}
		for position in &positions {
			let item = &mut items[*position];
			for token in std::mem::take(&mut item.tokens) {
				if session
					.server
					.components
					.authorization
					.verify_token(session, &token)
				{
					item.bodies.push(token.body);
				}
			}
			let token_resource = item
				.bodies
				.iter()
				.map(|body| body.resource.to_string())
				.collect::<Vec<_>>()
				.join(",");
			tangram_server_core::checkpoint!(
				session.server,
				"verification.index",
				resource = %item.resource,
				storage = !item.stored.contains(item.storage),
				token_resource,
			)
			.await;
		}

		// Search with the initial configuration, and start awaiting indexing if the search outlasts the delay.
		let verification = &session.server.config.verification;
		let config = tangram_server_core::config::verification_search_config(
			&verification.permissions.initial,
		);
		let (search_positions, args) = Self::search_args(items, &positions, subject);
		let search = Self::search(session, args, config);
		tokio::pin!(search);
		let delayed = match verification.index.delay {
			Some(delay) => tokio::time::timeout(delay, &mut search).await.ok(),
			None => Some((&mut search).await),
		};
		let indexed = if let Some(outputs) = delayed {
			Self::apply_search(items, &search_positions, outputs?);
			false
		} else {
			let indexing = Self::await_indexing(session);
			tokio::pin!(indexing);
			tokio::select! {
				outputs = &mut search => {
					Self::apply_search(items, &search_positions, outputs?);
					Self::read_storage(session, items).await?;
					if Self::unproven(items).is_empty() {
						return Ok(());
					}
					indexing.await?;
				},
				result = &mut indexing => {
					result?;
					Self::apply_search(items, &search_positions, search.await?);
				},
			}
			true
		};
		Self::read_storage(session, items).await?;

		// Await indexing, then search with the final configuration.
		let positions = Self::unproven(items);
		if positions.is_empty() {
			return Ok(());
		}
		if !indexed {
			Self::await_indexing(session).await?;
		}
		let config = tangram_server_core::config::verification_search_config(
			&verification.permissions.final_,
		);
		let (search_positions, args) = Self::search_args(items, &positions, subject);
		let outputs = Self::search(session, args, config).await?;
		Self::apply_search(items, &search_positions, outputs);
		Self::read_storage(session, items).await?;

		// Prove the permissions of processes that the index has not recorded through their parents.
		for position in Self::unproven(items) {
			self.verify_process_parent(session, &mut items[position], config, subject)
				.await?;
		}

		Ok(())
	}

	fn search_args(
		items: &[Item],
		positions: &[usize],
		subject: Option<&tg::authorization::Subject>,
	) -> (Vec<usize>, Vec<tangram_index::verify::Arg>) {
		// Search only for the missing permissions.
		let mut args = Vec::new();
		let mut search_positions = Vec::new();
		for position in positions {
			let item = &items[*position];
			let permissions = item.missing_permissions();
			if permissions.is_empty() {
				continue;
			}
			let arg = tangram_index::verify::Arg {
				requested: permissions,
				required: permissions,
				resource: item.resource.clone(),
				subject: subject.cloned(),
				tokens: item.bodies.clone(),
			};
			args.push(arg);
			search_positions.push(*position);
		}
		(search_positions, args)
	}

	async fn search(
		session: &Session,
		args: Vec<tangram_index::verify::Arg>,
		config: tangram_index::verify::Config,
	) -> tg::Result<Vec<tangram_index::verify::Output>> {
		if args.is_empty() {
			return Ok(Vec::new());
		}
		session
			.server
			.index
			.verify_batch(&args, config, &session.context.principal)
			.await
	}

	fn apply_search(
		items: &mut [Item],
		positions: &[usize],
		outputs: Vec<tangram_index::verify::Output>,
	) {
		for (position, output) in std::iter::zip(positions, outputs) {
			let item = &mut items[*position];
			item.exhausted = output.outcome == Outcome::Exhausted;
			let output = Authorization {
				expires_at: output.expires_at,
				outcome: output.outcome,
				permissions: output.permissions,
			};
			item.proofs.insert(item.requested, output);
		}
	}

	async fn await_indexing(session: &Session) -> tg::Result<()> {
		tangram_server_core::checkpoint!(session.server, "verification.index.wait").await;
		session
			.server
			.components
			.indexer
			.index(session, tg::index::Arg::default())
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

		Ok(())
	}

	async fn read_storage(session: &Session, items: &mut [Item]) -> tg::Result<()> {
		// Collect the resources whose storage is missing.
		let mut objects = Vec::new();
		let mut processes = Vec::new();
		for (position, item) in items.iter().enumerate() {
			if item.stored.contains(item.storage) {
				continue;
			}
			let tg::Selector::Id(id) = &item.resource else {
				return Err(tg::error!("a storage requirement requires a resource id"));
			};
			match item.storage {
				tg::storage::Set::Object(_) => {
					let id = tg::object::Id::try_from(id.clone()).map_err(|_| {
						tg::error!("the storage requirement does not match the resource")
					})?;
					objects.push((position, id));
				},
				tg::storage::Set::Process(_) => {
					let id = tg::process::Id::try_from(id.clone()).map_err(|_| {
						tg::error!("the storage requirement does not match the resource")
					})?;
					processes.push((position, id));
				},
			}
		}

		// Read the storage from the index.
		if !objects.is_empty() {
			let ids = objects.iter().map(|(_, id)| id.clone()).collect::<Vec<_>>();
			let outputs = session.server.index.try_get_objects(&ids).await?;
			for ((position, _), output) in std::iter::zip(objects, outputs) {
				if let Some(object) = output {
					items[position]
						.stored
						.insert(tg::storage::Set::Object(object.storage));
				}
			}
		}
		if !processes.is_empty() {
			let ids = processes
				.iter()
				.map(|(_, id)| id.clone())
				.collect::<Vec<_>>();
			let outputs = session.server.index.try_get_processes(&ids).await?;
			for ((position, _), output) in std::iter::zip(processes, outputs) {
				if let Some(process) = output {
					items[position]
						.stored
						.insert(tg::storage::Set::Process(process.storage));
				}
			}
		}

		Ok(())
	}

	/// Grant the permissions on a process that the index has not recorded whose subtree forms the process's parent grants, finding the parent in the runner's state or the process's control service.
	async fn verify_process_parent(
		&self,
		session: &Session,
		item: &mut Item,
		config: tangram_index::verify::Config,
		subject: Option<&tg::authorization::Subject>,
	) -> tg::Result<()> {
		// Skip every item other than one of missing process permissions without storage.
		let tg::Selector::Id(resource) = &item.resource else {
			return Ok(());
		};
		let Ok(process) = tg::process::Id::try_from(resource.clone()) else {
			return Ok(());
		};
		let tg::authorization::permission::Set::Process(missing) = item.missing_permissions()
		else {
			return Ok(());
		};
		if missing.is_empty() || !item.storage.is_empty() {
			return Ok(());
		}

		// Find the parent of a process that the index has not recorded.
		let Some(parent) = self
			.try_get_unindexed_process_parent(session, &process)
			.await?
		else {
			return Ok(());
		};

		// Search for the subtree forms of the missing permissions on the parent.
		let mut subtree = tg::authorization::permission::process::Set::empty();
		for permission in missing.iter() {
			subtree.insert(
				tg::authorization::permission::process::Set::from_permission(
					permission.to_subtree(),
				),
			);
		}
		let subtree = tg::authorization::permission::Set::Process(subtree);
		let arg = tangram_index::verify::Arg {
			requested: subtree,
			required: subtree,
			resource: tg::Selector::Id(parent.into()),
			subject: subject.cloned(),
			tokens: item.bodies.clone(),
		};
		let output = session
			.server
			.index
			.verify_batch(&[arg], config, &session.context.principal)
			.await?
			.pop()
			.ok_or_else(|| tg::error!("expected a parent verification outcome"))?;
		let tg::authorization::permission::Set::Process(parent_permissions) = output.permissions
		else {
			return Ok(());
		};

		// Grant each permission whose subtree form the parent grants.
		let mut granted = tg::authorization::permission::process::Set::empty();
		for permission in missing.iter() {
			let subtree = tg::authorization::permission::process::Set::from_permission(
				permission.to_subtree(),
			);
			if parent_permissions.contains(subtree) {
				granted.insert(
					tg::authorization::permission::process::Set::from_permission(permission),
				);
			}
		}
		if granted.is_empty() {
			return Ok(());
		}
		let output = Authorization {
			expires_at: output.expires_at,
			outcome: Outcome::Satisfied,
			permissions: tg::authorization::permission::Set::Process(granted),
		};
		item.proofs.insert(item.requested, output);

		Ok(())
	}

	/// Get the parent of a process that the index has not recorded, from the runner's state or, waiting for the process's start, from its control service.
	async fn try_get_unindexed_process_parent(
		&self,
		session: &Session,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Id>> {
		// Use the parent of a process that this runner runs.
		let state = session.server.components.runner.state();
		if let Some(sandbox) = state
			.processes()
			.get(process)
			.map(|entry| entry.value().clone())
			&& let Some(sandbox) = state.sandboxes().get_by_id(&sandbox)
			&& let Some(entry) = sandbox.processes.get(process)
		{
			return Ok(entry.parent.clone());
		}

		// Skip a process that the index has recorded, whose parent the index search already follows.
		if session
			.server
			.index
			.try_get_process(process)
			.await?
			.is_some()
		{
			return Ok(None);
		}

		// Skip a process whose ID is outside the proof window, since a process that is still starting has a recent ID.
		let now = session.server.clock.unix_timestamp();
		let now = now.to_u64().unwrap_or_default() * 1000;
		let window = session.server.config.process.id_proof_window;
		let window = window.as_millis().try_into().unwrap_or(u64::MAX);
		if process
			.timestamp()
			.is_none_or(|timestamp| timestamp.abs_diff(now) > window)
		{
			return Ok(None);
		}

		// Ask the process's control service.
		session
			.server
			.components
			.process
			.try_get_process_parent_from_control_service(session, process)
			.await
	}

	fn unproven(items: &[Item]) -> Vec<usize> {
		items
			.iter()
			.enumerate()
			.filter(|(_, item)| !item.proven())
			.map(|(position, _)| position)
			.collect()
	}

	fn verify_outputs(
		outputs: Vec<Option<Authorization>>,
		stored: Vec<tg::storage::Set>,
		requirements: &[(tg::authorization::permission::Set, tg::storage::Set)],
		exhausted: &[bool],
	) -> Vec<Output> {
		std::iter::zip(outputs, stored)
			.enumerate()
			.map(|(position, (output, storage))| {
				let (required_permissions, required_storage) = requirements[position];
				let output = output.unwrap_or(Authorization {
					expires_at: None,
					outcome: Outcome::Unsatisfied,
					permissions: required_permissions.empty_like(),
				});
				let permissions_satisfied = output.permissions.contains(required_permissions);
				let outcome = if permissions_satisfied && storage.contains(required_storage) {
					Outcome::Satisfied
				} else if exhausted[position] {
					Outcome::Exhausted
				} else {
					Outcome::Unsatisfied
				};
				Output {
					expires_at: output.expires_at,
					outcome,
					permissions: output.permissions,
					storage,
				}
			})
			.collect()
	}
}

impl Item {
	fn new(
		arg: Arg,
		required: Option<tg::authorization::permission::Set>,
		subject: Option<&tg::authorization::Subject>,
	) -> tg::Result<Self> {
		let Arg {
			permissions: requested,
			resource,
			storage,
			tokens,
			trusted,
		} = arg;
		let required = required.unwrap_or(requested);
		if !requested.contains(required) {
			return Err(tg::error!(
				"the required permissions must be contained in the requested permissions"
			));
		}

		// Prove every requested permission for a subject, which has no optional permissions, and only the required permissions otherwise.
		let target = if subject.is_some() {
			requested
		} else {
			required
		};

		// Scoped verification must not borrow the caller's authority.
		let (tokens, trusted) = if subject.is_some() {
			(Vec::new(), None)
		} else {
			(tokens, trusted)
		};

		let mut item = Self {
			bodies: Vec::new(),
			error: None,
			exhausted: false,
			proofs: Proofs::default(),
			requested,
			required,
			resource,
			storage,
			stored: storage.empty_like(),
			target,
			tokens,
		};

		// Prove the empty permissions and the trusted permissions.
		let permissions = trusted.unwrap_or_else(|| requested.empty_like());
		let output = Authorization {
			expires_at: None,
			outcome: Outcome::Satisfied,
			permissions,
		};
		item.proofs.insert(requested, output);

		Ok(item)
	}

	fn permissions(&self) -> tg::authorization::permission::Set {
		self.proofs
			.output(self.requested)
			.map_or_else(|| self.requested.empty_like(), |output| output.permissions)
	}

	fn missing_permissions(&self) -> tg::authorization::permission::Set {
		let mut permissions = self.target;
		permissions.remove(self.permissions());
		permissions
	}

	fn proven(&self) -> bool {
		self.permissions().contains(self.target) && self.stored.contains(self.storage)
	}

	fn insert_all(&mut self) {
		let output = Authorization {
			expires_at: None,
			outcome: Outcome::Satisfied,
			permissions: self.requested,
		};
		self.proofs.insert(self.requested, output);
	}

	fn insert_body(&mut self, body: &tg::authorization::Body) {
		let mut permissions = self.requested.empty_like();
		for permission in self
			.requested
			.iter()
			.filter(|permission| body.authorizes(*permission))
		{
			permissions.insert(tg::authorization::permission::Set::from_permission(
				permission,
			));
		}
		let output = Authorization {
			expires_at: Some(body.expires_at),
			outcome: Outcome::Satisfied,
			permissions,
		};
		self.proofs.insert(self.requested, output);
	}

	fn insert_permissions(&mut self, permissions: &[tg::authorization::Permission]) {
		let mut output = self.requested.empty_like();
		for permission in self.requested.iter().filter(|requested| {
			permissions
				.iter()
				.any(|permission| permission.implies(*requested))
		}) {
			output.insert(tg::authorization::permission::Set::from_permission(
				permission,
			));
		}
		let output = Authorization {
			expires_at: None,
			outcome: Outcome::Satisfied,
			permissions: output,
		};
		self.proofs.insert(self.requested, output);
	}

	fn insert_sync_output(
		&mut self,
		session: &Session,
		output: &tg::sync::service::Output,
	) -> tg::Result<()> {
		let tg::Selector::Id(id) = self.resource.clone() else {
			return Err(tg::error!("expected an ID for a sync verification"));
		};
		let storage = match output {
			tg::sync::service::Output::Object(output) => tg::storage::Set::Object(output.storage),
			tg::sync::service::Output::Process(output) => tg::storage::Set::Process(output.storage),
		};
		self.stored.insert(storage);
		let now = session.server.clock.unix_timestamp();
		for body in output.tokens() {
			if body.resource != id {
				return Err(tg::error!(
					"received an authorization body for the wrong resource from a sync"
				));
			}
			body.validate_at(now).map_err(|error| {
				tg::error!(!error, "received an invalid authorization body from a sync")
			})?;
			self.insert_body(body);
			if !self.bodies.contains(body) {
				self.bodies.push(body.clone());
			}
		}

		Ok(())
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::verify::Component for Component {
	fn verify_batch<'a>(
		&'a self,
		session: &'a Session,
		arg: VerifyBatchArg<'a>,
	) -> BoxFuture<'a, tg::Result<Vec<Output>>> {
		self.verify_batch(session, arg).boxed()
	}
}
