use {
	futures::{FutureExt as _, future::BoxFuture},
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_server_core::Session,
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) fn create_token(
		session: &Session,
		resource: tg::Id,
		permissions: Vec<tg::authorization::Permission>,
		expires_at: i64,
	) -> tg::Result<Option<tg::authorization::Token>> {
		let Some(private_key) = session.server.authorization_tokens.private_key.as_ref() else {
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

	pub(crate) async fn authorize_owner(
		session: &Session,
		owner: Option<&tg::Principal>,
	) -> tg::Result<()> {
		let Some(owner) = owner else {
			return Ok(());
		};
		let authorized = match owner.to_id() {
			Some(id) => {
				let permission = tangram_server_grant_common::write_permission_for_resource(&id)?;
				session
					.authorize(tg::Selector::Id(id), permission)
					.await?
					.check_exhaustion()?
					.permissions
					.contains(permission)
			},
			None => matches!(session.context.principal, tg::Principal::Root),
		};
		if !authorized {
			return Err(tg::error!("unauthorized"));
		}
		Ok(())
	}

	#[must_use]
	pub(crate) fn authorize_token(
		session: &Session,
		resource: &tg::Selector<tg::Id>,
		permissions: tg::authorization::permission::Set,
		token: &tg::authorization::Token,
	) -> bool {
		if !matches!(resource, tg::Selector::Id(id) if token.body.resource == *id) {
			return false;
		}
		if !Self::verify_token(session, token) {
			return false;
		}
		permissions
			.iter()
			.all(|permission| token.body.authorizes(permission))
	}

	#[must_use]
	pub(crate) fn verify_local_token(session: &Session, token: &tg::authorization::Token) -> bool {
		session
			.server
			.authorization_tokens
			.private_key
			.as_ref()
			.is_some_and(|private_key| private_key.name == token.metadata.key)
			&& Self::verify_token(session, token)
	}

	#[must_use]
	pub(crate) fn verify_token(session: &Session, token: &tg::authorization::Token) -> bool {
		let now = session.server.clock.unix_timestamp();
		let Some(public_key) = session
			.server
			.authorization_tokens
			.public_keys
			.get(&token.metadata.key)
		else {
			return false;
		};
		if token.verify_at(public_key, now).is_err() {
			return false;
		}
		true
	}

	#[must_use]
	pub(crate) fn try_get_sync_id_from_token(
		session: &Session,
		token: &tg::authorization::Token,
	) -> Option<tg::sync::Id> {
		let id = token.body.resource.clone().try_into().ok()?;
		let permission = tg::authorization::Permission::Sync(
			tg::authorization::permission::sync::Permission::Read,
		);
		(Self::verify_token(session, token) && token.body.authorizes(permission)).then_some(id)
	}

	pub(crate) fn create_read_token(
		session: &Session,
		id: &tg::Id,
	) -> tg::Result<Option<tg::authorization::Token>> {
		let permission = tangram_server_grant_common::read_permission_for_resource(id)?;
		let expires_at = session.server.clock.unix_timestamp()
			+ session
				.server
				.config
				.sync
				.permission_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		Self::create_token(session, id.clone(), vec![permission], expires_at)
	}

	pub(crate) fn add_tokens_to_value_data(
		session: &Session,
		data: &mut tg::value::Data,
	) -> tg::Result<()> {
		let now = session.server.clock.unix_timestamp();
		let expires_at = now
			+ session
				.server
				.config
				.object
				.permission_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		Self::add_tokens_to_value_data_with_expires_at(session, data, expires_at)
	}

	pub(crate) fn add_permanent_token_to_object_reference(
		session: &Session,
		reference: &mut tg::Reference,
		resource: &tg::artifact::Id,
	) -> tg::Result<()> {
		let token = Self::create_permanent_object_token(session, resource)?;
		if let Some(token) = token {
			let mut options = reference.options().clone();
			options.tokens.insert_local_authorization(token);
			reference.set_options(options);
		}

		Ok(())
	}

	pub(crate) fn create_permanent_object_token(
		session: &Session,
		resource: &tg::artifact::Id,
	) -> tg::Result<Option<tg::authorization::Token>> {
		let expires_at = i64::MAX;
		let token = Self::create_token(
			session,
			resource.clone().into(),
			vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)],
			expires_at,
		)?;

		Ok(token)
	}

	pub(crate) fn update_tokens_and_location(
		session: &Session,
		tokens: &mut tg::authorization::Tokens,
		output_location: Option<&mut Option<tg::Location>>,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		if let Some(output_location) = output_location {
			*output_location = Some(location.clone());
		}
		if location.is_local() {
			return Ok(());
		}
		let Some(entry) = tokens.remove_local() else {
			return Ok(());
		};
		if trusted {
			// Trust the remote tokens by signing their exact permissions locally.
			for token in &entry.authorization {
				if token.body.resource.kind() == tg::id::Kind::Sync {
					continue;
				}
				let body = &token.body;
				if let Some(token) = Self::create_token(
					session,
					body.resource.clone(),
					body.permissions.clone(),
					body.expires_at,
				)? {
					tokens.insert_local_authorization(token);
				}
			}
		}
		for token in entry.authorization {
			tokens.insert_authorization(location.clone(), token);
		}
		Ok(())
	}

	pub(crate) fn update_value_data_referents_for_location(
		session: &Session,
		data: &mut tg::value::Data,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		match data {
			tg::value::Data::Array(array) => {
				for value in array {
					Self::update_value_data_referents_for_location(
						session, value, location, trusted,
					)?;
				}
			},
			tg::value::Data::Map(map) => {
				for value in map.values_mut() {
					Self::update_value_data_referents_for_location(
						session, value, location, trusted,
					)?;
				}
			},
			tg::value::Data::Module(module) => {
				Self::update_tokens_and_location(
					session,
					&mut module.referent.options.tokens,
					Some(&mut module.referent.options.location),
					location,
					trusted,
				)?;
			},
			tg::value::Data::Mutation(mutation) => {
				Self::update_mutation_data_referents_for_location(
					session, mutation, location, trusted,
				)?;
			},
			tg::value::Data::Object(object) => {
				Self::update_tokens_and_location(
					session,
					&mut object.options.tokens,
					Some(&mut object.options.location),
					location,
					trusted,
				)?;
			},
			tg::value::Data::Template(template) => {
				Self::update_template_data_referents_for_location(
					session, template, location, trusted,
				)?;
			},
			tg::value::Data::Bool(_)
			| tg::value::Data::Bytes(_)
			| tg::value::Data::Null
			| tg::value::Data::Number(_)
			| tg::value::Data::Placeholder(_)
			| tg::value::Data::String(_) => {},
		}
		Ok(())
	}

	pub(crate) fn update_process_data_referents_for_location(
		session: &Session,
		data: &mut tg::process::Data,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		Self::update_tokens_and_location(
			session,
			&mut data.command.options.tokens,
			Some(&mut data.command.options.location),
			location,
			trusted,
		)?;
		if let tg::Either::Left(command) = &mut data.command.node {
			Self::update_process_command_referents_for_location(
				session, command, location, trusted,
			)?;
		}
		if let Some(children) = &mut data.children {
			for child in children {
				Self::update_process_child_referent_for_location(
					session, child, location, trusted,
				)?;
			}
		}
		if let Some(error) = &mut data.error {
			Self::update_tokens_and_location(
				session,
				&mut error.options.tokens,
				Some(&mut error.options.location),
				location,
				trusted,
			)?;
			if let tg::Either::Left(error) = &mut error.node {
				Self::update_error_data_referents_for_location(session, error, location, trusted)?;
			}
		}
		if let Some(log) = &mut data.log {
			Self::update_tokens_and_location(
				session,
				&mut log.options.tokens,
				Some(&mut log.options.location),
				location,
				trusted,
			)?;
		}
		if let Some(output) = &mut data.output {
			Self::update_tokens_and_location(
				session,
				&mut output.options.tokens,
				Some(&mut output.options.location),
				location,
				trusted,
			)?;
			Self::update_value_data_referents_for_location(
				session,
				&mut output.node,
				location,
				trusted,
			)?;
		}
		Ok(())
	}

	pub(crate) fn update_process_child_referent_for_location(
		session: &Session,
		child: &mut tg::process::data::Child,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		// Preserve the child's region when translating the parent's location.
		let child_location = match (&child.process.options.location, location) {
			(Some(tg::Location::Local(local)), tg::Location::Local(parent)) => {
				tg::Location::Local(tg::location::Local {
					region: local.region.clone().or_else(|| parent.region.clone()),
				})
			},
			(Some(tg::Location::Local(local)), tg::Location::Remote(remote)) => {
				tg::Location::Remote(tg::location::Remote {
					name: remote.name.clone(),
					region: local.region.clone().or_else(|| remote.region.clone()),
				})
			},
			(Some(child_location), _) => child_location.clone(),
			(None, _) => location.clone(),
		};
		Self::update_tokens_and_location(
			session,
			&mut child.process.options.tokens,
			None,
			location,
			trusted,
		)?;
		child.process.options.location = Some(child_location);
		Ok(())
	}

	pub(crate) fn update_process_command_referents_for_location(
		session: &Session,
		command: &mut tg::process::data::Command,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		Self::update_tokens_and_location(
			session,
			&mut command.executable.options.tokens,
			Some(&mut command.executable.options.location),
			location,
			trusted,
		)?;
		if let Some(stdin) = &mut command.stdin {
			Self::update_tokens_and_location(
				session,
				&mut stdin.options.tokens,
				Some(&mut stdin.options.location),
				location,
				trusted,
			)?;
		}
		for value in command.args.iter_mut().chain(command.env.values_mut()) {
			let (tg::command::data::Value::String(value) | tg::command::data::Value::Value(value)) =
				value;
			Self::update_value_data_referents_for_location(session, value, location, trusted)?;
		}
		Ok(())
	}

	pub(crate) fn update_error_data_referents_for_location(
		session: &Session,
		data: &mut tg::error::Data,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		if let Some(diagnostics) = &mut data.diagnostics {
			for diagnostic in diagnostics {
				if let Some(location_data) = &mut diagnostic.location {
					Self::update_tokens_and_location(
						session,
						&mut location_data.module.referent.options.tokens,
						Some(&mut location_data.module.referent.options.location),
						location,
						trusted,
					)?;
				}
			}
		}
		if let Some(location_data) = &mut data.location {
			Self::update_error_location_data_referents_for_location(
				session,
				location_data,
				location,
				trusted,
			)?;
		}
		if let Some(source) = &mut data.source {
			Self::update_tokens_and_location(
				session,
				&mut source.options.tokens,
				Some(&mut source.options.location),
				location,
				trusted,
			)?;
			if let tg::Either::Left(error) = &mut source.node {
				Self::update_error_data_referents_for_location(session, error, location, trusted)?;
			}
		}
		if let Some(stack) = &mut data.stack {
			for location_data in stack {
				Self::update_error_location_data_referents_for_location(
					session,
					location_data,
					location,
					trusted,
				)?;
			}
		}
		Ok(())
	}

	pub(crate) fn update_error_location_data_referents_for_location(
		session: &Session,
		data: &mut tg::error::data::Location,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		if let tg::error::data::File::Module(module) = &mut data.file {
			Self::update_tokens_and_location(
				session,
				&mut module.referent.options.tokens,
				Some(&mut module.referent.options.location),
				location,
				trusted,
			)?;
		}
		Ok(())
	}

	pub(crate) fn add_tokens_to_value_data_with_expires_at(
		session: &Session,
		data: &mut tg::value::Data,
		expires_at: i64,
	) -> tg::Result<()> {
		match data {
			tg::value::Data::Array(array) => {
				for value in array {
					Self::add_tokens_to_value_data_with_expires_at(session, value, expires_at)?;
				}
			},
			tg::value::Data::Map(map) => {
				for value in map.values_mut() {
					Self::add_tokens_to_value_data_with_expires_at(session, value, expires_at)?;
				}
			},
			tg::value::Data::Object(object) => {
				let token = Self::create_token(
					session,
					object.node.clone().into(),
					vec![tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					)],
					expires_at,
				)?;
				if let Some(token) = token {
					object.options.tokens.insert_local_authorization(token);
				}
			},
			tg::value::Data::Mutation(mutation) => {
				Self::add_tokens_to_mutation_data(session, mutation, expires_at)?;
			},
			tg::value::Data::Module(module) => {
				Self::add_tokens_to_module_data_with_expires_at(session, module, expires_at)?;
			},
			tg::value::Data::Template(template) => {
				Self::add_tokens_to_template_data(session, template, expires_at)?;
			},
			tg::value::Data::Bool(_)
			| tg::value::Data::Bytes(_)
			| tg::value::Data::Null
			| tg::value::Data::Number(_)
			| tg::value::Data::Placeholder(_)
			| tg::value::Data::String(_) => {},
		}
		Ok(())
	}

	pub(crate) fn add_tokens_to_module_data_with_expires_at(
		session: &Session,
		module: &mut tg::module::Data,
		expires_at: i64,
	) -> tg::Result<()> {
		let mut children = std::collections::BTreeSet::new();
		module.children(&mut children);
		if let Some(id) = children.into_iter().next() {
			let token = Self::create_token(
				session,
				id.into(),
				vec![tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				)],
				expires_at,
			)?;
			if let Some(token) = token {
				module
					.referent
					.options
					.tokens
					.insert_local_authorization(token);
			}
		}
		Ok(())
	}

	pub(crate) fn add_tokens_to_mutation_data(
		session: &Session,
		data: &mut tg::mutation::Data,
		expires_at: i64,
	) -> tg::Result<()> {
		match data {
			tg::mutation::Data::Unset => {},
			tg::mutation::Data::Set { value } | tg::mutation::Data::SetIfUnset { value } => {
				Self::add_tokens_to_value_data_with_expires_at(session, value, expires_at)?;
			},
			tg::mutation::Data::Prepend { values } | tg::mutation::Data::Append { values } => {
				for value in values {
					Self::add_tokens_to_value_data_with_expires_at(session, value, expires_at)?;
				}
			},
			tg::mutation::Data::Prefix { template, .. }
			| tg::mutation::Data::Suffix { template, .. } => {
				Self::add_tokens_to_template_data(session, template, expires_at)?;
			},
			tg::mutation::Data::Merge { value } => {
				for value in value.values_mut() {
					Self::add_tokens_to_value_data_with_expires_at(session, value, expires_at)?;
				}
			},
		}
		Ok(())
	}

	pub(crate) fn update_mutation_data_referents_for_location(
		session: &Session,
		data: &mut tg::mutation::Data,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		match data {
			tg::mutation::Data::Unset => {},
			tg::mutation::Data::Set { value } | tg::mutation::Data::SetIfUnset { value } => {
				Self::update_value_data_referents_for_location(session, value, location, trusted)?;
			},
			tg::mutation::Data::Prepend { values } | tg::mutation::Data::Append { values } => {
				for value in values {
					Self::update_value_data_referents_for_location(
						session, value, location, trusted,
					)?;
				}
			},
			tg::mutation::Data::Prefix { template, .. }
			| tg::mutation::Data::Suffix { template, .. } => {
				Self::update_template_data_referents_for_location(
					session, template, location, trusted,
				)?;
			},
			tg::mutation::Data::Merge { value } => {
				for value in value.values_mut() {
					Self::update_value_data_referents_for_location(
						session, value, location, trusted,
					)?;
				}
			},
		}
		Ok(())
	}

	pub(crate) fn add_tokens_to_template_data(
		session: &Session,
		data: &mut tg::template::Data,
		expires_at: i64,
	) -> tg::Result<()> {
		for component in &mut data.components {
			if let tg::template::data::Component::Artifact(artifact) = component {
				let token = Self::create_token(
					session,
					tg::object::Id::from(artifact.node.clone()).into(),
					vec![tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					)],
					expires_at,
				)?;
				if let Some(token) = token {
					artifact.options.tokens.insert_local_authorization(token);
				}
			}
		}
		Ok(())
	}

	pub(crate) fn update_template_data_referents_for_location(
		session: &Session,
		data: &mut tg::template::Data,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		for component in &mut data.components {
			if let tg::template::data::Component::Artifact(artifact) = component {
				Self::update_tokens_and_location(
					session,
					&mut artifact.options.tokens,
					Some(&mut artifact.options.location),
					location,
					trusted,
				)?;
			}
		}
		Ok(())
	}

	pub(crate) fn create_capture_permissions_batch_items(
		session: &Session,
		resource: tg::Id,
		version: Option<String>,
		roots: impl IntoIterator<Item = tg::Referent<tg::Id>>,
		principal: tg::Principal,
		created_at: i64,
	) -> tg::Result<Vec<tangram_index::batch::Item>> {
		let roots = roots
			.into_iter()
			.map(|resource| {
				let permissions =
					tangram_server_authorization_common::permission_capture_permissions(
						&resource.node,
					)?
					.empty_like();
				Ok((resource, permissions))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Self::create_capture_permissions_batch_items_with_permissions(
			session, resource, version, roots, principal, created_at,
		)
	}

	pub(crate) fn create_capture_permissions_batch_items_with_permissions(
		session: &Session,
		resource: tg::Id,
		version: Option<String>,
		roots: impl IntoIterator<Item = (tg::Referent<tg::Id>, tg::authorization::permission::Set)>,
		principal: tg::Principal,
		created_at: i64,
	) -> tg::Result<Vec<tangram_index::batch::Item>> {
		let (creator, subject) = match resource.kind() {
			tg::id::Kind::Process if version.is_none() => {
				let process = tg::process::Id::try_from(resource.clone())?;
				(
					Some(tg::Principal::Process(process.clone())),
					tg::authorization::Subject::Process(process),
				)
			},
			tg::id::Kind::Tag if version.is_some() => {
				let tag = tg::tag::Id::try_from(resource.clone())?;
				(None, tg::authorization::Subject::Tag(tag))
			},
			_ => return Err(tg::error!("invalid permission capture resource or version")),
		};
		let mut items = Vec::new();
		let mut pending = Vec::new();
		for (mut resource, mut proven) in roots {
			let requested = tangram_server_authorization_common::permission_capture_permissions(
				&resource.node,
			)?;
			let accepted = proven;
			for permission in requested.iter() {
				if accepted.iter().any(|proof| proof.implies(permission)) {
					proven.insert(permission.into());
				}
			}
			let mut tokens = Vec::new();
			if principal.is_root() {
				proven = requested;
			} else if !proven.contains(requested) {
				// Check exact proofs first so the fast path verifies only the tokens it needs.
				let local = resource.options.tokens.local_authorization();
				let exact = local
					.iter()
					.filter(|token| token.body.resource == resource.node);
				let other = local
					.iter()
					.filter(|token| token.body.resource != resource.node);
				for token in exact.chain(other) {
					if !Self::verify_token(session, token) {
						continue;
					}
					if token.body.resource == resource.node {
						for permission in requested.iter() {
							if token.body.authorizes(permission) {
								proven.insert(permission.into());
							}
						}
					}
					tokens.push(token.clone());
					if proven.contains(requested) {
						break;
					}
				}
			}
			if !proven.is_empty() {
				let arg = tangram_index::permission::put::Arg {
					created_at,
					creator: creator.clone(),
					permissions: proven,
					resource: resource.node.clone(),
					source: tangram_index::permission::Source::Direct { expires_at: None },
					subject: subject.clone(),
					time_to_touch: None,
					version: version.clone(),
				};
				items.push(tangram_index::batch::Item::PutPermission(arg));
			}
			if proven.contains(requested) {
				continue;
			}

			// Delegate only roots that still need capture, including authority held by pending syncs.
			let time_to_live = i64::try_from(
				session
					.server
					.config
					.indexer
					.permission_capture
					.delegation_time_to_live
					.as_secs(),
			)
			.map_err(|error| tg::error!(!error, "failed to convert the delegation time to live"))?;
			let expires_at = created_at
				.checked_add(time_to_live)
				.ok_or_else(|| tg::error!("the delegation expiration overflowed"))?;
			let mut sources = BTreeSet::new();
			let source = match &principal {
				tg::Principal::Anonymous => tg::authorization::Subject::Public,
				source => source.try_to_subject()?,
			};
			sources.insert(source);
			for token in &tokens {
				if let Some(sync) = Self::try_get_sync_id_from_token(session, token) {
					sources.insert(tg::authorization::Subject::Sync(sync));
				}
			}
			for source in sources {
				if source == subject {
					continue;
				}
				let arg = tangram_index::delegation::put::Arg {
					expires_at,
					resource: resource.node.clone(),
					source,
					subject: subject.clone(),
					version: version.clone(),
				};
				items.push(tangram_index::batch::Item::PutDelegation(arg));
			}
			resource.options.tokens = tg::authorization::Tokens::with_authorization(tokens);
			pending.push(resource);
		}
		if !pending.is_empty() && session.server.config.indexer.permission_capture.enabled {
			let arg = tangram_index::permission::capture::enqueue::Arg {
				id: uuid::Uuid::now_v7().as_bytes().to_vec(),
				principal,
				resource,
				roots: pending,
				version,
			};
			items.push(tangram_index::batch::Item::EnqueuePermissionCapture(arg));
		}

		Ok(items)
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::authorization::Component for Component {
	fn create_capture_permissions_batch_items(
		&self,
		session: &Session,
		resource: tg::Id,
		version: Option<String>,
		roots: Vec<tg::Referent<tg::Id>>,
		principal: tg::Principal,
		created_at: i64,
	) -> tg::Result<Vec<tangram_index::batch::Item>> {
		Self::create_capture_permissions_batch_items(
			session, resource, version, roots, principal, created_at,
		)
	}

	fn create_capture_permissions_batch_items_with_permissions(
		&self,
		session: &Session,
		resource: tg::Id,
		version: Option<String>,
		roots: Vec<(tg::Referent<tg::Id>, tg::authorization::permission::Set)>,
		principal: tg::Principal,
		created_at: i64,
	) -> tg::Result<Vec<tangram_index::batch::Item>> {
		Self::create_capture_permissions_batch_items_with_permissions(
			session, resource, version, roots, principal, created_at,
		)
	}

	fn add_permanent_token_to_object_reference<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		reference: &'a mut tg::Reference,
		resource: &'a tg::artifact::Id,
	) -> tg::Result<()> {
		Self::add_permanent_token_to_object_reference(session, reference, resource)
	}

	fn add_tokens_to_module_data_with_expires_at<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		module: &'a mut tg::module::Data,
		expires_at: i64,
	) -> tg::Result<()> {
		Self::add_tokens_to_module_data_with_expires_at(session, module, expires_at)
	}

	fn add_tokens_to_value_data<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		data: &'a mut tg::value::Data,
	) -> tg::Result<()> {
		Self::add_tokens_to_value_data(session, data)
	}

	fn add_tokens_to_value_data_with_expires_at<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		data: &'a mut tg::value::Data,
		expires_at: i64,
	) -> tg::Result<()> {
		Self::add_tokens_to_value_data_with_expires_at(session, data, expires_at)
	}

	fn authorize_owner<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		owner: Option<&'a tg::Principal>,
	) -> BoxFuture<'a, tg::Result<()>> {
		Self::authorize_owner(session, owner).boxed()
	}

	fn authorize_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		resource: &'a tg::Selector<tg::Id>,
		permissions: tg::authorization::permission::Set,
		token: &'a tg::authorization::Token,
	) -> bool {
		Self::authorize_token(session, resource, permissions, token)
	}

	fn create_permanent_object_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		resource: &'a tg::artifact::Id,
	) -> tg::Result<Option<tg::authorization::Token>> {
		Self::create_permanent_object_token(session, resource)
	}

	fn create_read_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::Id,
	) -> tg::Result<Option<tg::authorization::Token>> {
		Self::create_read_token(session, id)
	}

	fn create_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		resource: tg::Id,
		permissions: Vec<tg::authorization::Permission>,
		expires_at: i64,
	) -> tg::Result<Option<tg::authorization::Token>> {
		Self::create_token(session, resource, permissions, expires_at)
	}

	fn try_get_sync_id_from_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		token: &'a tg::authorization::Token,
	) -> Option<tg::sync::Id> {
		Self::try_get_sync_id_from_token(session, token)
	}

	fn update_error_data_referents_for_location<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		data: &'a mut tg::error::Data,
		location: &'a tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		Self::update_error_data_referents_for_location(session, data, location, trusted)
	}

	fn update_process_child_referent_for_location<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		child: &'a mut tg::process::data::Child,
		location: &'a tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		Self::update_process_child_referent_for_location(session, child, location, trusted)
	}

	fn update_process_data_referents_for_location<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		data: &'a mut tg::process::Data,
		location: &'a tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		Self::update_process_data_referents_for_location(session, data, location, trusted)
	}

	fn update_tokens_and_location<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		tokens: &'a mut tg::authorization::Tokens,
		output_location: Option<&'a mut Option<tg::Location>>,
		location: &'a tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		Self::update_tokens_and_location(session, tokens, output_location, location, trusted)
	}

	fn update_value_data_referents_for_location<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		data: &'a mut tg::value::Data,
		location: &'a tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		Self::update_value_data_referents_for_location(session, data, location, trusted)
	}

	fn verify_local_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		token: &'a tg::authorization::Token,
	) -> bool {
		Self::verify_local_token(session, token)
	}

	fn verify_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		token: &'a tg::authorization::Token,
	) -> bool {
		Self::verify_token(session, token)
	}
}
