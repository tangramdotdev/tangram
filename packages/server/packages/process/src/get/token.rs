use {
	crate::Component,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::{authorization::permission::process::Permission, prelude::*},
	tangram_server_core::{Session, authorization::CreateObjectToken},
};

impl Component {
	pub(super) async fn add_tokens_to_process_get_output(
		session: &Session,
		id: &tg::process::Id,
		authorization: tangram_server_core::authorization::Output,
		output: &mut tg::process::get::Output,
	) -> tg::Result<()> {
		if session.server.authorization_tokens.secret.is_none() {
			return Ok(());
		}

		// Bound newly created tokens by the lifetime of the accepted authorization.
		let expires_at = Self::process_get_token_expires_at(session, authorization.expires_at)?;
		let permissions = authorization.permissions;
		if let Some(token) = session.server.components.authorization.create_token(
			session,
			id.clone().into(),
			permissions.iter().collect(),
			expires_at,
		)? {
			output.tokens.insert_local_authorization(token);
		}

		// Derive child tokens from the subtree permissions on this process.
		let child_permissions = permissions
			.iter()
			.filter(|permission| {
				matches!(permission, tg::authorization::Permission::Process(permission)
				if *permission != Permission::Parent && *permission == permission.to_subtree())
			})
			.collect::<Vec<_>>();
		if !child_permissions.is_empty() {
			for child in output.data.children.iter_mut().flatten() {
				if let Some(token) = session.server.components.authorization.create_token(
					session,
					child.process.node.clone().into(),
					child_permissions.clone(),
					expires_at,
				)? {
					child
						.process
						.options
						.tokens
						.insert_local_authorization(token);
				}
			}
		}

		// Derive object subtree tokens from the process field permissions.
		let object_time_to_live = i64::try_from(
			session
				.server
				.config
				.object
				.permission_time_to_live
				.as_secs(),
		)
		.map_err(|error| tg::error!(!error, "failed to convert the object permission ttl"))?;
		let object_expires_at = session
			.server
			.clock
			.unix_timestamp()
			.checked_add(object_time_to_live)
			.ok_or_else(|| tg::error!("the object permission expiration overflowed"))?
			.min(expires_at);
		let data = &mut output.data;
		let permits =
			|permission| permissions.contains(tg::authorization::Permission::Process(permission));
		let command = permits(Permission::NodeCommandObjects);
		let error = permits(Permission::NodeErrorObjects);
		let log = permits(Permission::NodeLogObjects);
		let output = permits(Permission::NodeOutputObjects);

		// Collect the objects of the permitted fields.
		let mut objects = BTreeSet::new();
		if command {
			objects.extend(data.command.objects().into_iter().map(|object| object.node));
		}
		if error && let Some(error) = &data.error {
			match &error.node {
				tg::Either::Left(data) => data.children(&mut objects),
				tg::Either::Right(id) => {
					objects.insert(id.clone().into());
				},
			}
		}
		if log && let Some(log) = &data.log {
			objects.insert(log.node.clone().into());
		}
		if output && let Some(output) = &data.output {
			output.node.children(&mut objects);
		}

		// Verify the objects as the process, so that a token authorizes only what the process has proven.
		let proofs = Self::verify_process_objects(session, id, objects).await?;
		let mut create_token = |object: &tg::object::Id| {
			Self::create_process_object_token(session, &proofs, object, object_expires_at)
		};

		if command {
			let mut options = tg::referent::Options::default();
			for mut object in data.command.objects() {
				if let Some(token) = create_token(&object.node)? {
					object.options.tokens.insert_local_authorization(token);
				}
				options.tokens.inherit(&object.options.tokens);
			}
			data.command.options.tokens.inherit(&options.tokens);
			if let tg::Either::Left(command) = &mut data.command.node {
				command.inherit_location_and_tokens(&options);
			}
		}
		if error && let Some(error) = &mut data.error {
			match &mut error.node {
				tg::Either::Left(data) => {
					Self::add_tokens_to_process_get_error(session, data, &mut create_token)?;
				},
				tg::Either::Right(id) => {
					if let Some(token) = create_token(&id.clone().into())? {
						error.options.tokens.insert_local_authorization(token);
					}
				},
			}
		}
		if log
			&& let Some(log) = &mut data.log
			&& let Some(token) = create_token(&log.node.clone().into())?
		{
			log.options.tokens.insert_local_authorization(token);
		}
		if output && let Some(output) = &mut data.output {
			session
				.server
				.components
				.authorization
				.add_tokens_to_value_data(&mut output.node, &mut create_token)?;
		}

		Ok(())
	}

	fn add_tokens_to_process_get_error(
		session: &Session,
		data: &mut tg::error::Data,
		create_token: &mut CreateObjectToken<'_>,
	) -> tg::Result<()> {
		for diagnostic in data.diagnostics.iter_mut().flatten() {
			if let Some(location) = &mut diagnostic.location {
				session
					.server
					.components
					.authorization
					.add_tokens_to_module_data(&mut location.module, create_token)?;
			}
		}
		for location in data
			.location
			.iter_mut()
			.chain(data.stack.iter_mut().flatten())
		{
			if let tg::error::data::File::Module(module) = &mut location.file {
				session
					.server
					.components
					.authorization
					.add_tokens_to_module_data(module, create_token)?;
			}
		}
		if let Some(source) = &mut data.source {
			match &mut source.node {
				tg::Either::Left(error) => {
					Self::add_tokens_to_process_get_error(session, error, create_token)?;
				},
				tg::Either::Right(id) => {
					if let Some(token) = create_token(&id.clone().into())? {
						source.options.tokens.insert_local_authorization(token);
					}
				},
			}
		}
		Ok(())
	}

	/// Verify the objects as the process with the permissions that the process has directly, without a search, and return the permissions that the process has proven on each.
	pub(crate) async fn verify_process_objects(
		session: &Session,
		id: &tg::process::Id,
		objects: BTreeSet<tg::object::Id>,
	) -> tg::Result<BTreeMap<tg::object::Id, tangram_server_core::verify::Output>> {
		if objects.is_empty() {
			return Ok(BTreeMap::new());
		}
		let mut permissions = tg::authorization::permission::object::Set::NODE;
		permissions.insert(tg::authorization::permission::object::Set::SUBTREE);
		let permissions = tg::authorization::permission::Set::Object(permissions);
		let storage = tangram_server_core::verify::empty_storage(permissions);
		let objects = objects.into_iter().collect::<Vec<_>>();
		let args = objects
			.iter()
			.map(|object| (object.clone(), permissions, storage))
			.collect::<Vec<_>>();
		let subject = tg::authorization::Subject::Process(id.clone());
		let outputs = session
			.verify_batch_with_subject_without_search(args, permissions.empty_like(), subject)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to verify the process objects"))?;
		let proofs = std::iter::zip(objects, outputs)
			.filter(|(_, output)| !output.permissions.is_empty())
			.collect();

		Ok(proofs)
	}

	/// Create a token for an object with the permissions that the process has proven on it.
	pub(crate) fn create_process_object_token(
		session: &Session,
		proofs: &BTreeMap<tg::object::Id, tangram_server_core::verify::Output>,
		object: &tg::object::Id,
		expires_at: i64,
	) -> tg::Result<Option<tg::authorization::Token>> {
		let Some(proof) = proofs.get(object) else {
			return Ok(None);
		};
		let expires_at = proof.expires_at.map_or(expires_at, |proof_expires_at| {
			proof_expires_at.min(expires_at)
		});
		let permissions = proof.permissions.iter().collect();
		let token = session.server.components.authorization.create_token(
			session,
			object.clone().into(),
			permissions,
			expires_at,
		)?;

		Ok(token)
	}
}
