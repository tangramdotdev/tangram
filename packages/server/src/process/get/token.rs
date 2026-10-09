use {
	crate::Session,
	tangram_client::{authorization::permission::process::Permission, prelude::*},
};

impl Session {
	pub(super) fn add_tokens_to_process_get_output(
		&self,
		id: &tg::process::Id,
		authorization: crate::authorization::Output,
		output: &mut tg::process::get::Output,
	) -> tg::Result<()> {
		if self.server.authorization_tokens.private_key.is_none() {
			return Ok(());
		}

		// Bound newly created tokens by the lifetime of the accepted authorization.
		let expires_at = self.process_get_token_expires_at(authorization.expires_at)?;
		let permissions = authorization.permissions;
		if let Some(token) =
			self.create_token(id.clone().into(), permissions.iter().collect(), expires_at)?
		{
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
				if let Some(token) = self.create_token(
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
			self.server.config.object.permission_time_to_live.as_secs(),
		)
		.map_err(|error| {
			tg::error!(
				!error,
				"failed to convert the object permission time to live"
			)
		})?;
		let object_expires_at = self
			.server
			.clock
			.unix_timestamp()?
			.checked_add(object_time_to_live)
			.ok_or_else(|| tg::error!("the object permission expiration overflowed"))?
			.min(expires_at);
		let data = &mut output.data;
		let permits =
			|permission| permissions.contains(tg::authorization::Permission::Process(permission));

		if permits(Permission::NodeCommandObjects) {
			let mut options = tg::referent::Options::default();
			for mut object in data.command.objects() {
				self.add_token_to_object_referent_with_expires_at(&mut object, object_expires_at)?;
				options.tokens.inherit(&object.options.tokens);
			}
			data.command.options.tokens.inherit(&options.tokens);
			if let tg::Either::Left(command) = &mut data.command.node {
				command.inherit_location_and_tokens(&options);
			}
		}
		if permits(Permission::NodeErrorObjects)
			&& let Some(error) = &mut data.error
		{
			match error {
				tg::Either::Left(error) => {
					self.add_tokens_to_process_get_error(error, object_expires_at)?;
				},
				tg::Either::Right(error) => {
					self.add_token_to_object_referent_with_expires_at(error, object_expires_at)?;
				},
			}
		}
		if permits(Permission::NodeLogObjects)
			&& let Some(log) = &mut data.log
		{
			self.add_token_to_object_referent_with_expires_at(log, object_expires_at)?;
		}
		if permits(Permission::NodeOutputObjects)
			&& let Some(output) = &mut data.output
		{
			self.add_tokens_to_value_data_with_expires_at(output, object_expires_at)?;
		}

		Ok(())
	}

	fn add_tokens_to_process_get_error(
		&self,
		data: &mut tg::error::Data,
		expires_at: i64,
	) -> tg::Result<()> {
		for diagnostic in data.diagnostics.iter_mut().flatten() {
			if let Some(location) = &mut diagnostic.location {
				self.add_tokens_to_module_data_with_expires_at(&mut location.module, expires_at)?;
			}
		}
		for location in data
			.location
			.iter_mut()
			.chain(data.stack.iter_mut().flatten())
		{
			if let tg::error::data::File::Module(module) = &mut location.file {
				self.add_tokens_to_module_data_with_expires_at(module, expires_at)?;
			}
		}
		if let Some(source) = &mut data.source {
			match &mut source.node {
				tg::Either::Left(error) => {
					self.add_tokens_to_process_get_error(error, expires_at)?;
				},
				tg::Either::Right(id) => {
					let mut referent = tg::Referent::with_node(id.clone());
					self.add_token_to_object_referent_with_expires_at(&mut referent, expires_at)?;
					source.options.tokens.inherit(&referent.options.tokens);
				},
			}
		}
		Ok(())
	}
}
