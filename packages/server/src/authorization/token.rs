use {crate::Session, num::ToPrimitive as _, tangram_client::prelude::*};

impl Session {
	pub(crate) fn create_read_token(
		&self,
		id: &tg::Id,
	) -> tg::Result<Option<tg::authorization::Token>> {
		let permission = Self::read_permission_for_resource(id)?;
		let expires_at = self.server.clock.unix_timestamp()?
			+ self
				.server
				.config
				.sync
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		self.create_token(id.clone(), vec![permission], expires_at)
	}

	pub(crate) fn add_tokens_to_value_data(&self, data: &mut tg::value::Data) -> tg::Result<()> {
		let now = self.server.clock.unix_timestamp()?;
		let expires_at = now
			+ self
				.server
				.config
				.object
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		self.add_tokens_to_value_data_with_expires_at(data, expires_at)
	}

	pub(crate) fn add_token_to_object_referent<T>(
		&self,
		referent: &mut tg::Referent<T>,
	) -> tg::Result<()>
	where
		T: Clone + Into<tg::Id>,
	{
		let expires_at = self.server.clock.unix_timestamp()?
			+ self
				.server
				.config
				.object
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		let token = self.create_token(
			referent.node.clone().into(),
			vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)],
			expires_at,
		)?;
		if let Some(token) = token {
			referent.options.tokens.set_local(token);
		}
		Ok(())
	}

	pub(crate) fn update_tokens_and_location(
		&self,
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
		let Some(token) = tokens.remove_local() else {
			return Ok(());
		};
		let local_token = if trusted {
			// Trust the remote token by signing its exact permissions locally.
			let body = &token.body;
			self.create_token(
				body.resource.clone(),
				body.permissions.clone(),
				body.expires_at,
			)?
		} else {
			None
		};
		tokens.set(location.clone(), token);
		if let Some(token) = local_token {
			tokens.set_local(token);
		}
		Ok(())
	}

	pub(crate) fn update_value_data_referents_for_location(
		&self,
		data: &mut tg::value::Data,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		match data {
			tg::value::Data::Array(array) => {
				for value in array {
					self.update_value_data_referents_for_location(value, location, trusted)?;
				}
			},
			tg::value::Data::Map(map) => {
				for value in map.values_mut() {
					self.update_value_data_referents_for_location(value, location, trusted)?;
				}
			},
			tg::value::Data::Module(module) => {
				self.update_tokens_and_location(
					&mut module.referent.options.tokens,
					Some(&mut module.referent.options.location),
					location,
					trusted,
				)?;
			},
			tg::value::Data::Mutation(mutation) => {
				self.update_mutation_data_referents_for_location(mutation, location, trusted)?;
			},
			tg::value::Data::Object(object) => {
				self.update_tokens_and_location(
					&mut object.options.tokens,
					Some(&mut object.options.location),
					location,
					trusted,
				)?;
			},
			tg::value::Data::Template(template) => {
				self.update_template_data_referents_for_location(template, location, trusted)?;
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
		&self,
		data: &mut tg::process::Data,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		self.update_tokens_and_location(
			&mut data.command.options.tokens,
			Some(&mut data.command.options.location),
			location,
			trusted,
		)?;
		if let Some(children) = &mut data.children {
			for child in children {
				self.update_tokens_and_location(
					&mut child.process.options.tokens,
					Some(&mut child.process.options.location),
					location,
					trusted,
				)?;
			}
		}
		if let Some(error) = &mut data.error {
			match error {
				tg::Either::Left(error) => {
					self.update_error_data_referents_for_location(error, location, trusted)?;
				},
				tg::Either::Right(error) => {
					self.update_tokens_and_location(
						&mut error.options.tokens,
						Some(&mut error.options.location),
						location,
						trusted,
					)?;
				},
			}
		}
		if let Some(log) = &mut data.log {
			self.update_tokens_and_location(
				&mut log.options.tokens,
				Some(&mut log.options.location),
				location,
				trusted,
			)?;
		}
		if let Some(output) = &mut data.output {
			self.update_value_data_referents_for_location(output, location, trusted)?;
		}
		Ok(())
	}

	pub(crate) fn update_error_data_referents_for_location(
		&self,
		data: &mut tg::error::Data,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		if let Some(diagnostics) = &mut data.diagnostics {
			for diagnostic in diagnostics {
				if let Some(location_data) = &mut diagnostic.location {
					self.update_tokens_and_location(
						&mut location_data.module.referent.options.tokens,
						Some(&mut location_data.module.referent.options.location),
						location,
						trusted,
					)?;
				}
			}
		}
		if let Some(location_data) = &mut data.location {
			self.update_error_location_data_referents_for_location(
				location_data,
				location,
				trusted,
			)?;
		}
		if let Some(source) = &mut data.source {
			self.update_tokens_and_location(
				&mut source.options.tokens,
				Some(&mut source.options.location),
				location,
				trusted,
			)?;
			if let tg::Either::Left(error) = &mut source.node {
				self.update_error_data_referents_for_location(error, location, trusted)?;
			}
		}
		if let Some(stack) = &mut data.stack {
			for location_data in stack {
				self.update_error_location_data_referents_for_location(
					location_data,
					location,
					trusted,
				)?;
			}
		}
		Ok(())
	}

	fn update_error_location_data_referents_for_location(
		&self,
		data: &mut tg::error::data::Location,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		if let tg::error::data::File::Module(module) = &mut data.file {
			self.update_tokens_and_location(
				&mut module.referent.options.tokens,
				Some(&mut module.referent.options.location),
				location,
				trusted,
			)?;
		}
		Ok(())
	}

	fn add_tokens_to_value_data_with_expires_at(
		&self,
		data: &mut tg::value::Data,
		expires_at: i64,
	) -> tg::Result<()> {
		match data {
			tg::value::Data::Array(array) => {
				for value in array {
					self.add_tokens_to_value_data_with_expires_at(value, expires_at)?;
				}
			},
			tg::value::Data::Map(map) => {
				for value in map.values_mut() {
					self.add_tokens_to_value_data_with_expires_at(value, expires_at)?;
				}
			},
			tg::value::Data::Object(object) => {
				let token = self.create_token(
					object.node.clone().into(),
					vec![tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					)],
					expires_at,
				)?;
				if let Some(token) = token {
					object.options.tokens.set_local(token);
				}
			},
			tg::value::Data::Mutation(mutation) => {
				self.add_tokens_to_mutation_data(mutation, expires_at)?;
			},
			tg::value::Data::Module(module) => {
				let mut children = std::collections::BTreeSet::new();
				module.children(&mut children);
				if let Some(id) = children.into_iter().next() {
					let token = self.create_token(
						id.into(),
						vec![tg::authorization::Permission::Object(
							tg::authorization::permission::object::Permission::Subtree,
						)],
						expires_at,
					)?;
					if let Some(token) = token {
						module.referent.options.tokens.set_local(token);
					}
				}
			},
			tg::value::Data::Template(template) => {
				self.add_tokens_to_template_data(template, expires_at)?;
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

	fn add_tokens_to_mutation_data(
		&self,
		data: &mut tg::mutation::Data,
		expires_at: i64,
	) -> tg::Result<()> {
		match data {
			tg::mutation::Data::Unset => {},
			tg::mutation::Data::Set { value } | tg::mutation::Data::SetIfUnset { value } => {
				self.add_tokens_to_value_data_with_expires_at(value, expires_at)?;
			},
			tg::mutation::Data::Prepend { values } | tg::mutation::Data::Append { values } => {
				for value in values {
					self.add_tokens_to_value_data_with_expires_at(value, expires_at)?;
				}
			},
			tg::mutation::Data::Prefix { template, .. }
			| tg::mutation::Data::Suffix { template, .. } => {
				self.add_tokens_to_template_data(template, expires_at)?;
			},
			tg::mutation::Data::Merge { value } => {
				for value in value.values_mut() {
					self.add_tokens_to_value_data_with_expires_at(value, expires_at)?;
				}
			},
		}
		Ok(())
	}

	fn update_mutation_data_referents_for_location(
		&self,
		data: &mut tg::mutation::Data,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		match data {
			tg::mutation::Data::Unset => {},
			tg::mutation::Data::Set { value } | tg::mutation::Data::SetIfUnset { value } => {
				self.update_value_data_referents_for_location(value, location, trusted)?;
			},
			tg::mutation::Data::Prepend { values } | tg::mutation::Data::Append { values } => {
				for value in values {
					self.update_value_data_referents_for_location(value, location, trusted)?;
				}
			},
			tg::mutation::Data::Prefix { template, .. }
			| tg::mutation::Data::Suffix { template, .. } => {
				self.update_template_data_referents_for_location(template, location, trusted)?;
			},
			tg::mutation::Data::Merge { value } => {
				for value in value.values_mut() {
					self.update_value_data_referents_for_location(value, location, trusted)?;
				}
			},
		}
		Ok(())
	}

	fn add_tokens_to_template_data(
		&self,
		data: &mut tg::template::Data,
		expires_at: i64,
	) -> tg::Result<()> {
		for component in &mut data.components {
			if let tg::template::data::Component::Artifact(artifact) = component {
				let token = self.create_token(
					tg::object::Id::from(artifact.node.clone()).into(),
					vec![tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					)],
					expires_at,
				)?;
				if let Some(token) = token {
					artifact.options.tokens.set_local(token);
				}
			}
		}
		Ok(())
	}

	fn update_template_data_referents_for_location(
		&self,
		data: &mut tg::template::Data,
		location: &tg::Location,
		trusted: bool,
	) -> tg::Result<()> {
		for component in &mut data.components {
			if let tg::template::data::Component::Artifact(artifact) = component {
				self.update_tokens_and_location(
					&mut artifact.options.tokens,
					Some(&mut artifact.options.location),
					location,
					trusted,
				)?;
			}
		}
		Ok(())
	}
}
