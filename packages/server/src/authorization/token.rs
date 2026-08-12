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
			referent.options.tokens.insert_local(token);
		}
		Ok(())
	}

	pub(crate) fn update_tokens_for_location(
		&self,
		tokens: &mut tg::authorization::Tokens,
		location: &tg::Location,
	) -> tg::Result<()> {
		if location == &tg::Location::Local(tg::location::Local::default()) {
			return Ok(());
		}
		let Some(token) = tokens.remove_local() else {
			return Ok(());
		};
		let body = token.body.clone();
		tokens.insert(location.clone(), token);
		let token = self.create_token(body.resource, body.permissions, body.expires_at)?;
		if let Some(token) = token {
			tokens.insert_local(token);
		}
		Ok(())
	}

	pub(crate) fn update_value_data_tokens_for_location(
		&self,
		data: &mut tg::value::Data,
		location: &tg::Location,
	) -> tg::Result<()> {
		match data {
			tg::value::Data::Array(array) => {
				for value in array {
					self.update_value_data_tokens_for_location(value, location)?;
				}
			},
			tg::value::Data::Map(map) => {
				for value in map.values_mut() {
					self.update_value_data_tokens_for_location(value, location)?;
				}
			},
			tg::value::Data::Module(module) => {
				self.update_tokens_for_location(&mut module.referent.options.tokens, location)?;
			},
			tg::value::Data::Mutation(mutation) => {
				self.update_mutation_data_tokens_for_location(mutation, location)?;
			},
			tg::value::Data::Object(object) => {
				self.update_tokens_for_location(&mut object.options.tokens, location)?;
			},
			tg::value::Data::Template(template) => {
				self.update_template_data_tokens_for_location(template, location)?;
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
					object.options.tokens.insert_local(token);
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
						module.referent.options.tokens.insert_local(token);
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

	fn update_mutation_data_tokens_for_location(
		&self,
		data: &mut tg::mutation::Data,
		location: &tg::Location,
	) -> tg::Result<()> {
		match data {
			tg::mutation::Data::Unset => {},
			tg::mutation::Data::Set { value } | tg::mutation::Data::SetIfUnset { value } => {
				self.update_value_data_tokens_for_location(value, location)?;
			},
			tg::mutation::Data::Prepend { values } | tg::mutation::Data::Append { values } => {
				for value in values {
					self.update_value_data_tokens_for_location(value, location)?;
				}
			},
			tg::mutation::Data::Prefix { template, .. }
			| tg::mutation::Data::Suffix { template, .. } => {
				self.update_template_data_tokens_for_location(template, location)?;
			},
			tg::mutation::Data::Merge { value } => {
				for value in value.values_mut() {
					self.update_value_data_tokens_for_location(value, location)?;
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
					artifact.options.tokens.insert_local(token);
				}
			}
		}
		Ok(())
	}

	fn update_template_data_tokens_for_location(
		&self,
		data: &mut tg::template::Data,
		location: &tg::Location,
	) -> tg::Result<()> {
		for component in &mut data.components {
			if let tg::template::data::Component::Artifact(artifact) = component {
				self.update_tokens_for_location(&mut artifact.options.tokens, location)?;
			}
		}
		Ok(())
	}
}
