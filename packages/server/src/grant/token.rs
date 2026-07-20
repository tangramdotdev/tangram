use {crate::Session, num::ToPrimitive as _, tangram_client::prelude::*};

impl Session {
	pub(crate) fn add_tokens_to_value_data(&self, data: &mut tg::value::Data) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
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
					tg::grant::Resource::Id(object.item.clone().into()),
					vec![tg::grant::Permission::Object(
						tg::grant::permission::object::Permission::Subtree,
					)],
					expires_at,
				)?;
				object.options.token = token;
			},
			tg::value::Data::Mutation(mutation) => {
				self.add_tokens_to_mutation_data(mutation, expires_at)?;
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

	fn add_tokens_to_template_data(
		&self,
		data: &mut tg::template::Data,
		expires_at: i64,
	) -> tg::Result<()> {
		for component in &mut data.components {
			if let tg::template::data::Component::Artifact(artifact) = component {
				let token = self.create_token(
					tg::grant::Resource::Id(tg::object::Id::from(artifact.item.clone()).into()),
					vec![tg::grant::Permission::Object(
						tg::grant::permission::object::Permission::Subtree,
					)],
					expires_at,
				)?;
				artifact.options.token = token;
			}
		}
		Ok(())
	}
}
