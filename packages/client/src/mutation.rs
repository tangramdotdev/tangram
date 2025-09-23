use {crate as tg, itertools::Itertools as _};

pub use self::data::Data;

pub mod data;

#[derive(Clone, Debug)]
pub enum Mutation {
	Unset,
	Set {
		value: Box<tg::Value>,
	},
	SetIfUnset {
		value: Box<tg::Value>,
	},
	Prepend {
		values: Vec<tg::Value>,
	},
	Append {
		values: Vec<tg::Value>,
	},
	Prefix {
		separator: Option<String>,
		template: tg::Template,
	},
	Suffix {
		separator: Option<String>,
		template: tg::Template,
	},
	Merge {
		value: tg::value::Map,
	},
}

impl Mutation {
	pub fn objects(&self) -> Vec<tg::Object> {
		match self {
			Self::Unset => vec![],
			Self::Set { value } | Self::SetIfUnset { value } => value.objects(),
			Self::Prepend { values } | Self::Append { values } => {
				values.iter().flat_map(tg::Value::objects).collect()
			},
			Self::Prefix { template, .. } | Self::Suffix { template, .. } => template.objects(),
			Self::Merge { value } => value.iter().flat_map(|(_key, val)| val.objects()).collect(),
		}
	}

	#[must_use]
	pub fn to_data(&self) -> Data {
		match self {
			Self::Unset => Data::Unset,
			Self::Set { value } => Data::Set {
				value: Box::new(value.to_data()),
			},
			Self::SetIfUnset { value } => Data::SetIfUnset {
				value: Box::new(value.to_data()),
			},
			Self::Prepend { values } => Data::Prepend {
				values: values.iter().map(tg::Value::to_data).collect(),
			},
			Self::Append { values } => Data::Append {
				values: values.iter().map(tg::Value::to_data).collect(),
			},
			Self::Prefix {
				template,
				separator,
			} => Data::Prefix {
				template: template.to_data(),
				separator: separator.clone(),
			},
			Self::Suffix {
				template,
				separator,
			} => Data::Suffix {
				template: template.to_data(),
				separator: separator.clone(),
			},
			Self::Merge { value } => Data::Merge {
				value: value
					.iter()
					.map(|(key, val)| (key.clone(), val.to_data()))
					.collect(),
			},
		}
	}

	pub fn apply(&self, map: &mut tg::value::Map, key: &str) -> tg::Result<()> {
		match (self, map.get_mut(key)) {
			(Self::Unset, _) => {
				map.remove(key);
			},
			(Self::Set { value }, _) | (Self::SetIfUnset { value }, None) => {
				map.insert(key.to_owned(), value.as_ref().clone());
			},
			(Self::SetIfUnset { .. }, _) => (),
			(Self::Prepend { values } | Self::Append { values }, None) => {
				map.insert(key.into(), values.clone().into());
			},
			(Self::Prepend { values }, Some(tg::Value::Array(array))) => {
				array.splice(0..0, values.clone());
			},
			(Self::Append { values }, Some(tg::Value::Array(array))) => {
				array.extend(values.clone());
			},
			(Self::Append { .. } | Self::Prepend { .. }, Some(_)) => {
				return Err(tg::error!(%key, "expected an array"));
			},
			(Self::Prefix { template, .. } | Self::Suffix { template, .. }, None) => {
				map.insert(key.to_owned(), template.clone().into());
			},
			(
				Self::Prefix {
					separator,
					template: first,
				},
				Some(value),
			) => {
				let second = match value {
					tg::Value::Template(template) => template.clone(),
					tg::Value::Object(tg::Object::Directory(directory)) => {
						tg::Template::with_components([directory.clone().into()])
					},
					tg::Value::Object(tg::Object::File(file)) => {
						tg::Template::with_components([file.clone().into()])
					},
					tg::Value::Object(tg::Object::Symlink(symlink)) => {
						tg::Template::with_components([symlink.clone().into()])
					},
					tg::Value::String(string) => {
						tg::Template::with_components([string.clone().into()])
					},
					_ => {
						return Err(tg::error!("expected an artifact, string, or template"));
					},
				};

				let template = if let Some(separator) = separator {
					let separator = tg::template::Component::from(separator.clone());
					let components = first
						.components
						.clone()
						.into_iter()
						.chain(second.components.clone())
						.interleave_shortest(std::iter::from_fn(move || Some(separator.clone())));
					tg::Template::with_components(components)
				} else {
					let components = first
						.components
						.clone()
						.into_iter()
						.chain(second.components.clone());
					tg::Template::with_components(components)
				};

				map.insert(key.to_owned(), template.into());
			},
			(
				Self::Suffix {
					separator,
					template: second,
				},
				Some(value),
			) => {
				let first = match value {
					tg::Value::Template(template) => template.clone(),
					tg::Value::Object(tg::Object::Directory(directory)) => {
						tg::Template::with_components([directory.clone().into()])
					},
					tg::Value::Object(tg::Object::File(file)) => {
						tg::Template::with_components([file.clone().into()])
					},
					tg::Value::Object(tg::Object::Symlink(symlink)) => {
						tg::Template::with_components([symlink.clone().into()])
					},
					tg::Value::String(string) => {
						tg::Template::with_components([string.clone().into()])
					},
					_ => {
						return Err(tg::error!("expected an artifact, string, or template"));
					},
				};

				let template = if let Some(separator) = separator {
					let separator = tg::template::Component::from(separator.clone());
					let components = first
						.components
						.clone()
						.into_iter()
						.chain(second.components.clone())
						.interleave_shortest(std::iter::from_fn(move || Some(separator.clone())));
					tg::Template::with_components(components)
				} else {
					let components = first
						.components
						.clone()
						.into_iter()
						.chain(second.components.clone());
					tg::Template::with_components(components)
				};

				map.insert(key.to_owned(), template.into());
			},
			(tg::Mutation::Merge { value }, None) => {
				map.insert(key.to_owned(), value.clone().into());
			},
			(tg::Mutation::Merge { value }, Some(tg::Value::Map(existing))) => {
				for (k, v) in value {
					if let Ok(mutation) = v.try_unwrap_mutation_ref() {
						mutation.apply(existing, k)?;
					} else {
						existing.insert(k.into(), v.clone());
					}
				}
			},
			(tg::Mutation::Merge { .. }, Some(_)) => {
				return Err(tg::error!(%key, "expected a map"));
			},
		}
		Ok(())
	}
}

impl TryFrom<Data> for Mutation {
	type Error = tg::Error;

	fn try_from(data: Data) -> Result<Self, Self::Error> {
		Ok(match data {
			Data::Unset => Self::Unset,
			Data::Set { value } => Self::Set {
				value: Box::new((*value).try_into()?),
			},
			Data::SetIfUnset { value } => Self::SetIfUnset {
				value: Box::new((*value).try_into()?),
			},
			Data::Prepend { values } => Self::Prepend {
				values: values
					.into_iter()
					.map(TryInto::try_into)
					.collect::<tg::Result<_>>()?,
			},
			Data::Append { values } => Self::Append {
				values: values
					.into_iter()
					.map(TryInto::try_into)
					.collect::<tg::Result<_>>()?,
			},
			Data::Prefix {
				template,
				separator,
			} => Self::Prefix {
				template: template.try_into()?,
				separator,
			},
			Data::Suffix {
				template,
				separator,
			} => Self::Suffix {
				template: template.try_into()?,
				separator,
			},
			Data::Merge { value } => Self::Merge {
				value: value
					.into_iter()
					.map(|(k, v)| Ok::<_, tg::Error>((k, v.try_into()?)))
					.collect::<tg::Result<_>>()?,
			},
		})
	}
}

impl std::fmt::Display for Mutation {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut printer = tg::value::print::Printer::new(f, tg::value::print::Options::default());
		printer.mutation(self)?;
		Ok(())
	}
}
