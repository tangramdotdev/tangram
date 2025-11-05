use {crate::prelude::*, itertools::Itertools as _, std::collections::BTreeSet};

#[derive(
	Clone,
	Debug,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Data {
	#[tangram_serialize(id = 0)]
	Unset,

	#[tangram_serialize(id = 1)]
	Set { value: Box<tg::value::Data> },

	#[tangram_serialize(id = 2)]
	SetIfUnset { value: Box<tg::value::Data> },

	#[tangram_serialize(id = 3)]
	Prepend { values: Vec<tg::value::Data> },

	#[tangram_serialize(id = 4)]
	Append { values: Vec<tg::value::Data> },

	#[tangram_serialize(id = 5)]
	Prefix {
		#[serde(default, skip_serializing_if = "Option::is_none")]
		separator: Option<String>,
		template: tg::template::Data,
	},

	#[tangram_serialize(id = 6)]
	Suffix {
		#[serde(default, skip_serializing_if = "Option::is_none")]
		separator: Option<String>,
		template: tg::template::Data,
	},

	#[tangram_serialize(id = 7)]
	Merge { value: tg::value::data::Map },
}

impl Data {
	pub fn children(&self, children: &mut BTreeSet<tg::object::Id>) {
		match self {
			Self::Unset => (),
			Self::Set { value } | Self::SetIfUnset { value } => {
				value.children(children);
			},
			Self::Prepend { values } | Self::Append { values } => {
				for value in values {
					value.children(children);
				}
			},
			Self::Prefix { template, .. } | Self::Suffix { template, .. } => {
				template.children(children);
			},
			Self::Merge { value } => {
				for value in value.values() {
					value.children(children);
				}
			},
		}
	}

	pub fn apply(&self, map: &mut tg::value::data::Map, key: &str) -> tg::Result<()> {
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
			(Self::Prepend { values }, Some(tg::value::Data::Array(array))) => {
				array.splice(0..0, values.clone());
			},
			(Self::Append { values }, Some(tg::value::Data::Array(array))) => {
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
					tg::value::Data::Template(template) => template.clone(),
					tg::value::Data::Object(tg::object::Id::Directory(directory)) => {
						tg::template::Data::with_components([directory.clone().into()])
					},
					tg::value::Data::Object(tg::object::Id::File(file)) => {
						tg::template::Data::with_components([file.clone().into()])
					},
					tg::value::Data::Object(tg::object::Id::Symlink(symlink)) => {
						tg::template::Data::with_components([symlink.clone().into()])
					},
					tg::value::Data::String(string) => {
						tg::template::Data::with_components([string.clone().into()])
					},
					_ => {
						return Err(tg::error!("expected an artifact, string, or template"));
					},
				};

				let template = if let Some(separator) = separator {
					let separator = tg::template::data::Component::from(separator.clone());
					let components = first
						.components
						.clone()
						.into_iter()
						.chain(second.components.clone())
						.interleave_shortest(std::iter::from_fn(move || Some(separator.clone())));
					tg::template::Data::with_components(components)
				} else {
					let components = first
						.components
						.clone()
						.into_iter()
						.chain(second.components.clone());
					tg::template::Data::with_components(components)
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
					tg::value::Data::Template(template) => template.clone(),
					tg::value::Data::Object(tg::object::Id::Directory(directory)) => {
						tg::template::Data::with_components([directory.clone().into()])
					},
					tg::value::Data::Object(tg::object::Id::File(file)) => {
						tg::template::Data::with_components([file.clone().into()])
					},
					tg::value::Data::Object(tg::object::Id::Symlink(symlink)) => {
						tg::template::Data::with_components([symlink.clone().into()])
					},
					tg::value::Data::String(string) => {
						tg::template::Data::with_components([string.clone().into()])
					},
					_ => {
						return Err(tg::error!("expected an artifact, string, or template"));
					},
				};

				let template = if let Some(separator) = separator {
					let separator = tg::template::data::Component::from(separator.clone());
					let components = first
						.components
						.clone()
						.into_iter()
						.chain(second.components.clone())
						.interleave_shortest(std::iter::from_fn(move || Some(separator.clone())));
					tg::template::Data::with_components(components)
				} else {
					let components = first
						.components
						.clone()
						.into_iter()
						.chain(second.components.clone());
					tg::template::Data::with_components(components)
				};

				map.insert(key.to_owned(), template.into());
			},
			(Self::Merge { value }, None) => {
				map.insert(key.to_owned(), value.clone().into());
			},
			(Self::Merge { value }, Some(tg::value::Data::Map(existing))) => {
				for (k, v) in value {
					if let Ok(mutation) = v.try_unwrap_mutation_ref() {
						mutation.apply(existing, k)?;
					} else {
						existing.insert(k.into(), v.clone());
					}
				}
			},
			(Self::Merge { .. }, Some(_)) => {
				return Err(tg::error!(%key, "expected a map"));
			},
		}
		Ok(())
	}
}
