use crate as tg;
use futures::{stream::FuturesOrdered, TryStreamExt as _};
use itertools::Itertools as _;
use std::collections::BTreeSet;

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
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Data {
	Unset,
	Set {
		value: Box<tg::value::Data>,
	},
	SetIfUnset {
		value: Box<tg::value::Data>,
	},
	Prepend {
		values: Vec<tg::value::Data>,
	},
	Append {
		values: Vec<tg::value::Data>,
	},
	Prefix {
		#[serde(default, skip_serializing_if = "Option::is_none")]
		separator: Option<String>,
		template: tg::template::Data,
	},
	Suffix {
		#[serde(default, skip_serializing_if = "Option::is_none")]
		separator: Option<String>,
		template: tg::template::Data,
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
		}
	}

	pub async fn data<H>(&self, handle: &H) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		Ok(match self {
			Self::Unset => Data::Unset,
			Self::Set { value } => Data::Set {
				value: Box::new(Box::pin(value.data(handle)).await?),
			},
			Self::SetIfUnset { value } => Data::SetIfUnset {
				value: Box::new(Box::pin(value.data(handle)).await?),
			},
			Self::Prepend { values } => Data::Prepend {
				values: values
					.iter()
					.map(|value| value.data(handle))
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await?,
			},
			Self::Append { values } => Data::Append {
				values: values
					.iter()
					.map(|value| value.data(handle))
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await?,
			},
			Self::Prefix {
				template,
				separator,
			} => Data::Prefix {
				template: template.data(handle).await?,
				separator: separator.clone(),
			},
			Self::Suffix {
				template,
				separator,
			} => Data::Suffix {
				template: template.data(handle).await?,
				separator: separator.clone(),
			},
		})
	}
}

impl Data {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		match self {
			Self::Unset => [].into(),
			Self::Set { value } | Self::SetIfUnset { value } => value.children(),
			Self::Prepend { values } | Self::Append { values } => {
				values.iter().flat_map(tg::value::Data::children).collect()
			},
			Self::Prefix { template, .. } | Self::Suffix { template, .. } => template.children(),
		}
	}
}

impl TryFrom<Data> for Mutation {
	type Error = tg::Error;

	fn try_from(data: Data) -> std::result::Result<Self, Self::Error> {
		Ok(match data {
			Data::Unset => Self::Unset,
			Data::Set { value } => Self::Set {
				value: Box::new((*value).try_into()?),
			},
			Data::SetIfUnset { value } => Self::SetIfUnset {
				value: Box::new((*value).try_into()?),
			},
			Data::Prepend { values } => Self::Prepend {
				values: values.into_iter().map(TryInto::try_into).try_collect()?,
			},
			Data::Append { values } => Self::Append {
				values: values.into_iter().map(TryInto::try_into).try_collect()?,
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
