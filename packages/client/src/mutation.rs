use crate as tg;
use futures::{stream::FuturesOrdered, TryStreamExt as _};
use itertools::Itertools as _;

#[derive(Debug, Clone)]
pub enum Mutation {
	Unset,
	Set {
		value: Box<tg::Value>,
	},
	SetIfUnset {
		value: Box<tg::Value>,
	},
	ArrayPrepend {
		values: Vec<tg::Value>,
	},
	ArrayAppend {
		values: Vec<tg::Value>,
	},
	TemplatePrepend {
		template: tg::Template,
		separator: Option<String>,
	},
	TemplateAppend {
		template: tg::Template,
		separator: Option<String>,
	},
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Data {
	Unset,
	Set {
		value: Box<tg::value::Data>,
	},
	SetIfUnset {
		value: Box<tg::value::Data>,
	},
	ArrayPrepend {
		values: Vec<tg::value::Data>,
	},
	ArrayAppend {
		values: Vec<tg::value::Data>,
	},
	TemplatePrepend {
		template: tg::template::Data,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		separator: Option<String>,
	},
	TemplateAppend {
		template: tg::template::Data,
		#[serde(default, skip_serializing_if = "Option::is_none")]
		separator: Option<String>,
	},
}

impl Mutation {
	pub async fn data<H>(
		&self,
		tg: &H,
		transaction: Option<&H::Transaction<'_>>,
	) -> tg::Result<Data>
	where
		H: tg::Handle,
	{
		Ok(match self {
			Self::Unset => Data::Unset,
			Self::Set { value } => Data::Set {
				value: Box::new(Box::pin(value.data(tg, transaction)).await?),
			},
			Self::SetIfUnset { value } => Data::SetIfUnset {
				value: Box::new(Box::pin(value.data(tg, transaction)).await?),
			},
			Self::ArrayPrepend { values } => Data::ArrayPrepend {
				values: values
					.iter()
					.map(|value| value.data(tg, transaction))
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await?,
			},
			Self::ArrayAppend { values } => Data::ArrayAppend {
				values: values
					.iter()
					.map(|value| value.data(tg, transaction))
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await?,
			},
			Self::TemplatePrepend {
				template,
				separator,
			} => Data::TemplatePrepend {
				template: template.data(tg, transaction).await?,
				separator: separator.clone(),
			},
			Self::TemplateAppend {
				template,
				separator,
			} => Data::TemplateAppend {
				template: template.data(tg, transaction).await?,
				separator: separator.clone(),
			},
		})
	}

	pub fn objects(&self) -> Vec<tg::object::Handle> {
		match self {
			Self::Unset => vec![],
			Self::Set { value } | Self::SetIfUnset { value } => value.objects(),
			Self::ArrayPrepend { values } | Self::ArrayAppend { values } => {
				values.iter().flat_map(tg::Value::objects).collect()
			},
			Self::TemplatePrepend { template, .. } | Self::TemplateAppend { template, .. } => {
				template.objects()
			},
		}
	}
}

impl Data {
	#[must_use]
	pub fn children(&self) -> Vec<tg::object::Id> {
		match self {
			Self::Unset => vec![],
			Self::Set { value } | Self::SetIfUnset { value } => value.children(),
			Self::ArrayPrepend { values } | Self::ArrayAppend { values } => {
				values.iter().flat_map(tg::value::Data::children).collect()
			},
			Self::TemplatePrepend { template, .. } | Self::TemplateAppend { template, .. } => {
				template.children()
			},
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
			Data::ArrayPrepend { values } => Self::ArrayPrepend {
				values: values.into_iter().map(TryInto::try_into).try_collect()?,
			},
			Data::ArrayAppend { values } => Self::ArrayAppend {
				values: values.into_iter().map(TryInto::try_into).try_collect()?,
			},
			Data::TemplatePrepend {
				template,
				separator,
			} => Self::TemplatePrepend {
				template: template.try_into()?,
				separator,
			},
			Data::TemplateAppend {
				template,
				separator,
			} => Self::TemplateAppend {
				template: template.try_into()?,
				separator,
			},
		})
	}
}

impl std::fmt::Display for Mutation {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "(mutation)")?;
		Ok(())
	}
}
