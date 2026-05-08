use crate::prelude::*;

#[derive(Clone, Debug)]
enum Kind {
	Unset,
	Set {
		value: tg::Value,
	},
	SetIfUnset {
		value: tg::Value,
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

#[derive(Clone, Debug, Default)]
pub struct Builder {
	kind: Option<Kind>,
}

impl Builder {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	#[must_use]
	pub fn unset(mut self) -> Self {
		self.kind = Some(Kind::Unset);
		self
	}

	#[must_use]
	pub fn set(mut self, value: impl Into<tg::Value>) -> Self {
		self.kind = Some(Kind::Set {
			value: value.into(),
		});
		self
	}

	#[must_use]
	pub fn set_if_unset(mut self, value: impl Into<tg::Value>) -> Self {
		self.kind = Some(Kind::SetIfUnset {
			value: value.into(),
		});
		self
	}

	#[must_use]
	pub fn prepend(mut self, values: impl IntoIterator<Item = tg::Value>) -> Self {
		self.kind = Some(Kind::Prepend {
			values: values.into_iter().collect(),
		});
		self
	}

	#[must_use]
	pub fn append(mut self, values: impl IntoIterator<Item = tg::Value>) -> Self {
		self.kind = Some(Kind::Append {
			values: values.into_iter().collect(),
		});
		self
	}

	#[must_use]
	pub fn prefix(mut self, template: impl Into<tg::Template>) -> Self {
		self.kind = Some(Kind::Prefix {
			separator: None,
			template: template.into(),
		});
		self
	}

	#[must_use]
	pub fn prefix_with_separator(
		mut self,
		separator: impl Into<String>,
		template: impl Into<tg::Template>,
	) -> Self {
		self.kind = Some(Kind::Prefix {
			separator: Some(separator.into()),
			template: template.into(),
		});
		self
	}

	#[must_use]
	pub fn suffix(mut self, template: impl Into<tg::Template>) -> Self {
		self.kind = Some(Kind::Suffix {
			separator: None,
			template: template.into(),
		});
		self
	}

	#[must_use]
	pub fn suffix_with_separator(
		mut self,
		separator: impl Into<String>,
		template: impl Into<tg::Template>,
	) -> Self {
		self.kind = Some(Kind::Suffix {
			separator: Some(separator.into()),
			template: template.into(),
		});
		self
	}

	#[must_use]
	pub fn merge(mut self, value: impl IntoIterator<Item = (String, tg::Value)>) -> Self {
		self.kind = Some(Kind::Merge {
			value: value.into_iter().collect(),
		});
		self
	}

	pub fn build(self) -> tg::Result<tg::Mutation> {
		let kind = self
			.kind
			.ok_or_else(|| tg::error!("cannot create a mutation without a kind"))?;
		Ok(match kind {
			Kind::Unset => tg::Mutation::Unset,
			Kind::Set { value } => tg::Mutation::Set {
				value: Box::new(value),
			},
			Kind::SetIfUnset { value } => tg::Mutation::SetIfUnset {
				value: Box::new(value),
			},
			Kind::Prepend { values } => tg::Mutation::Prepend { values },
			Kind::Append { values } => tg::Mutation::Append { values },
			Kind::Prefix {
				separator,
				template,
			} => tg::Mutation::Prefix {
				separator,
				template,
			},
			Kind::Suffix {
				separator,
				template,
			} => tg::Mutation::Suffix {
				separator,
				template,
			},
			Kind::Merge { value } => tg::Mutation::Merge { value },
		})
	}
}
