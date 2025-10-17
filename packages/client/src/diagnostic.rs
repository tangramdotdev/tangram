use {crate as tg, lsp_types as lsp};

pub use self::{data::Diagnostic as Data, data::Severity};

pub mod data;

#[derive(Clone, Debug)]
pub struct Diagnostic {
	pub location: Option<tg::Location>,
	pub severity: Severity,
	pub message: String,
}

impl Diagnostic {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		self.location
			.as_ref()
			.map(tg::Location::children)
			.into_iter()
			.flatten()
			.collect()
	}

	#[must_use]
	pub fn to_data(&self) -> data::Diagnostic {
		let location = self.location.as_ref().map(tg::Location::to_data);
		let severity = self.severity;
		let message = self.message.clone();
		data::Diagnostic {
			location,
			message,
			severity,
		}
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let location = data.location.map(TryInto::try_into).transpose()?;
		let severity = data.severity;
		let message = data.message;
		Ok(Self {
			location,
			severity,
			message,
		})
	}
}

impl TryFrom<data::Diagnostic> for Diagnostic {
	type Error = tg::Error;

	fn try_from(value: data::Diagnostic) -> Result<Self, Self::Error> {
		Self::try_from_data(value)
	}
}

impl std::fmt::Display for Diagnostic {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let Self {
			severity,
			message,
			location,
		} = self;
		write!(f, "{severity} {message}")?;
		if let Some(location) = location {
			write!(f, " {location}")?;
		}
		Ok(())
	}
}

impl From<Diagnostic> for lsp::Diagnostic {
	fn from(value: Diagnostic) -> Self {
		let range = value
			.location
			.map(|location| location.range.into())
			.unwrap_or_default();
		let severity = Some(value.severity.into());
		let source = Some("tangram".to_owned());
		let message = value.message;
		Self {
			range,
			severity,
			source,
			message,
			..Default::default()
		}
	}
}
