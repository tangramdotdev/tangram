use {super::Data, crate::prelude::*, std::collections::BTreeMap, tangram_either::Either};

#[derive(Clone, Debug, Default)]
pub struct Error {
	pub code: Option<tg::error::Code>,
	pub diagnostics: Option<Vec<tg::Diagnostic>>,
	pub location: Option<tg::error::Location>,
	pub message: Option<String>,
	pub source: Option<tg::Referent<Either<Box<tg::error::Object>, Box<tg::Error>>>>,
	pub stack: Option<Vec<tg::error::Location>>,
	pub values: BTreeMap<String, String>,
}

impl Error {
	pub fn to_data(&self) -> Data {
		let code = self.code;
		let diagnostics = self
			.diagnostics
			.as_ref()
			.map(|data| data.iter().map(tg::Diagnostic::to_data).collect());
		let location = self.location.as_ref().map(tg::error::Location::to_data);
		let message = self.message.clone();
		let source = self.source.as_ref().map(|source| {
			source.clone().map(|item| match item {
				Either::Left(object) => {
					let data = object.to_data();
					Either::Left(Box::new(data))
				},
				Either::Right(handle) => {
					let id = handle.id();
					Either::Right(id)
				},
			})
		});
		let stack = self
			.stack
			.as_ref()
			.map(|stack| stack.iter().map(tg::error::Location::to_data).collect());
		let values = self.values.clone();
		Data {
			code,
			diagnostics,
			location,
			message,
			source,
			stack,
			values,
		}
	}

	pub fn try_from_data(data: Data) -> tg::Result<Self> {
		let code = data.code;
		let diagnostics = data
			.diagnostics
			.map(|data| {
				data.into_iter()
					.map(tg::Diagnostic::try_from_data)
					.collect()
			})
			.transpose()?;
		let location = data
			.location
			.map(tg::error::Location::try_from_data)
			.transpose()?;
		let message = data.message;
		let source = data
			.source
			.map(|source| {
				source.try_map(|item| {
					let error = match item {
						Either::Left(data) => {
							let object = Error::try_from_data(*data)?;
							Either::Left(Box::new(object))
						},
						Either::Right(id) => {
							let error = tg::Error::with_id(id);
							Either::Right(Box::new(error))
						},
					};
					Ok::<_, tg::Error>(error)
				})
			})
			.transpose()?;
		let stack = data
			.stack
			.map(|stack| {
				stack
					.into_iter()
					.map(tg::error::Location::try_from_data)
					.collect()
			})
			.transpose()?;
		let values = data.values;
		Ok(Self {
			code,
			diagnostics,
			location,
			message,
			source,
			stack,
			values,
		})
	}

	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		let mut children = Vec::new();
		if let Some(diagnostics) = &self.diagnostics {
			for diagnostic in diagnostics {
				children.extend(diagnostic.children());
			}
		}
		if let Some(location) = &self.location {
			children.extend(location.children());
		}
		if let Some(stack) = &self.stack {
			for location in stack {
				children.extend(location.children());
			}
		}
		if let Some(source) = &self.source {
			match &source.item {
				Either::Left(object) => {
					children.extend(object.children());
				},
				Either::Right(error) => {
					children.push((*error.clone()).into());
				},
			}
		}
		children
	}
}
