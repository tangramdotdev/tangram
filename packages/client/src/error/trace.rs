use super::Error;

pub struct Trace<'a>(pub(super) &'a Error);

impl std::fmt::Display for Trace<'_> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut current = Some(self.0);
		while let Some(error) = current {
			if let Some(object) = error.state().object() {
				let object = object.unwrap_error_ref();
				let message = object.message.as_deref().unwrap_or("an error occurred");
				write!(f, "-> {message}")?;
				for (key, value) in &object.values {
					write!(f, "\n   {key} = {value}")?;
				}
				if let Some(location) = &object.location
					&& location.file.is_internal()
				{
					write!(f, "\n   {location}")?;
				}
			} else {
				write!(f, "-> {}", error.id())?;
			}
			current =
				std::error::Error::source(error).and_then(|source| source.downcast_ref::<Error>());
			if current.is_some() {
				writeln!(f)?;
			}
		}
		Ok(())
	}
}
