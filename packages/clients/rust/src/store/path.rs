use crate::prelude::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Component<'a> {
	Id {
		id: tg::artifact::Id,
		suffix: &'a str,
	},
	Tag {
		component: tg::specifier::Component,
		suffix: Option<&'a str>,
	},
}

impl Component<'_> {
	#[must_use]
	pub fn module_suffix(&self) -> Option<&str> {
		match self {
			Self::Id { suffix, .. } => (!suffix.is_empty()).then_some(*suffix),
			Self::Tag { suffix, .. } => *suffix,
		}
	}
}

pub fn parse_component(value: &str) -> tg::Result<Component<'_>> {
	if let Some((id, suffix)) = tg::Id::try_parse_prefix(value) {
		if !suffix.is_empty() && !suffix.starts_with('.') {
			return Err(tg::error!(%value, "invalid store path component"));
		}
		let id = id.try_into()?;
		return Ok(Component::Id { id, suffix });
	}

	if let Some((component, suffix)) = value.split_once("@module") {
		if suffix.is_empty() || !suffix.starts_with('.') {
			return Err(tg::error!(%value, "invalid store path component"));
		}
		let component = component.parse()?;
		return Ok(Component::Tag {
			component,
			suffix: Some(suffix),
		});
	}

	let component = value.parse()?;
	Ok(Component::Tag {
		component,
		suffix: None,
	})
}

#[must_use]
pub fn module_component(component: &str, suffix: &str) -> String {
	format!("{component}@module{suffix}")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_components() {
		let id = tg::artifact::Id::from(tg::directory::Id::new(b"test"));
		let value = id.to_string();
		assert!(matches!(
			parse_component(&value).unwrap(),
			Component::Id { suffix: "", .. }
		));
		assert!(matches!(
			parse_component(&format!("{id}.tg.ts")).unwrap(),
			Component::Id {
				suffix: ".tg.ts",
				..
			}
		));
		assert!(matches!(
			parse_component("foo.tg.ts").unwrap(),
			Component::Tag { suffix: None, .. }
		));
		assert!(matches!(
			parse_component("foo.tg.ts@module.tg.ts").unwrap(),
			Component::Tag {
				suffix: Some(".tg.ts"),
				..
			}
		));
	}
}
