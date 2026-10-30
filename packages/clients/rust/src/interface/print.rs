use crate::prelude::*;

mod javascript;
mod python;

/// Print an interface as an interface file in a syntax, with the magic comment that names the syntax on its first line if the syntax is not JSON.
pub fn print(interface: &tg::Interface, syntax: tg::interface::Syntax) -> tg::Result<String> {
	match syntax {
		tg::interface::Syntax::JavaScript => javascript::print(interface),
		tg::interface::Syntax::Json => Ok(interface.serialize_json()? + "\n"),
		tg::interface::Syntax::Python => python::print(interface),
	}
}

/// Print the specifier of an import, which is its path or its object ID, and its kind if the specifier does not imply it.
fn import(module: &tg::module::Data) -> tg::Result<(String, Option<String>)> {
	let (specifier, implied) = match &module.referent.node {
		tg::module::data::Source::Path(path) => {
			let specifier = path
				.to_str()
				.ok_or_else(|| tg::error!("the path of the import is not valid UTF-8"))?
				.to_owned();
			let implied = tg::module::module_kind_for_path(path).ok();
			(specifier, implied)
		},
		tg::module::data::Source::Edge(tg::graph::data::Edge::Object(id)) => (id.to_string(), None),
		tg::module::data::Source::Edge(_) => {
			return Err(tg::error!("expected the import to be a path or an object"));
		},
	};
	let kind = (implied != Some(module.kind)).then(|| module.kind.to_string());
	Ok((specifier, kind))
}

/// Print a number literal, which must be finite.
fn number(value: f64) -> tg::Result<String> {
	if !value.is_finite() {
		return Err(tg::error!("the number literal is not finite"));
	}
	Ok(value.to_string())
}

/// Print a string as a quoted string with escapes, which is valid in JavaScript and Python.
fn string(value: &str) -> String {
	serde_json::to_string(value).unwrap()
}

/// Determine whether a name is an identifier that is not one of the reserved words.
fn is_identifier(name: &str, reserved: &[&str]) -> bool {
	let mut chars = name.chars();
	chars
		.next()
		.is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
		&& chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
		&& !reserved.contains(&name)
}
