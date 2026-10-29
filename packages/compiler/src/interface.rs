use tangram_client::prelude::*;

#[cfg(feature = "javascript")]
mod javascript;
#[cfg(feature = "python")]
mod python;

/// An export that a module's source declares, before it is given a type.
#[cfg(any(feature = "javascript", feature = "python"))]
struct Export {
	/// The text of the export's documentation comment.
	documentation: Option<String>,

	/// The byte range of the export's name in the module's source.
	range: Option<std::ops::Range<usize>>,
}

/// Get the interface of a module from its source without types, giving every export the type of a command whose args and output are any values.
pub fn module(module: &tg::module::Data, text: &str) -> tg::Result<tg::Interface> {
	#[cfg(not(any(feature = "javascript", feature = "python")))]
	let _ = text;

	// Discover the exports.
	#[cfg_attr(
		not(any(feature = "javascript", feature = "python")),
		expect(unused_variables)
	)]
	let exports = match module.kind {
		tg::module::Kind::JavaScript | tg::module::Kind::TypeScript => {
			#[cfg(feature = "javascript")]
			{
				javascript::exports(module, text)
			}
			#[cfg(not(feature = "javascript"))]
			{
				return Err(tg::error!("the javascript feature is not enabled"));
			}
		},
		tg::module::Kind::Python => {
			#[cfg(feature = "python")]
			{
				python::exports(module, text)
			}
			#[cfg(not(feature = "python"))]
			{
				return Err(tg::error!("the python feature is not enabled"));
			}
		},
		tg::module::Kind::Config => {
			// Give a config module a default export, because its runtime declares its exports.
			#[cfg(any(feature = "javascript", feature = "python"))]
			{
				let export = Export {
					documentation: None,
					range: None,
				};
				Ok(std::collections::BTreeMap::from([(
					"default".to_owned(),
					export,
				)]))
			}
			#[cfg(not(any(feature = "javascript", feature = "python")))]
			{
				return Err(tg::error!(
					"the javascript and python features are not enabled"
				));
			}
		},
		_ => {
			return Err(tg::error!(kind = %module.kind, "cannot get the interface of the module"));
		},
	};

	#[cfg(any(feature = "javascript", feature = "python"))]
	{
		let exports = exports.map_err(
			|error| tg::error!(!error, module = ?module.without_token(), "failed to discover the exports"),
		)?;

		// Create the interface.
		let exports = exports
			.into_iter()
			.map(|(name, export)| {
				let range = export.range.and_then(|range| {
					tg::Range::try_from_byte_range_in_string(
						text,
						range,
						tg::position::Encoding::Utf8,
					)
				});
				let export = tg::interface::Export {
					documentation: export.documentation,
					error: None,
					range,
					type_: Some(untyped_command()),
				};
				(name, export)
			})
			.collect();
		let interface = tg::Interface {
			definitions: std::collections::BTreeMap::new(),
			exports,
			imports: std::collections::BTreeMap::new(),
		};

		Ok(interface)
	}
}

/// Get the name of an interface's import of a module, adding the import if the interface does not import the module yet.
#[cfg(all(feature = "javascript", feature = "python"))]
#[cfg_attr(not(any(feature = "tsrs", feature = "ty")), expect(dead_code))]
pub(crate) fn import(
	imports: &mut std::collections::BTreeMap<String, tg::module::Data>,
	module: &tg::module::Data,
) -> String {
	// Reuse the name of an existing import of the module, keeping the module's tokens so that the compiler can load it.
	let identity = module.without_token();
	if let Some((name, _)) = imports
		.iter()
		.find(|(_, import)| import.without_token() == identity)
	{
		return name.clone();
	}

	// Name the import after the module's file, and make the name unique.
	let path = match &module.referent.node {
		tg::module::data::Source::Path(path) => Some(path.as_path()),
		tg::module::data::Source::Edge(_) => module.referent.path(),
	};
	let stem = path
		.and_then(|path| path.file_name())
		.and_then(|name| name.to_str())
		.and_then(|name| name.split('.').next())
		.unwrap_or_default()
		.chars()
		.map(|character| {
			if character.is_ascii_alphanumeric() || character == '_' {
				character
			} else {
				'_'
			}
		})
		.collect::<String>();
	let stem = if stem.is_empty() || stem.starts_with(|character: char| character.is_ascii_digit())
	{
		format!("module{stem}")
	} else {
		stem
	};
	let mut name = stem.clone();
	let mut suffix = 1;
	while imports.contains_key(&name) {
		suffix += 1;
		name = format!("{stem}{suffix}");
	}
	imports.insert(name.clone(), module.clone());

	name
}

/// Describe why an export is not a value, with the message and values of each error in the error's chain.
#[cfg(all(feature = "javascript", feature = "python"))]
#[cfg_attr(not(any(feature = "tsrs", feature = "ty")), expect(dead_code))]
pub(crate) fn non_value_message(error: &tg::Error) -> String {
	let mut messages = Vec::new();
	let mut current = Some(error);
	while let Some(error) = current {
		let mut message = error
			.message()
			.unwrap_or_else(|| "an error occurred".to_owned());
		if let Some(object) = error.state().object()
			&& let Ok(object) = object.try_unwrap_error_ref()
			&& !object.values.is_empty()
		{
			let values = object
				.values
				.iter()
				.map(|(key, value)| format!("{key} = {value}"))
				.collect::<Vec<_>>()
				.join(", ");
			message = format!("{message} ({values})");
		}
		messages.push(message);
		current =
			std::error::Error::source(error).and_then(|source| source.downcast_ref::<tg::Error>());
	}

	messages.join(": ")
}

/// Get the type of a command whose args and output are any values.
#[cfg(any(feature = "javascript", feature = "python"))]
fn untyped_command() -> tg::interface::Type {
	let args = tg::interface::Type::Array {
		item: Box::new(tg::interface::Type::Value),
	};
	tg::interface::Type::Command {
		args: Box::new(args),
		output: Box::new(tg::interface::Type::Value),
	}
}

#[cfg(any(feature = "javascript", feature = "python"))]
fn located_error(
	module: &tg::module::Data,
	text: &str,
	range: std::ops::Range<usize>,
	error: tg::Error,
) -> tg::Error {
	let Ok(module) = tg::Module::try_from_data(module.without_token()) else {
		return error;
	};
	let Some(range) =
		tg::Range::try_from_byte_range_in_string(text, range, tg::position::Encoding::Utf8)
	else {
		return error;
	};
	let location = tg::error::Location {
		file: tg::error::File::Module(module),
		range,
		symbol: None,
	};
	let mut object = (**error.state().object().unwrap().unwrap_error_ref()).clone();
	object.location = Some(location);
	tg::Error::with_object(object)
}

#[cfg(all(test, any(feature = "javascript", feature = "python")))]
mod tests {
	use tangram_client::prelude::*;

	pub(super) fn source(kind: tg::module::Kind) -> tg::module::Data {
		tg::module::Data {
			kind,
			referent: tg::Referent::with_node(tg::module::data::Source::Path(
				"/test/module".into(),
			)),
		}
	}
}
