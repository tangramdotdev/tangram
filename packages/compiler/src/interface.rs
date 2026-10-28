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
					range,
					type_: untyped_command(),
				};
				(name, export)
			})
			.collect();
		let interface = tg::Interface {
			definitions: std::collections::BTreeMap::new(),
			exports,
		};

		Ok(interface)
	}
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
