use {
	super::Export,
	ruff_python_ast::{Expr, Stmt},
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
	ty_text_size::Ranged,
};

/// Discover a module's exports, with the ranges of their names and their docstrings.
pub(super) fn exports(
	module: &tg::module::Data,
	text: &str,
) -> tg::Result<BTreeMap<String, Export>> {
	let parsed = ruff_python_parser::parse_module(text).map_err(|error| {
		let range = usize::from(error.location.start())..usize::from(error.location.end());
		super::located_error(
			module,
			text,
			range,
			tg::error!(argument, !error, "failed to parse the python module"),
		)
	})?;
	crate::analyze::python::validate_imports(module, text, &parsed.syntax().body)?;
	let mut names = BTreeSet::new();
	let mut all = None;
	let mut sources = BTreeMap::new();
	let body = &parsed.syntax().body;
	for (index, statement) in body.iter().enumerate() {
		// Record the range of each name the statement binds and its docstring.
		let documentation = match statement {
			Stmt::ClassDef(class) => class.body.first().and_then(docstring),
			Stmt::FunctionDef(function) => function.body.first().and_then(docstring),
			_ => body.get(index + 1).and_then(docstring),
		};
		let mut ranges = Vec::new();
		match statement {
			Stmt::AnnAssign(assign) => ranges.push(assign.target.range()),
			Stmt::Assign(assign) => ranges.extend(assign.targets.iter().map(Ranged::range)),
			Stmt::ClassDef(class) => ranges.push(class.name.range()),
			Stmt::FunctionDef(function) => ranges.push(function.name.range()),
			Stmt::ImportFrom(import) => ranges.extend(
				import
					.names
					.iter()
					.map(|alias| alias.asname.as_ref().unwrap_or(&alias.name).range()),
			),
			_ => {},
		}
		for range in ranges {
			let Some(name) = text.get(usize::from(range.start())..usize::from(range.end())) else {
				continue;
			};
			let export = Export {
				documentation: documentation.clone(),
				range: Some(usize::from(range.start())..usize::from(range.end())),
			};
			sources.insert(name.to_owned(), export);
		}

		match statement {
			Stmt::AnnAssign(assign) => {
				if let Some(value) = &assign.value {
					bindings(&assign.target, &mut names);
					if matches!(assign.target.as_ref(), Expr::Name(name) if name.id.as_str() == "__all__")
					{
						all = Some(literal_names(value).map_err(|error| {
							super::located_error(
								module,
								text,
								usize::from(assign.range.start())..usize::from(assign.range.end()),
								error,
							)
						})?);
					}
				}
			},
			Stmt::Assign(assign) => {
				for target in &assign.targets {
					bindings(target, &mut names);
					if matches!(target, Expr::Name(name) if name.id.as_str() == "__all__") {
						all = Some(literal_names(&assign.value).map_err(|error| {
							super::located_error(
								module,
								text,
								usize::from(assign.range.start())..usize::from(assign.range.end()),
								error,
							)
						})?);
					}
				}
			},
			Stmt::AugAssign(assign) if matches!(assign.target.as_ref(), Expr::Name(name) if name.id.as_str() == "__all__") =>
			{
				return Err(tg::error!(
					"cross-language imports require a literal __all__ assignment"
				));
			},
			Stmt::ClassDef(class) => {
				names.insert(class.name.to_string());
			},
			Stmt::Delete(delete) => {
				let mut deleted = BTreeSet::new();
				for target in &delete.targets {
					bindings(target, &mut deleted);
				}
				names.retain(|name| !deleted.contains(name));
			},
			Stmt::FunctionDef(function) => {
				names.insert(function.name.to_string());
			},
			Stmt::ImportFrom(import) => {
				for alias in &import.names {
					names.insert(alias.asname.as_ref().unwrap_or(&alias.name).to_string());
				}
			},
			_ => {},
		}
	}
	if let Some(all) = all {
		names = all;
	} else {
		names.retain(|name| !name.starts_with('_'));
	}

	// Attach the source of each export.
	let exports = names
		.into_iter()
		.map(|name| {
			let export = sources.remove(&name).unwrap_or(Export {
				documentation: None,
				range: None,
			});
			(name, export)
		})
		.collect();

	Ok(exports)
}

/// Get the cleaned text of a statement that is a docstring.
fn docstring(statement: &Stmt) -> Option<String> {
	let Stmt::Expr(expression) = statement else {
		return None;
	};
	let literal = expression.value.as_string_literal_expr()?;
	let text = literal.value.to_str();
	let mut lines = text.lines();
	let first = lines.next().unwrap_or_default().trim();
	let rest = lines.collect::<Vec<_>>();
	let indentation = rest
		.iter()
		.filter(|line| !line.trim().is_empty())
		.map(|line| line.len() - line.trim_start().len())
		.min()
		.unwrap_or(0);
	let text = std::iter::once(first)
		.chain(
			rest.iter()
				.map(|line| line.get(indentation..).unwrap_or_default()),
		)
		.collect::<Vec<_>>()
		.join("\n")
		.trim()
		.to_owned();
	if text.is_empty() {
		return None;
	}
	Some(text)
}

fn bindings(expression: &Expr, names: &mut BTreeSet<String>) {
	match expression {
		Expr::List(list) => {
			for element in &list.elts {
				bindings(element, names);
			}
		},
		Expr::Name(name) => {
			names.insert(name.id.to_string());
		},
		Expr::Starred(starred) => bindings(&starred.value, names),
		Expr::Tuple(tuple) => {
			for element in &tuple.elts {
				bindings(element, names);
			}
		},
		_ => {},
	}
}

fn literal_names(expression: &Expr) -> tg::Result<BTreeSet<String>> {
	let elements = match expression {
		Expr::List(list) => &list.elts,
		Expr::Tuple(tuple) => &tuple.elts,
		_ => {
			return Err(tg::error!(
				"cross-language imports require a literal __all__ list or tuple"
			));
		},
	};
	elements
		.iter()
		.map(|element| match element {
			Expr::StringLiteral(literal) => Ok(literal.value.to_string()),
			_ => Err(tg::error!(
				"cross-language imports require literal strings in __all__"
			)),
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	fn exports(text: &str) -> tg::Result<BTreeSet<String>> {
		let exports = super::exports(
			&crate::interface::tests::source(tg::module::Kind::Python),
			text,
		)?;
		Ok(exports.into_keys().collect())
	}

	#[test]
	fn parse_errors_have_original_source_locations() {
		let error = exports("\ndef broken(: pass").unwrap_err();
		assert_eq!(error.kind(), Some(tg::error::Kind::Argument));
		let error = error.to_data_or_id().unwrap_left();
		let location = error.location.unwrap();
		assert_eq!(location.range.start.line, 1);
	}

	#[test]
	fn discovers_public_bindings_and_aliases() {
		let text = "from .other import a as renamed\nimport os\ndef default(): pass\nasync def work(): pass\nalias = work\n_hidden = work\ndef removed(): pass\ndel removed\n";
		assert_eq!(
			exports(text).unwrap(),
			["alias", "default", "renamed", "work"]
				.map(str::to_owned)
				.into()
		);
	}

	#[test]
	fn honors_explicit_exports() {
		let text = "from .other import _private, default\n__all__ = ['_private', 'default']\n";
		assert_eq!(
			exports(text).unwrap(),
			["_private", "default"].map(str::to_owned).into()
		);
		assert!(exports("__all__ = compute_exports()").is_err());
		assert!(exports("from .other import *").is_err());
		assert!(exports("from .other import *\n__all__ = ['default']").is_err());
	}
}
