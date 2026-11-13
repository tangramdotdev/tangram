use {
	super::Export,
	oxc::ast::{
		Comment,
		ast::{
			BindingPattern, Declaration, ExportDefaultDeclarationKind, ImportDeclarationSpecifier,
			ImportOrExportKind, Statement, StringLiteral, WithClause,
		},
	},
	oxc::span::GetSpan as _,
	std::{collections::BTreeMap, ops::Range},
	tangram_client::prelude::*,
};

/// Discover a module's exports, with the ranges of their names, their documentation comments, and the namespaces and exports of other modules that they re-export.
pub(super) fn exports(
	module: &tg::module::Data,
	text: &str,
) -> tg::Result<BTreeMap<String, Export>> {
	let allocator = oxc::allocator::Allocator::default();
	let parsed = oxc::parser::Parser::new(&allocator, text, oxc::span::SourceType::ts()).parse();
	if let Some(error) = parsed.diagnostics.first().cloned() {
		let span = error.labels.first().map_or(0..0, |label| {
			label.offset() as usize..(label.offset() + label.len()) as usize
		});
		return Err(super::located_error(
			module,
			text,
			span,
			tg::error!(argument, !error, "failed to parse the JavaScript module"),
		));
	}

	// Get the namespaces and exports of other modules that the module imports as values, by their local names.
	let mut imports = BTreeMap::new();
	for statement in &parsed.program.body {
		let Statement::ImportDeclaration(declaration) = statement else {
			continue;
		};
		if declaration.import_kind != ImportOrExportKind::Value {
			continue;
		}
		for specifier in declaration.specifiers.iter().flatten() {
			let (local, export) = match specifier {
				ImportDeclarationSpecifier::ImportDefaultSpecifier(specifier) => {
					(&specifier.local, Some("default".to_owned()))
				},
				ImportDeclarationSpecifier::ImportNamespaceSpecifier(specifier) => {
					(&specifier.local, None)
				},
				ImportDeclarationSpecifier::ImportSpecifier(specifier) => {
					if specifier.import_kind != ImportOrExportKind::Value {
						continue;
					}
					(
						&specifier.local,
						Some(specifier.imported.name().to_string()),
					)
				},
			};
			let import = import(
				module,
				text,
				&declaration.source,
				declaration.with_clause.as_deref(),
			)?;
			let reexport = tg::interface::Reexport { export, import };
			imports.insert(local.name.to_string(), reexport);
		}
	}

	// Discover the exports, with the namespaces and exports of other modules that they re-export.
	let mut names = BTreeMap::new();
	for statement in &parsed.program.body {
		let documentation = documentation(&parsed.program.comments, statement.span().start, text);
		let mut ranges = BTreeMap::new();
		let mut reexports = BTreeMap::new();
		match statement {
			Statement::ExportAllDeclaration(export)
				if export.export_kind == ImportOrExportKind::Value =>
			{
				let Some(name) = &export.exported else {
					return Err(super::located_error(
						module,
						text,
						export.span.start as usize..export.span.end as usize,
						tg::error!(
							"cross-language imports require named re-exports instead of export *"
						),
					));
				};
				let span = name.span();
				let name = name.name().to_string();
				let import = import(module, text, &export.source, export.with_clause.as_deref())?;
				let reexport = tg::interface::Reexport {
					export: None,
					import,
				};
				ranges.insert(name.clone(), span.start as usize..span.end as usize);
				reexports.insert(name, reexport);
			},
			Statement::ExportDeclaration(export) => match &export.declaration {
				Declaration::ClassDeclaration(class) if !class.declare => {
					if let Some(id) = &class.id {
						ranges.insert(
							id.name.to_string(),
							id.span.start as usize..id.span.end as usize,
						);
					}
				},
				Declaration::FunctionDeclaration(function) if function.body.is_some() => {
					if let Some(id) = &function.id {
						ranges.insert(
							id.name.to_string(),
							id.span.start as usize..id.span.end as usize,
						);
					}
				},
				Declaration::TSEnumDeclaration(declaration) if !declaration.declare => {
					ranges.insert(
						declaration.id.name.to_string(),
						declaration.id.span.start as usize..declaration.id.span.end as usize,
					);
				},
				Declaration::VariableDeclaration(declaration) if !declaration.declare => {
					for declarator in &declaration.declarations {
						bindings(&declarator.id, &mut ranges);
					}
				},
				_ => {},
			},
			Statement::ExportDefaultDeclaration(export) => {
				if !matches!(
					export.declaration,
					ExportDefaultDeclarationKind::TSInterfaceDeclaration(_)
				) {
					ranges.insert(
						"default".to_owned(),
						export.span.start as usize..export.span.end as usize,
					);
				}
				if let ExportDefaultDeclarationKind::Identifier(identifier) = &export.declaration
					&& let Some(reexport) = imports.get(identifier.name.as_str())
				{
					reexports.insert("default".to_owned(), reexport.clone());
				}
			},
			Statement::ExportFromDeclaration(export)
				if export.export_kind == ImportOrExportKind::Value =>
			{
				let import = import(module, text, &export.source, export.with_clause.as_deref())?;
				for specifier in &export.specifiers {
					if specifier.export_kind == ImportOrExportKind::Value {
						let span = specifier.exported.span();
						let name = specifier.exported.name().to_string();
						let reexport = tg::interface::Reexport {
							export: Some(specifier.local.name().to_string()),
							import: import.clone(),
						};
						ranges.insert(name.clone(), span.start as usize..span.end as usize);
						reexports.insert(name, reexport);
					}
				}
			},
			Statement::ExportNamedDeclaration(export)
				if export.export_kind == ImportOrExportKind::Value =>
			{
				for specifier in &export.specifiers {
					if specifier.export_kind == ImportOrExportKind::Value {
						let span = specifier.exported.span();
						let name = specifier.exported.name().to_string();
						if let Some(reexport) = imports.get(specifier.local.name().as_str()) {
							reexports.insert(name.clone(), reexport.clone());
						}
						ranges.insert(name, span.start as usize..span.end as usize);
					}
				}
			},
			_ => {},
		}
		for (name, range) in ranges {
			let reexport = reexports.remove(&name);
			let export = Export {
				documentation: documentation.clone(),
				range: Some(range),
				reexport,
			};
			names.insert(name, export);
		}
	}

	Ok(names)
}

/// Get the import of a module from the specifier and the attributes of the statement that imports or re-exports from it.
fn import(
	module: &tg::module::Data,
	text: &str,
	source: &StringLiteral,
	with_clause: Option<&WithClause>,
) -> tg::Result<tg::module::Import> {
	let attributes = with_clause.map(crate::analyze::javascript::import_attributes);
	source
		.value
		.as_str()
		.ok_or_else(|| tg::error!("the import specifier must be valid unicode"))
		.and_then(|specifier| {
			tg::module::Import::with_specifier_and_attributes(specifier, attributes)
		})
		.map_err(|error| {
			super::located_error(
				module,
				text,
				source.span.start as usize..source.span.end as usize,
				tg::error!(!error, "failed to get the import of the module"),
			)
		})
}

/// Get the text of the documentation comment attached to the statement at an offset.
fn documentation(comments: &[Comment], offset: u32, text: &str) -> Option<String> {
	let comment = comments
		.iter()
		.rev()
		.find(|comment| comment.attached_to == offset && comment.is_jsdoc())?;
	let span = comment.content_span();
	let content = text.get(span.start as usize..span.end as usize)?;
	let lines = content
		.lines()
		.map(|line| {
			let line = line.trim();
			let line = line.trim_prefix('*');
			line.trim_prefix(' ')
		})
		.collect::<Vec<_>>();
	let documentation = lines.join("\n").trim().to_owned();
	if documentation.is_empty() {
		return None;
	}
	Some(documentation)
}

fn bindings(pattern: &BindingPattern, names: &mut BTreeMap<String, Range<usize>>) {
	match pattern {
		BindingPattern::ArrayPattern(pattern) => {
			for element in pattern.elements.iter().flatten() {
				bindings(element, names);
			}
			if let Some(rest) = &pattern.rest {
				bindings(&rest.argument, names);
			}
		},
		BindingPattern::AssignmentPattern(pattern) => bindings(&pattern.left, names),
		BindingPattern::BindingIdentifier(identifier) => {
			names.insert(
				identifier.name.to_string(),
				identifier.span.start as usize..identifier.span.end as usize,
			);
		},
		BindingPattern::ObjectPattern(pattern) => {
			for property in &pattern.properties {
				bindings(&property.value, names);
			}
			if let Some(rest) = &pattern.rest {
				bindings(&rest.argument, names);
			}
		},
	}
}

#[cfg(test)]
mod tests {
	use {super::*, std::collections::BTreeSet};

	fn exports(text: &str) -> tg::Result<BTreeSet<String>> {
		let exports = super::exports(
			&crate::interface::tests::source(tg::module::Kind::TypeScript),
			text,
		)?;
		Ok(exports.into_keys().collect())
	}

	#[test]
	fn parse_errors_have_original_source_locations() {
		let error = exports("\nexport const = 42;").unwrap_err();
		assert_eq!(error.kind(), Some(tg::error::Kind::Argument));
		let error = error.to_data_or_id().unwrap_left();
		assert!(error.source.is_some());
		let location = error.location.unwrap();
		assert_eq!(location.range.start.line, 1);
	}

	#[test]
	fn discovers_exports_without_inferring_types() {
		let text = "export default () => 42; export function a() {} export const b = factory(); export { b as alias }; export { c as renamed, type T } from './c'; export type { U } from './u'; export interface I {} export declare const ambient: number; export declare function ambientFunction(): void; export const {x, y: z} = value;";
		let names = exports(text).unwrap();
		assert_eq!(
			names,
			["a", "alias", "b", "default", "renamed", "x", "z"]
				.map(str::to_owned)
				.into()
		);
	}

	#[test]
	fn discovers_the_modules_of_reexports() {
		let text = r#"import * as local from "./local.tg.ts"; import * as types from "./types.tg.ts"; import { tool, tool as alias, type Type } from "./tools.tg.ts"; import main from "./main.tg.ts"; import type { Hidden } from "./hidden.tg.ts"; export * as tools from "./tools.tg.ts"; export * as scripts from "./scripts" with { kind: "python" }; export { local, local as renamed, tool, alias as aliased, main, Hidden }; export default types; export const value = local; export { tool as forwarded, default as fallback, "x-y" as dashed, type Type as T } from "./barrel.tg.ts";"#;
		let exports = super::exports(
			&crate::interface::tests::source(tg::module::Kind::TypeScript),
			text,
		)
		.unwrap();
		let reexports = exports
			.into_iter()
			.map(|(name, export)| {
				let reexport = export.reexport.map(|reexport| {
					let tg::interface::Reexport { export, import } = reexport;
					(import.reference.to_string(), import.kind, export)
				});
				(name, reexport)
			})
			.collect::<BTreeMap<_, _>>();
		let reexport = |specifier: &str, kind, export: Option<&str>| {
			Some((specifier.to_owned(), kind, export.map(ToOwned::to_owned)))
		};
		let expected = BTreeMap::from([
			("Hidden".to_owned(), None),
			(
				"aliased".to_owned(),
				reexport("./tools.tg.ts", None, Some("tool")),
			),
			(
				"dashed".to_owned(),
				reexport("./barrel.tg.ts", None, Some("x-y")),
			),
			("default".to_owned(), reexport("./types.tg.ts", None, None)),
			(
				"fallback".to_owned(),
				reexport("./barrel.tg.ts", None, Some("default")),
			),
			(
				"forwarded".to_owned(),
				reexport("./barrel.tg.ts", None, Some("tool")),
			),
			("local".to_owned(), reexport("./local.tg.ts", None, None)),
			(
				"main".to_owned(),
				reexport("./main.tg.ts", None, Some("default")),
			),
			("renamed".to_owned(), reexport("./local.tg.ts", None, None)),
			(
				"scripts".to_owned(),
				reexport("./scripts", Some(tg::module::Kind::Python), None),
			),
			(
				"tool".to_owned(),
				reexport("./tools.tg.ts", None, Some("tool")),
			),
			("tools".to_owned(), reexport("./tools.tg.ts", None, None)),
			("value".to_owned(), None),
		]);
		assert_eq!(reexports, expected);
	}

	#[test]
	fn rejects_wildcard_exports() {
		assert!(exports("export * from './other';").is_err());
		assert!(exports("export type * from './other';").unwrap().is_empty());
	}
}
