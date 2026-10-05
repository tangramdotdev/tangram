use {
	oxc::ast::ast::{
		BindingPattern, Declaration, ExportDefaultDeclarationKind, ImportOrExportKind, Statement,
	},
	oxc::span::GetSpan as _,
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::Range,
	},
	tangram_client::prelude::*,
};

pub(super) fn exports(module: &tg::module::Data, text: &str) -> tg::Result<BTreeSet<String>> {
	Ok(declarations(module, text)?.into_keys().collect())
}

pub(super) fn declarations(
	module: &tg::module::Data,
	text: &str,
) -> tg::Result<BTreeMap<String, Range<usize>>> {
	let allocator = oxc::allocator::Allocator::default();
	let parsed = oxc::parser::Parser::new(&allocator, text, oxc::span::SourceType::ts()).parse();
	if let Some(error) = parsed.diagnostics.first() {
		let span = error.labels.first().map_or(0..0, |label| {
			label.offset() as usize..(label.offset() + label.len()) as usize
		});
		return Err(super::located_error(
			module,
			text,
			span,
			tg::error!("failed to parse the JavaScript module: {error}"),
		));
	}
	let mut names = BTreeMap::new();
	for statement in &parsed.program.body {
		match statement {
			Statement::ExportDeclaration(export) => match &export.declaration {
				Declaration::FunctionDeclaration(function) if function.body.is_some() => {
					if let Some(id) = &function.id {
						names.insert(
							id.name.to_string(),
							id.span.start as usize..id.span.end as usize,
						);
					}
				},
				Declaration::VariableDeclaration(declaration) if !declaration.declare => {
					for declarator in &declaration.declarations {
						bindings(&declarator.id, &mut names);
					}
				},
				Declaration::ClassDeclaration(class) if !class.declare => {
					if let Some(id) = &class.id {
						names.insert(
							id.name.to_string(),
							id.span.start as usize..id.span.end as usize,
						);
					}
				},
				Declaration::TSEnumDeclaration(declaration) if !declaration.declare => {
					names.insert(
						declaration.id.name.to_string(),
						declaration.id.span.start as usize..declaration.id.span.end as usize,
					);
				},
				_ => {},
			},
			Statement::ExportDefaultDeclaration(export) => {
				if !matches!(
					export.declaration,
					ExportDefaultDeclarationKind::TSInterfaceDeclaration(_)
				) {
					names.insert(
						"default".to_owned(),
						export.span.start as usize..export.span.end as usize,
					);
				}
			},
			Statement::ExportNamedDeclaration(export) => {
				if export.export_kind == ImportOrExportKind::Value {
					for specifier in &export.specifiers {
						if specifier.export_kind == ImportOrExportKind::Value {
							let span = specifier.exported.span();
							names.insert(
								specifier.exported.name().to_string(),
								span.start as usize..span.end as usize,
							);
						}
					}
				}
			},
			Statement::ExportFromDeclaration(export) => {
				if export.export_kind == ImportOrExportKind::Value {
					for specifier in &export.specifiers {
						if specifier.export_kind == ImportOrExportKind::Value {
							let span = specifier.exported.span();
							names.insert(
								specifier.exported.name().to_string(),
								span.start as usize..span.end as usize,
							);
						}
					}
				}
			},
			Statement::ExportAllDeclaration(export)
				if export.export_kind == ImportOrExportKind::Value =>
			{
				return Err(super::located_error(
					module,
					text,
					export.span.start as usize..export.span.end as usize,
					tg::error!(
						"cross-language imports require named re-exports instead of export *"
					),
				));
			},
			_ => {},
		}
	}
	Ok(names)
}

fn bindings(pattern: &BindingPattern, names: &mut BTreeMap<String, Range<usize>>) {
	match pattern {
		BindingPattern::BindingIdentifier(identifier) => {
			names.insert(
				identifier.name.to_string(),
				identifier.span.start as usize..identifier.span.end as usize,
			);
		},
		BindingPattern::ArrayPattern(pattern) => {
			for element in pattern.elements.iter().flatten() {
				bindings(element, names);
			}
			if let Some(rest) = &pattern.rest {
				bindings(&rest.argument, names);
			}
		},
		BindingPattern::AssignmentPattern(pattern) => bindings(&pattern.left, names),
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
	use super::*;

	fn exports(text: &str) -> tg::Result<BTreeSet<String>> {
		super::exports(&crate::load::tests::source(tg::module::Kind::Ts), text)
	}

	#[test]
	fn parse_errors_have_original_source_locations() {
		let error = exports("\nexport const = 42;")
			.unwrap_err()
			.to_data_or_id()
			.unwrap_left();
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
	fn rejects_wildcard_exports() {
		assert!(exports("export * from './other';").is_err());
		assert!(exports("export type * from './other';").unwrap().is_empty());
	}
}
