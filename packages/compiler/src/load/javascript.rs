use {
	super::Export,
	oxc::ast::{
		Comment,
		ast::{
			BindingPattern, Declaration, ExportDefaultDeclarationKind, ImportOrExportKind,
			Statement,
		},
	},
	oxc::span::GetSpan as _,
	std::{collections::BTreeMap, ops::Range},
	tangram_client::prelude::*,
};

/// Discover a module's exports, with the ranges of their names and their documentation comments.
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
			tg::error!(!error, "failed to parse the JavaScript module"),
		));
	}
	let mut names = BTreeMap::new();
	for statement in &parsed.program.body {
		let documentation = documentation(&parsed.program.comments, statement.span().start, text);
		let mut ranges = BTreeMap::new();
		match statement {
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
			},
			Statement::ExportFromDeclaration(export)
				if export.export_kind == ImportOrExportKind::Value =>
			{
				for specifier in &export.specifiers {
					if specifier.export_kind == ImportOrExportKind::Value {
						let span = specifier.exported.span();
						ranges.insert(
							specifier.exported.name().to_string(),
							span.start as usize..span.end as usize,
						);
					}
				}
			},
			Statement::ExportNamedDeclaration(export)
				if export.export_kind == ImportOrExportKind::Value =>
			{
				for specifier in &export.specifiers {
					if specifier.export_kind == ImportOrExportKind::Value {
						let span = specifier.exported.span();
						ranges.insert(
							specifier.exported.name().to_string(),
							span.start as usize..span.end as usize,
						);
					}
				}
			},
			_ => {},
		}
		for (name, range) in ranges {
			let export = Export {
				documentation: documentation.clone(),
				range: Some(range),
			};
			names.insert(name, export);
		}
	}
	Ok(names)
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
			let line = line.strip_prefix('*').unwrap_or(line);
			line.strip_prefix(' ').unwrap_or(line)
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
			&crate::load::tests::source(tg::module::Kind::TypeScript),
			text,
		)?;
		Ok(exports.into_keys().collect())
	}

	#[test]
	fn parse_errors_have_original_source_locations() {
		let error = exports("\nexport const = 42;")
			.unwrap_err()
			.to_data_or_id()
			.unwrap_left();
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
	fn rejects_wildcard_exports() {
		assert!(exports("export * from './other';").is_err());
		assert!(exports("export type * from './other';").unwrap().is_empty());
	}
}
