use {
	super::Analysis,
	oxc::{
		ast::ast::{
			ExportAllDeclaration, ExportFromDeclaration, Expression, ImportAttributeKey,
			ImportDeclaration, ObjectPropertyKind, PropertyKey, WithClause,
		},
		ast_visit::Visit as _,
		span::Span,
	},
	std::collections::{BTreeMap, HashSet},
	tangram_client::prelude::*,
};

struct Visitor<'a> {
	diagnostics: Vec<tg::diagnostic::Data>,
	imports: HashSet<tg::module::Import, fnv::FnvBuildHasher>,
	module: &'a tg::module::Data,
	static_imports: HashSet<tg::module::Import, fnv::FnvBuildHasher>,
	text: &'a str,
}

pub(super) fn analyze(module: &tg::module::Data, text: &str) -> Analysis {
	let allocator = oxc::allocator::Allocator::default();

	let mut diagnostics = Vec::new();

	// Parse the text.
	let source_type = oxc::span::SourceType::ts();
	let output = oxc::parser::Parser::new(&allocator, text, source_type).parse();
	for error in &output.diagnostics {
		diagnostics.push(crate::util::convert_diagnostic(error, module, text));
	}

	// Create the visitor and visit the module.
	let mut visitor = Visitor::new(module, text);
	visitor.visit_program(&output.program);
	diagnostics.extend(visitor.diagnostics);
	let imports = visitor.imports;
	let static_imports = visitor.static_imports;

	Analysis {
		diagnostics,
		imports,
		static_imports,
	}
}

/// Get the attributes of an import from its with clause.
pub(crate) fn import_attributes(with_clause: &WithClause) -> BTreeMap<String, String> {
	let mut attributes = BTreeMap::new();
	for entry in &with_clause.with_entries {
		let key = match &entry.key {
			ImportAttributeKey::Identifier(ident) => ident.name.to_string(),
			ImportAttributeKey::StringLiteral(lit) => {
				let Some(key) = lit.value.as_str() else {
					continue;
				};
				key.to_owned()
			},
		};
		let Some(value) = entry.value.value.as_str() else {
			continue;
		};
		attributes.insert(key, value.to_owned());
	}
	attributes
}

impl<'a> Visitor<'a> {
	fn new(module: &'a tg::module::Data, text: &'a str) -> Self {
		Self {
			diagnostics: Vec::new(),
			imports: HashSet::default(),
			module,
			static_imports: HashSet::default(),
			text,
		}
	}

	fn get_import_attributes_from_import_expression_options(
		options: &Expression<'a>,
	) -> Option<BTreeMap<String, String>> {
		let Expression::ObjectExpression(obj) = options else {
			return None;
		};
		for property in &obj.properties {
			let ObjectPropertyKind::ObjectProperty(prop) = property else {
				continue;
			};
			let is_with = match &prop.key {
				PropertyKey::StaticIdentifier(ident) => ident.name == "with",
				PropertyKey::StringLiteral(lit) => lit.value == "with",
				_ => false,
			};
			if !is_with {
				continue;
			}
			let Expression::ObjectExpression(attrs_obj) = &prop.value else {
				continue;
			};
			let mut attributes = BTreeMap::new();
			for kind in &attrs_obj.properties {
				let ObjectPropertyKind::ObjectProperty(property) = kind else {
					continue;
				};
				let key = match &property.key {
					PropertyKey::StaticIdentifier(identifier) => identifier.name.to_string(),
					PropertyKey::StringLiteral(literal) => {
						let Some(key) = literal.value.as_str() else {
							continue;
						};
						key.to_owned()
					},
					_ => continue,
				};
				if let Expression::StringLiteral(value) = &property.value
					&& let Some(value) = value.value.as_str()
				{
					attributes.insert(key, value.to_owned());
				}
			}
			return Some(attributes);
		}
		None
	}

	fn add_static_import(
		&mut self,
		specifier: Option<&str>,
		attributes: Option<&BTreeMap<String, String>>,
		span: Span,
	) {
		if let Some(import) = self.add_import(specifier, attributes, span) {
			self.static_imports.insert(import);
		}
	}

	fn add_import(
		&mut self,
		specifier: Option<&str>,
		attributes: Option<&BTreeMap<String, String>>,
		span: Span,
	) -> Option<tg::module::Import> {
		let result = specifier
			.ok_or_else(|| tg::error!("the import specifier must be valid unicode"))
			.and_then(|specifier| {
				tg::module::Import::with_specifier_and_attributes(specifier, attributes.cloned())
			});
		match result {
			Ok(import) => {
				self.imports.insert(import.clone());
				Some(import)
			},
			Err(error) => {
				let byte_range = span.start as usize..span.end as usize;
				let range = tg::Range::try_from_byte_range_in_string(
					self.text,
					byte_range,
					tg::position::Encoding::Utf8,
				)
				.unwrap_or(tg::Range {
					start: tg::Position {
						line: 0,
						character: 0,
					},
					end: tg::Position {
						line: 0,
						character: 0,
					},
				});
				let location = Some(tg::module::data::Location {
					module: self.module.clone(),
					range,
				});
				let diagnostic = tg::diagnostic::Data {
					location,
					message: error
						.message()
						.unwrap_or_else(|| "failed to parse the import".to_owned()),
					severity: tg::diagnostic::Severity::Error,
				};
				self.diagnostics.push(diagnostic);
				None
			},
		}
	}
}

impl<'a> oxc::ast_visit::Visit<'a> for Visitor<'a> {
	fn visit_import_declaration(&mut self, declaration: &ImportDeclaration<'a>) {
		let specifier = declaration.source.value.as_str();
		let attributes = declaration.with_clause.as_deref().map(import_attributes);
		if declaration.import_kind.is_type() {
			self.add_import(specifier, attributes.as_ref(), declaration.span);
		} else {
			self.add_static_import(specifier, attributes.as_ref(), declaration.span);
		}
	}

	fn visit_export_from_declaration(&mut self, declaration: &ExportFromDeclaration<'a>) {
		let specifier = declaration.source.value.as_str();
		let attributes = declaration.with_clause.as_deref().map(import_attributes);
		if declaration.export_kind.is_type() {
			self.add_import(specifier, attributes.as_ref(), declaration.span);
		} else {
			self.add_static_import(specifier, attributes.as_ref(), declaration.span);
		}
		oxc::ast_visit::walk::walk_export_from_declaration(self, declaration);
	}

	fn visit_export_all_declaration(&mut self, declaration: &ExportAllDeclaration<'a>) {
		let specifier = declaration.source.value.as_str();
		let attributes = declaration.with_clause.as_deref().map(import_attributes);
		if declaration.export_kind.is_type() {
			self.add_import(specifier, attributes.as_ref(), declaration.span);
		} else {
			self.add_static_import(specifier, attributes.as_ref(), declaration.span);
		}
	}

	fn visit_import_expression(&mut self, expression: &oxc::ast::ast::ImportExpression<'a>) {
		if let Expression::StringLiteral(literal) = &expression.source {
			let attributes = expression.options.as_ref().and_then(|import_attributes| {
				Self::get_import_attributes_from_import_expression_options(import_attributes)
			});
			self.add_import(literal.value.as_str(), attributes.as_ref(), expression.span);
		} else {
			self.diagnostics.push(tg::diagnostic::Data {
				message: "The argument to the import function must be a string literal.".to_owned(),
				location: {
					let byte_range = expression.span.start as usize..expression.span.end as usize;
					let range = tg::Range::try_from_byte_range_in_string(
						self.text,
						byte_range,
						tg::position::Encoding::Utf8,
					)
					.unwrap_or(tg::Range {
						start: tg::Position {
							line: 0,
							character: 0,
						},
						end: tg::Position {
							line: 0,
							character: 0,
						},
					});
					Some(tg::module::data::Location {
						module: self.module.clone(),
						range,
					})
				},
				severity: tg::diagnostic::Severity::Error,
			});
		}
		oxc::ast_visit::walk::walk_import_expression(self, expression);
	}
}

#[cfg(test)]
mod tests {
	use {super::*, crate::Compiler, indoc::indoc};

	// Analysis collects the specifiers of every static import, dynamic import, and re-export, including nested ones.
	#[test]
	fn test() {
		let text = indoc!(
			r#"
				import defaultImport from "default_import";
				import { namedImport } from "./named_import.tg.js";
				import * as namespaceImport from "namespace_import";
				let dynamicImport = import("./dynamic_import.tg.ts");
				let include = import("./include.txt");
				export function nested() {
					let nestedDynamicImport = import("nested_dynamic_import");
					let nestedInclude = import("./nested_include.txt");
				}
				export { namedExport } from "named_export";
				export * as namespaceExport from "./namespace_export.ts";
			"#
		);
		let module = tg::module::Data {
			kind: tg::module::Kind::TypeScript,
			referent: tg::Referent::with_node(tg::module::data::Source::Path("test.tg.ts".into())),
		};
		let analysis = Compiler::analyze(&module, text).unwrap();
		let found = analysis.imports;
		let found_static = analysis.static_imports;
		let expected_static = [
			"default_import",
			"./named_import.tg.js",
			"namespace_import",
			"named_export",
			"./namespace_export.ts",
		]
		.into_iter()
		.map(|specifier| {
			tg::module::Import::with_specifier_and_attributes(specifier, None).unwrap()
		})
		.collect();
		assert_eq!(found_static, expected_static);
		let expected = [
			"default_import",
			"./named_import.tg.js",
			"namespace_import",
			"./dynamic_import.tg.ts",
			"nested_dynamic_import",
			"named_export",
			"./namespace_export.ts",
			"./include.txt",
			"./nested_include.txt",
		]
		.into_iter()
		.map(|specifier| {
			tg::module::Import::with_specifier_and_attributes(specifier, None).unwrap()
		})
		.collect();
		assert_eq!(found, expected);
	}
}
