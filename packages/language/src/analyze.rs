use crate::{parse, Import, Module};
use itertools::Itertools;
use std::{
	collections::{BTreeMap, HashSet},
	rc::Rc,
};
use swc::ecma::{ast, visit::VisitWith};
use swc_core as swc;
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Analysis {
	pub metadata: Option<tg::package::Metadata>,
	pub imports: HashSet<Import, fnv::FnvBuildHasher>,
	pub includes: HashSet<tg::Path, fnv::FnvBuildHasher>,
}

pub struct Error {
	pub message: String,
	pub line: usize,
	pub column: usize,
}

impl Module {
	#[tracing::instrument(skip(text))]
	pub fn analyze(text: String) -> Result<Analysis> {
		// Parse the text.
		let parse::Output {
			program,
			source_map,
		} = Module::parse(text).wrap_err("Failed to parse the module.")?;

		// Create the visitor and visit the module.
		let mut visitor = Visitor::new(source_map);
		program.visit_with(&mut visitor);

		// Handle any errors.
		let errors = visitor.errors;
		if !errors.is_empty() {
			let message = errors
				.iter()
				.map(ToString::to_string)
				.collect_vec()
				.join("\n");
			return Err(error!("{message}"));
		}

		// Create the output.
		let output = Analysis {
			metadata: visitor.metadata,
			imports: visitor.imports,
			includes: visitor.includes,
		};

		Ok(output)
	}
}

impl Error {
	pub fn new(message: impl std::fmt::Display, loc: &swc::common::Loc) -> Self {
		let line = loc.line - 1;
		let column = loc.col_display;
		Self {
			message: message.to_string(),
			line,
			column,
		}
	}
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let line = self.line + 1;
		let column = self.column + 1;
		let message = &self.message;
		write!(f, "{line}:{column} {message}").unwrap();
		Ok(())
	}
}

#[derive(Default)]
struct Visitor {
	source_map: Rc<swc::common::SourceMap>,
	errors: Vec<Error>,
	metadata: Option<tg::package::Metadata>,
	imports: HashSet<Import, fnv::FnvBuildHasher>,
	includes: HashSet<tg::Path, fnv::FnvBuildHasher>,
}

impl Visitor {
	fn new(source_map: Rc<swc::common::SourceMap>) -> Self {
		Self {
			source_map,
			..Default::default()
		}
	}
}

impl swc::ecma::visit::Visit for Visitor {
	fn visit_export_decl(&mut self, n: &ast::ExportDecl) {
		// Check that this export statement has a declaration.
		let Some(decl) = n.decl.as_var() else {
			n.visit_children_with(self);
			return;
		};

		// Visit each declaration.
		for decl in &decl.decls {
			// Get the object from the declaration.
			let ast::VarDeclarator { name, init, .. } = decl;
			let Some(ident) = name.as_ident().map(|ident| &ident.sym) else {
				continue;
			};
			if ident != "metadata" {
				continue;
			}
			let Some(init) = init.as_deref() else {
				continue;
			};
			let Some(object) = init.as_object() else {
				continue;
			};
			let metadata = self.object_to_json(object);
			let Ok(metadata) = serde_json::from_value(metadata) else {
				continue;
			};
			self.metadata = Some(metadata);
		}

		n.visit_children_with(self);
	}

	fn visit_import_decl(&mut self, n: &ast::ImportDecl) {
		self.add_import(&n.src.value, n.with.as_deref(), n.span);
	}

	fn visit_named_export(&mut self, n: &ast::NamedExport) {
		if let Some(src) = n.src.as_deref() {
			self.add_import(&src.value, n.with.as_deref(), n.span);
		}
	}

	fn visit_export_all(&mut self, n: &ast::ExportAll) {
		self.add_import(&n.src.value, n.with.as_deref(), n.span);
	}

	fn visit_call_expr(&mut self, n: &ast::CallExpr) {
		match &n.callee {
			// Handle a call expression.
			ast::Callee::Expr(callee) => {
				// Ignore call expressions that are not tg.include().
				let Some(callee) = callee.as_member() else {
					n.visit_children_with(self);
					return;
				};
				let Some(obj) = callee.obj.as_ident() else {
					n.visit_children_with(self);
					return;
				};
				let Some(prop) = callee.prop.as_ident() else {
					n.visit_children_with(self);
					return;
				};
				if !(&obj.sym == "tg" && &prop.sym == "include") {
					n.visit_children_with(self);
					return;
				}

				// Get the location of the call.
				let loc = self.source_map.lookup_char_pos(n.span.lo);

				// Get the argument and verify it is a string literal.
				if n.args.len() != 1 {
					self.errors.push(Error::new(
						"tg.include must be called with exactly one argument.",
						&loc,
					));
					return;
				}
				let Some(ast::Lit::Str(arg)) = n.args[0].expr.as_lit() else {
					self.errors.push(Error::new(
						"The argument to tg.include must be a string literal.",
						&loc,
					));
					return;
				};

				// Parse the argument and add it to the set of includes.
				let Ok(include) = arg.value.to_string().parse() else {
					self.errors.push(Error::new(
						"Failed to parse the argument to tg.include.",
						&loc,
					));
					return;
				};
				self.includes.insert(include);
			},

			// Handle a dynamic import.
			ast::Callee::Import(_) => {
				let Some(ast::Lit::Str(arg)) = n.args.first().and_then(|arg| arg.expr.as_lit())
				else {
					let loc = self.source_map.lookup_char_pos(n.span.lo);
					self.errors.push(Error::new(
						"The argument to the import function must be a string literal.",
						&loc,
					));
					return;
				};
				let with = n.args.get(1).and_then(|arg| arg.expr.as_object());
				self.add_import(&arg.value, with, n.span);
			},

			// Ignore a call to super.
			ast::Callee::Super(_) => {
				n.visit_children_with(self);
			},
		}
	}
}

impl Visitor {
	fn add_import(
		&mut self,
		specifier: &str,
		attributes: Option<&ast::ObjectLit>,
		span: swc::common::Span,
	) {
		// Get the attributes.
		let attributes = if let Some(attributes) = attributes {
			let mut map = BTreeMap::default();
			let loc = self.source_map.lookup_char_pos(attributes.span.lo);
			for prop in &attributes.props {
				let Some(prop) = prop.as_prop() else {
					self.errors
						.push(Error::new("Spread properties are not allowed.", &loc));
					continue;
				};
				let Some(key_value) = prop.as_key_value() else {
					self.errors
						.push(Error::new("Only key-value properties are allowed.", &loc));
					continue;
				};
				let key = match &key_value.key {
					ast::PropName::Ident(ident) => ident.sym.to_string(),
					ast::PropName::Str(value) => value.value.to_string(),
					_ => {
						self.errors
							.push(Error::new("All keys must be strings.", &loc));
						continue;
					},
				};
				let value = if let ast::Expr::Lit(ast::Lit::Str(value)) = key_value.value.as_ref() {
					value.value.to_string()
				} else {
					self.errors
						.push(Error::new("All values must be strings.", &loc));
					continue;
				};
				map.insert(key, value);
			}
			Some(map)
		} else {
			None
		};

		// Parse the import.
		let Ok(import) = Import::with_specifier_and_attributes(specifier, attributes.as_ref())
		else {
			let loc = self.source_map.lookup_char_pos(span.lo());
			self.errors
				.push(Error::new("Failed to parse the import.", &loc));
			return;
		};

		// Add the import.
		self.imports.insert(import);
	}

	fn object_to_json(&mut self, object: &ast::ObjectLit) -> serde_json::Value {
		let mut output = serde_json::Map::new();
		let loc = self.source_map.lookup_char_pos(object.span.lo);
		for prop in &object.props {
			let Some(prop) = prop.as_prop() else {
				self.errors
					.push(Error::new("Spread properties are not allowed.", &loc));
				continue;
			};
			let Some(key_value) = prop.as_key_value() else {
				self.errors
					.push(Error::new("Only key-value properties are allowed.", &loc));
				continue;
			};
			let key = match &key_value.key {
				ast::PropName::Ident(ident) => ident.sym.to_string(),
				ast::PropName::Str(value) => value.value.to_string(),
				_ => {
					self.errors.push(Error::new("Keys must be strings.", &loc));
					continue;
				},
			};
			let value = match key_value.value.as_ref() {
				ast::Expr::Lit(ast::Lit::Null(_)) => serde_json::Value::Null,
				ast::Expr::Lit(ast::Lit::Bool(value)) => serde_json::Value::Bool(value.value),
				ast::Expr::Lit(ast::Lit::Num(value)) => {
					let Some(value) = serde_json::Number::from_f64(value.value) else {
						self.errors.push(Error::new("Invalid number.", &loc));
						continue;
					};
					serde_json::Value::Number(value)
				},
				ast::Expr::Lit(ast::Lit::Str(value)) => {
					serde_json::Value::String(value.value.to_string())
				},
				_ => {
					self.errors
						.push(Error::new("Values must be valid JSON.", &loc));
					continue;
				},
			};
			output.insert(key, value);
		}
		serde_json::Value::Object(output)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_analyze() {
		let text = r#"
			export let metadata = { name: "name", version: "version" };
			import defaultImport from "tg:default_import";
			import { namedImport } from "./named_import.tg";
			import * as namespaceImport from "tg:namespace_import";
			let dynamicImport = import("./dynamic_import.tg");
			let include = tg.include("./include.txt");
			export let nested = tg.target(() => {
				let nestedDynamicImport = import("tg:nested_dynamic_import");
				let nestedInclude = tg.include("./nested_include.txt");
			});
			export { namedExport } from "tg:named_export";
			export * as namespaceExport from "./namespace_export.tg";
		"#;
		let left = Module::analyze(text.to_owned()).unwrap();
		let metadata = tg::package::Metadata {
			name: Some("name".to_owned()),
			version: Some("version".to_owned()),
			..Default::default()
		};
		let imports = [
			"tg:default_import",
			"./named_import.tg",
			"tg:namespace_import",
			"./dynamic_import.tg",
			"tg:nested_dynamic_import",
			"tg:named_export",
			"./namespace_export.tg",
		]
		.into_iter()
		.map(|import| import.parse().unwrap())
		.collect();
		let includes = ["./include.txt", "./nested_include.txt"]
			.into_iter()
			.map(|include| include.parse().unwrap())
			.collect();
		let right = Analysis {
			metadata: Some(metadata),
			imports,
			includes,
		};
		assert_eq!(left, right);
	}
}
