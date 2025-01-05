use super::Compiler;
use itertools::Itertools as _;
use std::{
	collections::{BTreeMap, HashSet},
	rc::Rc,
};
use swc::ecma::{ast, visit::VisitWith};
use swc_core::{self as swc, common::Spanned};
use tangram_client as tg;

#[derive(Clone, Debug, PartialEq)]
pub struct Analysis {
	pub imports: HashSet<tg::Import, fnv::FnvBuildHasher>,
	pub metadata: Option<BTreeMap<String, tg::value::Data>>,
}

pub struct Error {
	pub message: String,
	pub line: usize,
	pub column: usize,
}

impl Compiler {
	/// Analyze a module.
	pub fn analyze_module(text: String) -> tg::Result<Analysis> {
		// Parse the text.
		let super::parse::Output {
			program,
			source_map,
		} = Self::parse_module(text)
			.map_err(|source| tg::error!(!source, "failed to parse the module"))?;

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
			return Err(tg::error!("{message}"));
		}

		// Create the output.
		let output = Analysis {
			imports: visitor.imports,
			metadata: visitor.metadata,
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
	errors: Vec<Error>,
	imports: HashSet<tg::Import, fnv::FnvBuildHasher>,
	metadata: Option<BTreeMap<String, tg::value::Data>>,
	source_map: Rc<swc::common::SourceMap>,
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
			let Some(metadata) = self.expr_to_json(init) else {
				continue;
			};
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
			// Handle a dynamic import.
			ast::Callee::Import(_) => {
				let Some(ast::Lit::Str(arg)) = n.args.first().and_then(|arg| arg.expr.as_lit())
				else {
					let loc = self.source_map.lookup_char_pos(n.span.lo);
					self.errors.push(Error::new(
						"the argument to the import function must be a string literal",
						&loc,
					));
					return;
				};
				let with = n
					.args
					.get(1)
					.and_then(|arg| arg.expr.as_object())
					.and_then(|object| {
						object.props.iter().find_map(|prop| {
							let ast::PropOrSpread::Prop(prop) = prop else {
								return None;
							};
							let ast::Prop::KeyValue(prop) = prop.as_ref() else {
								return None;
							};
							match &prop.key {
								ast::PropName::Ident(ident) if ident.sym.as_ref() == "with" => {
									prop.value.as_object()
								},
								ast::PropName::Str(str) if str.value.as_ref() == "with" => {
									prop.value.as_object()
								},
								_ => None,
							}
						})
					});
				self.add_import(&arg.value, with, n.span);
			},

			// Ignore other calls.
			_ => {
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
			let mut map = BTreeMap::new();
			let loc = self.source_map.lookup_char_pos(attributes.span.lo);
			for prop in &attributes.props {
				let Some(prop) = prop.as_prop() else {
					self.errors
						.push(Error::new("spread properties are not allowed", &loc));
					continue;
				};
				let Some(key_value) = prop.as_key_value() else {
					self.errors
						.push(Error::new("only key-value properties are allowed", &loc));
					continue;
				};
				let key = match &key_value.key {
					ast::PropName::Ident(ident) => ident.sym.to_string(),
					ast::PropName::Str(value) => value.value.to_string(),
					_ => {
						self.errors
							.push(Error::new("all keys must be strings", &loc));
						continue;
					},
				};
				let value = if let ast::Expr::Lit(ast::Lit::Str(value)) = key_value.value.as_ref() {
					value.value.to_string()
				} else {
					self.errors
						.push(Error::new("all values must be strings", &loc));
					continue;
				};
				map.insert(key, value);
			}
			Some(map)
		} else {
			None
		};

		// Parse the import.
		let import = match tg::Import::with_specifier_and_attributes(specifier, attributes) {
			Ok(import) => import,
			Err(error) => {
				let loc = self.source_map.lookup_char_pos(span.lo());
				let message = format!("failed to parse the import {specifier:#?}: {error}");
				self.errors.push(Error::new(message, &loc));
				return;
			},
		};

		// Add the import.
		self.imports.insert(import);
	}

	fn expr_to_json(&mut self, expr: &ast::Expr) -> Option<serde_json::Value> {
		let loc = self.source_map.lookup_char_pos(expr.span_lo());
		match expr {
			ast::Expr::Lit(ast::Lit::Null(_)) => Some(serde_json::Value::Null),
			ast::Expr::Lit(ast::Lit::Bool(value)) => Some(serde_json::Value::Bool(value.value)),
			ast::Expr::Lit(ast::Lit::Num(value)) => {
				let Some(value) = serde_json::Number::from_f64(value.value) else {
					self.errors.push(Error::new("invalid number", &loc));
					return None;
				};
				Some(serde_json::Value::Number(value))
			},
			ast::Expr::Lit(ast::Lit::Str(value)) => {
				Some(serde_json::Value::String(value.value.to_string()))
			},
			ast::Expr::Array(ast::ArrayLit { elems, .. }) => {
				let mut array = Vec::new();
				for elem in elems {
					let Some(elem) = elem else {
						self.errors
							.push(Error::new("array holes are not allowed", &loc));
						continue;
					};
					let value = self.expr_to_json(elem.expr.as_ref())?;
					array.push(value);
				}
				Some(serde_json::Value::Array(array))
			},
			ast::Expr::Object(ast::ObjectLit { props, .. }) => {
				let mut output = serde_json::Map::new();
				for prop in props {
					let Some(prop) = prop.as_prop() else {
						self.errors
							.push(Error::new("spread properties are not allowed", &loc));
						continue;
					};
					let Some(key_value) = prop.as_key_value() else {
						self.errors
							.push(Error::new("only key-value properties are allowed", &loc));
						continue;
					};
					let key = match &key_value.key {
						ast::PropName::Ident(ident) => ident.sym.to_string(),
						ast::PropName::Str(value) => value.value.to_string(),
						_ => {
							self.errors.push(Error::new("keys must be strings", &loc));
							continue;
						},
					};
					let value = self.expr_to_json(key_value.value.as_ref())?;
					output.insert(key, value);
				}
				Some(serde_json::Value::Object(output))
			},
			_ => {
				self.errors
					.push(Error::new("values must be valid JSON", &loc));
				None
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_analyze() {
		let text = r#"
			export let metadata = { description: "Hello, World!" };
			import defaultImport from "tg:default_import";
			import { namedImport } from "./named_import.tg.js";
			import * as namespaceImport from "tg:namespace_import";
			let dynamicImport = import("./dynamic_import.tg.ts");
			let include = import("./include.txt");
			export let nested = tg.target(() => {
				let nestedDynamicImport = import("tg:nested_dynamic_import");
				let nestedInclude = import("./nested_include.txt");
			});
			export { namedExport } from "tg:named_export";
			export * as namespaceExport from "./namespace_export.ts";
		"#;
		let left = Compiler::analyze_module(text.to_owned()).unwrap();
		let metadata = Some(
			[(
				"description".to_owned(),
				tg::value::Data::String("Hello, World!".to_owned()),
			)]
			.into(),
		);
		let imports = [
			"tg:default_import",
			"./named_import.tg.js",
			"tg:namespace_import",
			"./dynamic_import.tg.ts",
			"tg:nested_dynamic_import",
			"tg:named_export",
			"./namespace_export.ts",
			"./include.txt",
			"./nested_include.txt",
		]
		.into_iter()
		.map(|specifier| tg::Import::with_specifier_and_attributes(specifier, None).unwrap())
		.collect();
		let right = Analysis { imports, metadata };
		assert_eq!(left, right);
	}
}
