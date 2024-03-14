use super::Server;
use std::rc::Rc;
use swc::ecma::{ast, visit::VisitMutWith};
use swc_core as swc;
use tangram_error::{error, Result};

#[derive(Debug)]
pub struct Output {
	pub transpiled_text: String,
	pub source_map: String,
}

pub struct Error {
	message: String,
	line: usize,
	column: usize,
}

impl Server {
	pub fn transpile_module(text: String) -> Result<Output> {
		let globals = swc::common::Globals::default();
		swc::common::GLOBALS.set(&globals, move || {
			// Parse the text.
			let super::parse::Output {
				mut program,
				source_map,
			} = Self::parse_module(text)?;

			let unresolved_mark = swc::common::Mark::new();
			let top_level_mark = swc::common::Mark::new();

			// Create the resolver.
			let mut resolver =
				swc::ecma::transforms::base::resolver(unresolved_mark, top_level_mark, true);

			// Create the target visitor.
			let mut target_visitor = TargetVisitor {
				source_map: source_map.clone(),
				errors: Vec::new(),
			};

			// Create the include visitor.
			let mut include_visitor = IncludeVisitor {
				source_map: source_map.clone(),
				errors: Vec::new(),
			};

			// Create the stripper.
			let mut stripper = swc::ecma::transforms::typescript::strip(top_level_mark);

			// Create the fixer.
			let mut fixer = swc::ecma::transforms::base::fixer::fixer(None);

			// Visit the module.
			program.visit_mut_with(&mut resolver);
			program.visit_mut_with(&mut target_visitor);
			program.visit_mut_with(&mut include_visitor);
			program.visit_mut_with(&mut stripper);
			program.visit_mut_with(&mut fixer);

			// Create the writer.
			let mut transpiled_text = Vec::new();
			let mut source_mappings = Vec::new();
			let mut writer = swc::ecma::codegen::text_writer::JsWriter::new(
				source_map.clone(),
				"\n",
				&mut transpiled_text,
				Some(&mut source_mappings),
			);
			writer.set_indent_str("\t");

			// Create the config.
			let config = swc::ecma::codegen::Config::default();

			// Create the emitter.
			let mut emitter = swc::ecma::codegen::Emitter {
				cfg: config,
				comments: None,
				cm: source_map.clone(),
				wr: writer,
			};

			// Emit the module.
			emitter
				.emit_program(&program)
				.map_err(|error| error!(source = error, "failed to emit the program"))?;
			let transpiled_text = String::from_utf8(transpiled_text)
				.map_err(|error| error!(source = error, "failed to convert bytes to string"))?;

			// Create the source map.
			let mut output_source_map = Vec::new();
			source_map
				.build_source_map(&source_mappings)
				.to_writer(&mut output_source_map)
				.map_err(|error| error!(source = error, "failed to create the source map"))?;
			let source_map = String::from_utf8(output_source_map)
				.map_err(|error| error!(source = error, "failed to convert bytes to string"))?;

			// Create the output.
			let output = Output {
				transpiled_text,
				source_map,
			};

			Ok(output)
		})
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

struct TargetVisitor {
	source_map: Rc<swc::common::SourceMap>,
	errors: Vec<Error>,
}

impl swc::ecma::visit::VisitMut for TargetVisitor {
	fn visit_mut_expr(&mut self, n: &mut ast::Expr) {
		// Check that this is a call expression.
		let Some(expr) = n.as_mut_call() else {
			n.visit_mut_children_with(self);
			return;
		};

		// Visit the call.
		self.visit_call(expr, None);

		n.visit_mut_children_with(self);
	}

	fn visit_mut_export_default_expr(&mut self, n: &mut ast::ExportDefaultExpr) {
		// Check that this is a call expression.
		let Some(expr) = n.expr.as_mut_call() else {
			n.visit_mut_children_with(self);
			return;
		};

		// Visit the call.
		self.visit_call(expr, Some("default".to_owned()));

		n.visit_mut_children_with(self);
	}

	fn visit_mut_export_decl(&mut self, n: &mut ast::ExportDecl) {
		// Check that this export statement has a declaration.
		let Some(decl) = n.decl.as_mut_var() else {
			n.visit_mut_children_with(self);
			return;
		};

		// Visit each declaration.
		for decl in &mut decl.decls {
			let ast::VarDeclarator { name, init, .. } = decl;
			let Some(ident) = name.as_ident().map(|ident| &ident.sym) else {
				continue;
			};
			let Some(init) = init.as_deref_mut() else {
				continue;
			};
			let Some(expr) = init.as_mut_call() else {
				continue;
			};

			// Visit the call.
			self.visit_call(expr, Some(ident.to_string()));
		}

		n.visit_mut_children_with(self);
	}
}

impl TargetVisitor {
	fn visit_call(&mut self, n: &mut ast::CallExpr, export_name: Option<String>) {
		// Check if this is a call to tg.target.
		let Some(callee) = n.callee.as_expr().and_then(|expr| expr.as_member()) else {
			n.visit_mut_children_with(self);
			return;
		};
		let Some(obj) = callee.obj.as_ident() else {
			n.visit_mut_children_with(self);
			return;
		};
		let Some(prop) = callee.prop.as_ident() else {
			n.visit_mut_children_with(self);
			return;
		};
		if !(&obj.sym == "tg" && &prop.sym == "target") {
			n.visit_mut_children_with(self);
			return;
		}

		// Get the location of the call.
		let loc = self.source_map.lookup_char_pos(n.span.lo);

		// Get the name and function from the call.
		let (name, f) = match n.args.len() {
			// Handle one argument.
			1 => {
				let Some(name) = export_name else {
					self.errors.push(Error::new(
						"targets that are not exported must have a name",
						&loc,
					));
					n.visit_mut_children_with(self);
					return;
				};
				let Some(f) = n.args[0].expr.as_arrow() else {
					self.errors.push(Error::new(
						"the argument to tg.target must be an arrow function",
						&loc,
					));
					n.visit_mut_children_with(self);
					return;
				};
				(name, f)
			},

			// Handle two arguments.
			2 => {
				let Some(ast::Lit::Str(name)) = n.args[0].expr.as_lit() else {
					self.errors.push(Error::new(
						"the first argument to tg.target must be a string",
						&loc,
					));
					n.visit_mut_children_with(self);
					return;
				};
				let name = name.value.to_string();
				let Some(f) = n.args[1].expr.as_arrow() else {
					self.errors.push(Error::new(
						"the second argument to tg.target must be an arrow function",
						&loc,
					));
					n.visit_mut_children_with(self);
					return;
				};
				(name, f)
			},

			// Any other number of arguments is invalid.
			_ => {
				self.errors
					.push(Error::new("invalid number of arguments to tg.target", &loc));
				n.visit_mut_children_with(self);
				return;
			},
		};

		// Create the module property.
		let import_meta = ast::Expr::MetaProp(ast::MetaPropExpr {
			span: swc::common::DUMMY_SP,
			kind: swc::ecma::ast::MetaPropKind::ImportMeta,
		});
		let import_meta_url = ast::MemberExpr {
			span: swc::common::DUMMY_SP,
			obj: Box::new(import_meta),
			prop: ast::Ident::new("url".into(), n.span).into(),
		};
		let url_prop = ast::PropOrSpread::Prop(Box::new(ast::Prop::KeyValue(ast::KeyValueProp {
			key: ast::Ident::new("url".into(), n.span).into(),
			value: Box::new(import_meta_url.into()),
		})));

		// Create the name property.
		let key = ast::Ident::new("name".into(), n.span);
		let value: ast::Expr = ast::Lit::Str(ast::Str {
			value: name.into(),
			span: n.span,
			raw: None,
		})
		.into();
		let name_prop = ast::PropOrSpread::Prop(Box::new(ast::Prop::KeyValue(ast::KeyValueProp {
			key: key.into(),
			value: Box::new(value),
		})));

		// Create the function property.
		let function_prop =
			ast::PropOrSpread::Prop(Box::new(ast::Prop::KeyValue(ast::KeyValueProp {
				key: ast::Ident::new("function".into(), n.span).into(),
				value: Box::new(f.clone().into()),
			})));

		// Create the object.
		let object = ast::ObjectLit {
			props: vec![url_prop, name_prop, function_prop],
			span: swc::common::DUMMY_SP,
		};

		// Set the args.
		n.args = vec![ast::ExprOrSpread {
			spread: None,
			expr: object.into(),
		}];
	}
}

struct IncludeVisitor {
	source_map: Rc<swc::common::SourceMap>,
	errors: Vec<Error>,
}

impl swc::ecma::visit::VisitMut for IncludeVisitor {
	fn visit_mut_call_expr(&mut self, n: &mut ast::CallExpr) {
		// Ignore call expression that are not tg.include.
		let Some(callee) = n.callee.as_expr().and_then(|callee| callee.as_member()) else {
			n.visit_mut_children_with(self);
			return;
		};
		let Some(obj) = callee.obj.as_ident() else {
			n.visit_mut_children_with(self);
			return;
		};
		let Some(prop) = callee.prop.as_ident() else {
			n.visit_mut_children_with(self);
			return;
		};
		if !(&obj.sym == "tg" && &prop.sym == "include") {
			n.visit_mut_children_with(self);
			return;
		}

		// Get the location of the call.
		let loc = self.source_map.lookup_char_pos(n.span.lo);

		// Get the argument and verify it is a string literal.
		if n.args.len() != 1 {
			self.errors.push(Error::new(
				"tg.include must be called with exactly one argument",
				&loc,
			));

			return;
		}
		let Some(arg) = n.args[0].expr.as_lit() else {
			self.errors.push(Error::new(
				"the argument to tg.include must be a string literal",
				&loc,
			));
			return;
		};

		// Create the arg.
		let import_meta = ast::Expr::MetaProp(ast::MetaPropExpr {
			span: swc::common::DUMMY_SP,
			kind: ast::MetaPropKind::ImportMeta,
		});
		let import_meta_url = ast::MemberExpr {
			span: swc::common::DUMMY_SP,
			obj: Box::new(import_meta),
			prop: ast::Ident::new("url".into(), n.span).into(),
		};
		let url_prop = ast::PropOrSpread::Prop(Box::new(ast::Prop::KeyValue(ast::KeyValueProp {
			key: ast::Ident::new("url".into(), n.span).into(),
			value: Box::new(import_meta_url.into()),
		})));
		let path_prop = ast::PropOrSpread::Prop(Box::new(ast::Prop::KeyValue(ast::KeyValueProp {
			key: ast::Ident::new("path".into(), n.span).into(),
			value: Box::new(arg.clone().into()),
		})));
		let object = ast::ObjectLit {
			props: vec![url_prop, path_prop],
			span: swc::common::DUMMY_SP,
		};

		// Set the args.
		n.args = vec![ast::ExprOrSpread {
			spread: None,
			expr: object.into(),
		}];
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use indoc::indoc;

	#[test]
	fn test_export_default_target() {
		let text = indoc!(
			"
				export default tg.target(() => {});
			"
		);
		let left = Server::transpile_module(text.to_owned())
			.unwrap()
			.transpiled_text;
		let right = indoc!(
			r#"
				export default tg.target({
					url: import.meta.url,
					name: "default",
					function: ()=>{}
				});
			"#
		);
		assert_eq!(left, right);
	}

	#[test]
	fn test_export_named_target() {
		let text = indoc!(
			"
				export let named = tg.target(() => {});
			"
		);
		let left = Server::transpile_module(text.to_owned())
			.unwrap()
			.transpiled_text;
		let right = indoc!(
			r#"
				export let named = tg.target({
					url: import.meta.url,
					name: "named",
					function: ()=>{}
				});
			"#
		);
		assert_eq!(left, right);
	}

	#[test]
	fn test_named_target() {
		let text = indoc!(
			r#"
				tg.target("named", () => {});
			"#
		);
		let left = Server::transpile_module(text.to_owned())
			.unwrap()
			.transpiled_text;
		let right = indoc!(
			r#"
				tg.target({
					url: import.meta.url,
					name: "named",
					function: ()=>{}
				});
			"#
		);
		assert_eq!(left, right);
	}

	#[test]
	fn test_include() {
		let text = indoc!(
			r#"
				tg.include("./hello_world.txt");
			"#
		);
		let left = Server::transpile_module(text.to_owned())
			.unwrap()
			.transpiled_text;
		let right = indoc!(
			r#"
				tg.include({
					url: import.meta.url,
					path: "./hello_world.txt"
				});
			"#
		);
		assert_eq!(left, right);
	}
}
