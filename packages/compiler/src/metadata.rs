use {
	oxc::ast::ast::{
		ArrayExpressionElement, Declaration, Expression, ObjectPropertyKind, PropertyKey, Statement,
	},
	tangram_client::prelude::*,
};

pub fn metadata(text: &str) -> tg::Result<Option<serde_json::Map<String, serde_json::Value>>> {
	let allocator = oxc::allocator::Allocator::default();
	let source_type = oxc::span::SourceType::ts();
	let output = oxc::parser::Parser::new(&allocator, text, source_type).parse();

	if !output.errors.is_empty() {
		let messages: Vec<_> = output.errors.iter().map(ToString::to_string).collect();
		return Err(tg::error!(
			"failed to parse module: {}",
			messages.join(", ")
		));
	}

	for statement in &output.program.body {
		let Statement::ExportNamedDeclaration(export) = statement else {
			continue;
		};

		let Some(Declaration::VariableDeclaration(var_decl)) = &export.declaration else {
			continue;
		};

		for declarator in &var_decl.declarations {
			let oxc::ast::ast::BindingPattern::BindingIdentifier(ident) = &declarator.id else {
				continue;
			};

			if ident.name.as_str() != "metadata" {
				continue;
			}

			let Some(init) = &declarator.init else {
				return Err(tg::error!("metadata export has no initializer"));
			};

			let value = expression_to_json(init)?;

			let serde_json::Value::Object(map) = value else {
				return Err(tg::error!("expected metadata to be an object"));
			};

			return Ok(Some(map));
		}
	}

	Ok(None)
}

fn expression_to_json(expr: &Expression) -> tg::Result<serde_json::Value> {
	match expr {
		Expression::StringLiteral(lit) => Ok(serde_json::Value::String(lit.value.to_string())),

		Expression::NumericLiteral(lit) => {
			let number = serde_json::Number::from_f64(lit.value)
				.ok_or_else(|| tg::error!("invalid numeric literal"))?;
			Ok(serde_json::Value::Number(number))
		},

		Expression::BooleanLiteral(lit) => Ok(serde_json::Value::Bool(lit.value)),

		Expression::NullLiteral(_) => Ok(serde_json::Value::Null),

		Expression::ArrayExpression(arr) => {
			let mut values = Vec::new();
			for element in &arr.elements {
				match element {
					ArrayExpressionElement::SpreadElement(_) => {
						return Err(tg::error!(
							"spread elements are not supported in metadata arrays"
						));
					},
					ArrayExpressionElement::Elision(_) => {
						values.push(serde_json::Value::Null);
					},
					_ => {
						let expr = element.to_expression();
						values.push(expression_to_json(expr)?);
					},
				}
			}
			Ok(serde_json::Value::Array(values))
		},

		Expression::ObjectExpression(obj) => {
			let mut map = serde_json::Map::new();
			for property in &obj.properties {
				match property {
					ObjectPropertyKind::SpreadProperty(_) => {
						return Err(tg::error!(
							"spread properties are not supported in metadata objects"
						));
					},
					ObjectPropertyKind::ObjectProperty(prop) => {
						// Get the key.
						let key = match &prop.key {
							PropertyKey::StaticIdentifier(ident) => ident.name.to_string(),
							PropertyKey::StringLiteral(lit) => lit.value.to_string(),
							PropertyKey::NumericLiteral(lit) => lit.value.to_string(),
							_ => {
								return Err(tg::error!(
									"computed property keys are not supported in metadata"
								));
							},
						};

						// Get the value.
						let value = expression_to_json(&prop.value)?;
						map.insert(key, value);
					},
				}
			}
			Ok(serde_json::Value::Object(map))
		},

		Expression::UnaryExpression(unary) => {
			if unary.operator == oxc::ast::ast::UnaryOperator::UnaryNegation
				&& let Expression::NumericLiteral(lit) = &unary.argument
			{
				let number = serde_json::Number::from_f64(-lit.value)
					.ok_or_else(|| tg::error!("invalid numeric literal"))?;
				return Ok(serde_json::Value::Number(number));
			}
			Err(tg::error!(
				"only negative number unary expressions are supported in metadata"
			))
		},

		Expression::TemplateLiteral(template) => {
			if !template.expressions.is_empty() {
				return Err(tg::error!(
					"template literals with expressions are not supported in metadata"
				));
			}
			let value = template
				.quasis
				.first()
				.map(|quasi| quasi.value.raw.to_string())
				.unwrap_or_default();
			Ok(serde_json::Value::String(value))
		},

		Expression::ParenthesizedExpression(paren) => expression_to_json(&paren.expression),

		Expression::TSAsExpression(as_expr) => expression_to_json(&as_expr.expression),

		Expression::TSSatisfiesExpression(satisfies) => expression_to_json(&satisfies.expression),

		_ => Err(tg::error!(
			"only literal values are supported in metadata exports"
		)),
	}
}

#[cfg(test)]
mod tests {
	use {super::*, indoc::indoc};

	#[test]
	fn test_simple_metadata() {
		let text = indoc!(
			r#"
				export let metadata = {
					tag: "foo/bar",
					version: "1.0.0",
				};
			"#
		);
		let result = metadata(text).unwrap().unwrap();
		assert_eq!(result.get("tag").unwrap(), "foo/bar");
		assert_eq!(result.get("version").unwrap(), "1.0.0");
	}

	#[test]
	#[allow(clippy::float_cmp)]
	fn test_metadata_with_numbers() {
		let text = indoc!(
			r#"
				export let metadata = {
					tag: "test",
					count: 42,
					negative: -10,
				};
			"#
		);
		let result = metadata(text).unwrap().unwrap();
		assert_eq!(result.get("tag").unwrap(), "test");
		assert_eq!(result.get("count").unwrap().as_f64().unwrap(), 42.0);
		assert_eq!(result.get("negative").unwrap().as_f64().unwrap(), -10.0);
	}

	#[test]
	fn test_metadata_with_array() {
		let text = indoc!(
			r#"
				export let metadata = {
					tag: "test",
					items: ["a", "b", "c"],
				};
			"#
		);
		let result = metadata(text).unwrap().unwrap();
		let items = result.get("items").unwrap().as_array().unwrap();
		assert_eq!(items.len(), 3);
		assert_eq!(items[0], "a");
	}

	#[test]
	fn test_metadata_with_nested_object() {
		let text = indoc!(
			r#"
				export let metadata = {
					tag: "test",
					nested: {
						foo: "bar",
					},
				};
			"#
		);
		let result = metadata(text).unwrap().unwrap();
		let nested = result.get("nested").unwrap().as_object().unwrap();
		assert_eq!(nested.get("foo").unwrap(), "bar");
	}

	#[test]
	fn test_metadata_with_booleans_and_null() {
		let text = indoc!(
			r#"
				export let metadata = {
					tag: "test",
					enabled: true,
					disabled: false,
					nothing: null,
				};
			"#
		);
		let result = metadata(text).unwrap().unwrap();
		assert_eq!(result.get("enabled").unwrap(), true);
		assert_eq!(result.get("disabled").unwrap(), false);
		assert!(result.get("nothing").unwrap().is_null());
	}

	#[test]
	fn test_no_metadata() {
		let text = indoc!(
			r#"
				export let other = { foo: "bar" };
			"#
		);
		let result = metadata(text).unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_metadata_with_type_assertion() {
		let text = indoc!(
			r#"
				export let metadata = {
					tag: "test",
				} as const;
			"#
		);
		let result = metadata(text).unwrap().unwrap();
		assert_eq!(result.get("tag").unwrap(), "test");
	}

	#[test]
	fn test_metadata_const_declaration() {
		let text = indoc!(
			r#"
				export const metadata = {
					tag: "test/package",
				};
			"#
		);
		let result = metadata(text).unwrap().unwrap();
		assert_eq!(result.get("tag").unwrap(), "test/package");
	}

	#[test]
	fn test_metadata_function_call_error() {
		let text = indoc!(
			r"
				export let metadata = getMetadata();
			"
		);
		let result = metadata(text);
		assert!(result.is_err());
	}

	#[test]
	fn test_metadata_identifier_error() {
		let text = indoc!(
			r#"
				let tag = "foo";
				export let metadata = { tag };
			"#
		);
		let result = metadata(text);
		assert!(result.is_err());
	}

	#[test]
	fn test_metadata_not_object_error() {
		let text = indoc!(
			r#"
				export let metadata = "just a string";
			"#
		);
		let result = metadata(text);
		assert!(result.is_err());
	}

	#[test]
	fn test_template_literal() {
		let text = indoc!(
			r"
				export let metadata = {
					tag: `simple`,
				};
			"
		);
		let result = metadata(text).unwrap().unwrap();
		assert_eq!(result.get("tag").unwrap(), "simple");
	}
}
