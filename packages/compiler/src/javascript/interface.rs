use {
	super::Project,
	std::collections::{BTreeMap, HashMap},
	tangram_client::prelude::*,
	tsrs_ast::{Kind, Node, SourceFile, Symbol, SymbolFlags},
	tsrs_checker::{Checker, ElementFlags, LiteralValue, ObjectFlags, TypeFlags},
	tsrs_core::P,
};

/// The maximum depth of nested types to convert.
const DEPTH: usize = 32;

struct Converter<'a> {
	checker: &'a mut Checker,
	definitions: BTreeMap<String, tg::interface::Type>,
	depth: usize,
	names: HashMap<P<Symbol>, String>,
}

impl Project {
	/// Get the interface of a module, giving each export the type that the checker infers for it.
	pub(super) fn interface(
		&self,
		module: &tg::module::Data,
		text: &str,
	) -> tg::Result<tg::Interface> {
		// Discover the exports from the module's source.
		let mut interface = crate::interface::module(module, text)?;

		// Get the source file and a type checker.
		let context = Self::context();
		let program = self.program()?;
		let path = self.host.path(module);
		let source_file = program.get_source_file(&path).ok_or_else(|| {
			tg::error!("the javascript program did not contain the requested module")
		})?;
		let mut checker = program.get_type_checker(&context);
		let mut converter = Converter {
			checker: &mut checker,
			definitions: BTreeMap::new(),
			depth: 0,
			names: HashMap::new(),
		};

		// Give each export its type.
		let symbols = converter
			.module_exports(source_file)
			.into_iter()
			.map(|symbol| (symbol.name().to_owned(), symbol))
			.collect::<BTreeMap<_, _>>();
		for (name, export) in &mut interface.exports {
			let symbol = symbols
				.get(name)
				.copied()
				.ok_or_else(|| tg::error!(%name, "failed to find the export"))?;
			export.type_ = converter.export(symbol).map_err(
				|error| tg::error!(!error, %name, "failed to get the type of the export"),
			)?;
		}
		interface.definitions = converter.definitions;

		Ok(interface)
	}
}

impl Converter<'_> {
	fn module_exports(&mut self, source_file: P<SourceFile>) -> Vec<P<Symbol>> {
		let Some(symbol) = source_file.symbol() else {
			return Vec::new();
		};
		let symbol = self.checker.get_merged_symbol(symbol);
		self.checker.get_exports_of_module_exported(symbol)
	}

	/// Get the type of the command that runs an export, which calls a function and returns any other value.
	fn export(&mut self, symbol: P<Symbol>) -> tg::Result<tg::interface::Type> {
		let symbol = if symbol.flags().intersects(SymbolFlags::Alias) {
			self.checker.get_aliased_symbol(symbol)
		} else {
			symbol
		};
		let type_ = self.checker.get_type_of_symbol(symbol);

		// Get the type of a command that returns a value.
		let signatures = self.checker.get_call_signatures(type_).to_vec();
		if signatures.is_empty() {
			let output = self.type_(type_)?;
			let args = tg::interface::Type::Tuple {
				elements: Vec::new(),
				rest: None,
			};
			let type_ = tg::interface::Type::Command {
				args: Box::new(args),
				output: Box::new(output),
			};
			return Ok(type_);
		}

		// Get the type of a command that calls a function.
		let [signature] = signatures.as_slice() else {
			return Err(tg::error!("an overloaded function cannot be exported"));
		};
		let signature = *signature;
		let parameters = signature.parameters();
		let rest = self
			.checker
			.has_effective_rest_parameter_exported(signature);
		let count = if rest {
			parameters.len() - 1
		} else {
			parameters.len()
		};
		let minimum = signature.min_argument_count();
		let mut elements = Vec::new();
		for (index, parameter) in parameters.iter().take(count).enumerate() {
			let optional = index >= minimum;
			let type_ = self.checker.get_type_of_symbol(*parameter);
			let type_ = self.optional(type_, optional).map_err(
				|error| tg::error!(!error, name = %parameter.name(), "failed to get the type of the parameter"),
			)?;
			let element = tg::interface::Element { optional, type_ };
			elements.push(element);
		}
		let rest = if rest {
			let parameter = parameters[count];
			let type_ = self.checker.get_type_of_symbol(parameter);
			if !self.checker.is_array_type_exported(type_) {
				return Err(tg::error!(
					name = %parameter.name(),
					"a rest parameter must be an array"
				));
			}
			let item = self.checker.get_type_arguments_exported(type_)[0];
			let item = self.type_(item).map_err(
				|error| tg::error!(!error, name = %parameter.name(), "failed to get the type of the parameter"),
			)?;
			Some(Box::new(item))
		} else {
			None
		};
		let args = tg::interface::Type::Tuple { elements, rest };
		let output = self.checker.get_return_type_of_signature(signature);
		let output = self
			.checker
			.get_awaited_type_exported(output)
			.unwrap_or(output);
		let output = if output
			.flags
			.get()
			.intersects(TypeFlags::Void | TypeFlags::Undefined)
		{
			tg::interface::Type::Null
		} else {
			self.type_(output)
				.map_err(|error| tg::error!(!error, "failed to get the type of the output"))?
		};
		let type_ = tg::interface::Type::Command {
			args: Box::new(args),
			output: Box::new(output),
		};

		Ok(type_)
	}

	fn type_(&mut self, type_: P<tsrs_checker::Type>) -> tg::Result<tg::interface::Type> {
		if self.depth >= DEPTH {
			return Err(tg::error!("the type is too deeply nested"));
		}
		self.depth += 1;
		let output = self.type_inner(type_);
		self.depth -= 1;
		output
	}

	fn type_inner(&mut self, type_: P<tsrs_checker::Type>) -> tg::Result<tg::interface::Type> {
		// Convert the primitive types.
		let flags = type_.flags.get();
		if flags.intersects(TypeFlags::Any | TypeFlags::Unknown) {
			return Ok(tg::interface::Type::Value);
		}
		if flags.intersects(TypeFlags::Null) {
			return Ok(tg::interface::Type::Null);
		}
		if flags.intersects(TypeFlags::Boolean) && !flags.intersects(TypeFlags::Union) {
			return Ok(tg::interface::Type::Boolean);
		}
		if flags.intersects(TypeFlags::Number) {
			return Ok(tg::interface::Type::Number);
		}
		if flags.intersects(TypeFlags::String) {
			return Ok(tg::interface::Type::String);
		}
		if flags.intersects(
			TypeFlags::BooleanLiteral | TypeFlags::NumberLiteral | TypeFlags::StringLiteral,
		) && let Some(value) = type_.as_literal_type().value()
		{
			let value = match value {
				LiteralValue::Boolean(value) => tg::interface::Literal::Boolean(value),
				LiteralValue::Number(value) => tg::interface::Literal::Number(value.0),
				LiteralValue::String(value) => tg::interface::Literal::String(value.to_owned()),
				LiteralValue::BigInt(_) => return Err(self.error(type_)),
			};
			return Ok(tg::interface::Type::Literal { value });
		}

		// Convert a type parameter to its constraint.
		if flags.intersects(TypeFlags::TypeParameter) {
			let Some(constraint) = self.checker.get_base_constraint_of_type_exported(type_) else {
				return Ok(tg::interface::Type::Value);
			};
			if constraint == type_ {
				return Ok(tg::interface::Type::Value);
			}
			return self.type_(constraint);
		}

		// Convert a type that the Tangram library declares.
		if let Some(output) = self.library(type_)? {
			return Ok(output);
		}

		// Convert a union, merging the literals true and false into a boolean.
		if flags.intersects(TypeFlags::Union) {
			let members = type_.types().to_vec();
			return self.union(members);
		}

		// Convert the object types.
		if !flags.intersects(TypeFlags::Object) {
			return Err(self.error(type_));
		}
		if self.checker.is_array_type_exported(type_) {
			let item = self.checker.get_type_arguments_exported(type_)[0];
			let item = self.type_(item)?;
			return Ok(tg::interface::Type::Array {
				item: Box::new(item),
			});
		}
		if tsrs_checker::is_tuple_type_exported(type_) {
			return self.tuple(type_);
		}
		if !self.checker.get_call_signatures(type_).is_empty() {
			return Err(tg::error!(
				type = %self.checker.type_to_string_exported(type_),
				"a function is not a value"
			));
		}
		if let Some(symbol) = named(type_) {
			return self.definition(symbol, type_);
		}
		self.record(type_)
	}

	/// Convert the type of an optional parameter or field, which may be absent but is never undefined.
	fn optional(
		&mut self,
		type_: P<tsrs_checker::Type>,
		optional: bool,
	) -> tg::Result<tg::interface::Type> {
		let flags = type_.flags.get();
		if !optional || !flags.intersects(TypeFlags::Union) {
			return self.type_(type_);
		}
		let members = type_
			.types()
			.iter()
			.copied()
			.filter(|member| !member.flags.get().intersects(TypeFlags::Undefined))
			.collect::<Vec<_>>();
		if let [member] = members.as_slice() {
			return self.type_(*member);
		}
		self.depth += 1;
		let output = self.union(members);
		self.depth -= 1;
		output
	}

	fn union(&mut self, members: Vec<P<tsrs_checker::Type>>) -> tg::Result<tg::interface::Type> {
		let mut types = Vec::new();
		let mut booleans = 0;
		for member in members {
			if member.flags.get().intersects(TypeFlags::BooleanLiteral) {
				booleans += 1;
			}
			let member = self.type_(member)?;
			if !types.contains(&member) {
				types.push(member);
			}
		}
		if booleans == 2 {
			types.retain(|type_| {
				!matches!(
					type_,
					tg::interface::Type::Literal {
						value: tg::interface::Literal::Boolean(_)
					}
				)
			});
			types.push(tg::interface::Type::Boolean);
		}
		if types.contains(&tg::interface::Type::Value) {
			return Ok(tg::interface::Type::Value);
		}
		Ok(tg::interface::Type::Union { types })
	}

	fn tuple(&mut self, type_: P<tsrs_checker::Type>) -> tg::Result<tg::interface::Type> {
		let flags = type_
			.target()
			.filter(|target| target.object_flags().intersects(ObjectFlags::Tuple))
			.map(|target| target.as_tuple_type().element_flags())
			.unwrap_or_default();
		let arguments = self.checker.get_type_arguments_exported(type_).to_vec();
		let mut elements = Vec::new();
		let mut rest = None;
		for (index, argument) in arguments.into_iter().enumerate() {
			let flags = flags.get(index).copied().unwrap_or_default();
			let type_ = self.type_(argument)?;
			if flags.intersects(ElementFlags::Variadic) {
				return Err(self.error(argument));
			}
			if flags.intersects(ElementFlags::Rest) {
				if rest.is_some() {
					return Err(self.error(argument));
				}
				rest = Some(Box::new(type_));
				continue;
			}
			if rest.is_some() {
				return Err(tg::error!("a tuple element cannot follow a rest element"));
			}
			let optional = flags.intersects(ElementFlags::Optional);
			elements.push(tg::interface::Element { optional, type_ });
		}
		Ok(tg::interface::Type::Tuple { elements, rest })
	}

	/// Convert a type that the Tangram library declares, or the byte array type.
	fn library(&mut self, type_: P<tsrs_checker::Type>) -> tg::Result<Option<tg::interface::Type>> {
		let symbol = type_
			.alias()
			.and_then(|alias| alias.symbol())
			.or_else(|| type_.symbol());
		let Some(symbol) = symbol else {
			return Ok(None);
		};
		let declaration = symbol.declarations().first().copied();
		let file_name = declaration
			.and_then(tsrs_ast::get_source_file_of_node)
			.map(|source_file| source_file.file_name().to_owned());
		let Some(path) = file_name
			.as_deref()
			.and_then(|file_name| file_name.strip_prefix("/__library__/"))
		else {
			return Ok(None);
		};
		if path.starts_with("lib.") {
			let output = (symbol.name() == "Uint8Array").then_some(tg::interface::Type::Bytes);
			return Ok(output);
		}
		let name = qualified_name(declaration, symbol);
		let output = match name.as_str() {
			"Artifact" => tg::interface::Type::Artifact,
			"Blob" => tg::interface::Type::Blob,
			"Command" => {
				let arguments = if let Some(alias) = type_.alias()
					&& alias.symbol() == Some(symbol)
				{
					alias.type_arguments().to_vec()
				} else {
					self.checker.get_type_arguments_exported(type_).to_vec()
				};
				let args = match arguments.first() {
					Some(args) => self.type_(*args)?,
					None => tg::interface::Type::Array {
						item: Box::new(tg::interface::Type::Value),
					},
				};
				let output = match arguments.get(1) {
					Some(output) => self.type_(*output)?,
					None => tg::interface::Type::Value,
				};
				tg::interface::Type::Command {
					args: Box::new(args),
					output: Box::new(output),
				}
			},
			"Directory" => tg::interface::Type::Directory,
			"Error" => tg::interface::Type::Error,
			"File" => tg::interface::Type::File,
			"Graph" => tg::interface::Type::Graph,
			"Module" => tg::interface::Type::Module,
			"Mutation" => tg::interface::Type::Mutation,
			"Object" => tg::interface::Type::Object,
			"Placeholder" => tg::interface::Type::Placeholder,
			"Symlink" => tg::interface::Type::Symlink,
			"Template" => tg::interface::Type::Template,
			"Value" => tg::interface::Type::Value,
			_ => return Err(self.error(type_)),
		};
		Ok(Some(output))
	}

	/// Convert a named object type to a reference to a definition, so that a recursive type refers to itself.
	fn definition(
		&mut self,
		symbol: P<Symbol>,
		type_: P<tsrs_checker::Type>,
	) -> tg::Result<tg::interface::Type> {
		if let Some(name) = self.names.get(&symbol) {
			return Ok(tg::interface::Type::Reference { name: name.clone() });
		}
		let mut name = symbol.name().to_owned();
		let mut suffix = 1;
		while self.definitions.contains_key(&name)
			|| self.names.values().any(|other| *other == name)
		{
			suffix += 1;
			name = format!("{}{suffix}", symbol.name());
		}
		self.names.insert(symbol, name.clone());
		let definition = self.record(type_)?;
		self.definitions.insert(name.clone(), definition);
		Ok(tg::interface::Type::Reference { name })
	}

	/// Convert an object type to a record, or to a map when it only has a string index signature.
	fn record(&mut self, type_: P<tsrs_checker::Type>) -> tg::Result<tg::interface::Type> {
		let properties = self.checker.get_properties_of_type(type_).to_vec();
		let infos = self
			.checker
			.get_index_infos_of_type_exported(type_)
			.to_vec();
		match (properties.is_empty(), infos.as_slice()) {
			(true, [info]) if info.key_type().flags.get().intersects(TypeFlags::String) => {
				let value = self.type_(info.value_type())?;
				Ok(tg::interface::Type::Map {
					value: Box::new(value),
				})
			},
			(_, []) => {
				let mut fields = BTreeMap::new();
				for property in properties {
					let optional = property.flags().intersects(SymbolFlags::Optional);
					let type_ = self.checker.get_type_of_symbol(property);
					let type_ = self.optional(type_, optional).map_err(
						|error| tg::error!(!error, name = %property.name(), "failed to get the type of the field"),
					)?;
					let field = tg::interface::Field { optional, type_ };
					fields.insert(property.name().to_owned(), field);
				}
				Ok(tg::interface::Type::Record { fields })
			},
			_ => Err(self.error(type_)),
		}
	}

	fn error(&mut self, type_: P<tsrs_checker::Type>) -> tg::Error {
		let type_ = self.checker.type_to_string_exported(type_);
		tg::error!(%type_, "the type is not a value")
	}
}

/// Get the symbol of an object type that a module names with an interface or a type alias.
fn named(type_: P<tsrs_checker::Type>) -> Option<P<Symbol>> {
	if let Some(alias) = type_.alias()
		&& let Some(symbol) = alias.symbol()
	{
		return Some(symbol);
	}
	let symbol = type_.symbol()?;
	symbol
		.flags()
		.intersects(SymbolFlags::Interface)
		.then_some(symbol)
}

/// Get the name of a declaration in the Tangram library, qualified by the declarations that enclose it.
fn qualified_name(declaration: Option<P<Node>>, symbol: P<Symbol>) -> String {
	let mut names = Vec::new();
	let mut parent = declaration.and_then(|declaration| declaration.parent());
	while let Some(node) = parent {
		if matches!(
			node.kind(),
			Kind::ClassDeclaration | Kind::InterfaceDeclaration | Kind::ModuleDeclaration
		) && let Some(name) = node.name()
			&& matches!(name.kind(), Kind::Identifier)
			&& !tsrs_ast::is_global_scope_augmentation(node)
		{
			names.push(name.text().to_owned());
		}
		parent = node.parent();
	}
	names.reverse();
	names.push(symbol.name().to_owned());
	names.join(".")
}
