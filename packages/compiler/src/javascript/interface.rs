use {
	super::Project,
	std::collections::{BTreeMap, HashMap, HashSet},
	tangram_client::prelude::*,
	tsrs_ast::{Kind, Node, SourceFile, Symbol, SymbolFlags},
	tsrs_checker::{Checker, ElementFlags, LiteralValue, ObjectFlags, TypeFlags},
	tsrs_core::P,
};

/// The maximum depth of nested types to convert.
const DEPTH: usize = 32;

struct Converter<'a> {
	active: Vec<P<tsrs_checker::Type>>,
	checker: &'a mut Checker,
	definitions: BTreeMap<String, tg::interface::Type>,
	depth: usize,
	depth_exceeded: bool,
	exports: HashMap<String, P<Symbol>>,
	failures: HashMap<P<tsrs_checker::Type>, tg::Error>,
	host: &'a super::host::Host,
	imports: BTreeMap<String, tg::module::Data>,
	log: Vec<Entry>,
	names: HashMap<(P<tsrs_checker::Type>, Mode), String>,
	path: String,
	recursive: HashSet<P<tsrs_checker::Type>>,
	reserved: HashSet<String>,
	verified: HashMap<P<tsrs_checker::Type>, Target>,
}

/// A mutation of the converter that a rollback undoes.
enum Entry {
	Definition(String),
	Import(String),
	Name(P<tsrs_checker::Type>, Mode),
	Verified(P<tsrs_checker::Type>),
}

/// The value boundary represented by a converted type.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum Mode {
	/// Values that a parameter accepts without resolving them in the callee.
	Input,

	/// Values produced by deeply resolving an export or a function result.
	Output,

	/// Types that already describe values, including shared named definitions.
	Value,
}

/// A named type that another module exports.
#[derive(Clone)]
struct Target {
	module: tg::module::Data,
	name: String,
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
			active: Vec::new(),
			checker: &mut checker,
			definitions: BTreeMap::new(),
			depth: 0,
			depth_exceeded: false,
			exports: HashMap::new(),
			failures: HashMap::new(),
			host: &self.host,
			imports: BTreeMap::new(),
			log: Vec::new(),
			names: HashMap::new(),
			path: path.clone(),
			recursive: HashSet::new(),
			reserved: HashSet::new(),
			verified: HashMap::new(),
		};

		// Get the module's exports.
		let symbols = converter
			.module_exports(source_file)
			.into_iter()
			.map(|symbol| (symbol.name().to_owned(), symbol))
			.collect::<BTreeMap<_, _>>();

		// Reserve the names of the module's exports, so that a definition of another type cannot take them.
		for (name, symbol) in &symbols {
			let symbol = if symbol.flags().intersects(SymbolFlags::Alias) {
				converter.checker.get_aliased_symbol(*symbol)
			} else {
				*symbol
			};
			converter.exports.insert(name.clone(), symbol);
		}

		// Define the module's exported named types, so that other interfaces can refer to them.
		for (name, symbol) in &symbols {
			converter.export_definition(name, *symbol);
		}

		// Give each export its type, or the reason that it is not a value, discarding the definitions and imports of an export that is not a value.
		for (name, export) in &mut interface.exports {
			let symbol = symbols
				.get(name)
				.copied()
				.ok_or_else(|| tg::error!(%name, "failed to find the export"))?;
			let checkpoint = converter.log.len();
			match converter.export(symbol) {
				Ok(type_) => {
					export.error = None;
					export.type_ = Some(type_);
				},
				Err(error) => {
					converter.rollback(checkpoint);
					converter.depth_exceeded = false;
					export.error = Some(crate::interface::non_value_message(&error));
					export.type_ = None;
				},
			}
		}
		interface.definitions = converter.definitions;
		interface.imports = converter.imports;

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

	/// Define an exported record or recursive type by its export name, leaving the definitions unchanged if the type is not a value.
	fn export_definition(&mut self, name: &str, symbol: P<Symbol>) {
		let symbol = if symbol.flags().intersects(SymbolFlags::Alias) {
			self.checker.get_aliased_symbol(symbol)
		} else {
			symbol
		};
		if !symbol
			.flags()
			.intersects(SymbolFlags::Interface | SymbolFlags::TypeAlias)
			|| self.definitions.contains_key(name)
		{
			return;
		}
		let type_ = self.checker.get_declared_type_of_symbol_exported(symbol);
		let checkpoint = self.log.len();

		// Skip a type that is converted in place, which is any type other than a record that does not refer to itself.
		if !self.is_record(type_) {
			self.type_(type_, Mode::Value).ok();
			self.rollback(checkpoint);
			self.depth_exceeded = false;
			if !self.recursive.contains(&type_) {
				return;
			}
		}

		// Define the type.
		self.insert_name(type_, Mode::Value, name.to_owned());
		self.active.push(type_);
		let definition = self.structure(type_, Mode::Value);
		self.active.pop();
		if let Ok(definition) = definition {
			self.insert_definition(name.to_owned(), definition);
		} else {
			self.rollback(checkpoint);
			self.depth_exceeded = false;
		}
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
			let output = self.type_(type_, Mode::Output)?;
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
			let type_ = self.optional(type_, optional, Mode::Input).map_err(
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
			let item = self.type_(item, Mode::Input).map_err(
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
			self.type_(output, Mode::Output)
				.map_err(|error| tg::error!(!error, "failed to get the type of the output"))?
		};
		let type_ = tg::interface::Type::Command {
			args: Box::new(args),
			output: Box::new(output),
		};

		Ok(type_)
	}

	fn type_(
		&mut self,
		type_: P<tsrs_checker::Type>,
		mode: Mode,
	) -> tg::Result<tg::interface::Type> {
		if self.depth >= DEPTH {
			self.depth_exceeded = true;
			return Err(tg::error!("the type is too deeply nested"));
		}
		self.depth += 1;
		let output = self.type_inner(type_, mode);
		self.depth -= 1;
		output
	}

	fn type_inner(
		&mut self,
		type_: P<tsrs_checker::Type>,
		mode: Mode,
	) -> tg::Result<tg::interface::Type> {
		// Resolve output promises, while requiring inputs and shared definitions to be values.
		if mode == Mode::Output {
			if let Some(awaited) = self.checker.get_awaited_type_exported(type_)
				&& awaited != type_
			{
				return self.type_(awaited, mode);
			}
		} else if self.checker.get_promised_type_of_promise(type_).is_some() {
			return Err(self.error(type_));
		}

		// Convert the primitive types, widening open-ended template literal types to strings.
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
		if flags.intersects(TypeFlags::String | TypeFlags::TemplateLiteral) {
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
			return self.type_(constraint, mode);
		}

		// Convert a type that the Tangram library declares.
		if let Some(output) = self.library(type_)? {
			return Ok(output);
		}

		// Convert a named record or a recursive type to a definition.
		let symbol = named(self.checker, type_);
		if let Some(symbol) = symbol
			&& (self.recursive.contains(&type_) || self.is_record(type_))
		{
			return self.definition(symbol, type_, mode);
		}

		// Record a cycle and return a placeholder that the enclosing conversion discards, unless a definition in the cycle already ends it.
		if let Some(index) = self.active.iter().position(|active| *active == type_)
			&& (symbol.is_some() || !self.is_defined(index, mode))
		{
			if !self.record_cycle(index, type_) {
				let type_ = self.checker.type_to_string_exported(type_);
				return Err(tg::error!(%type_, "a recursive type must have a name"));
			}
			return Ok(tg::interface::Type::Value);
		}

		// Convert the type in place.
		let checkpoint = self.log.len();
		let origin = self.recursive_origin(type_).is_some();
		self.active.push(type_);
		let output = self.structure(type_, mode);
		self.active.pop();

		// Convert a recursive type to a definition instead.
		if let Some(symbol) = symbol
			&& self.recursive.contains(&type_)
		{
			self.rollback(checkpoint);
			self.depth_exceeded = false;
			return self.definition(symbol, type_, mode);
		}

		// Convert a union again if the named union that it flattens turned out to be recursive.
		if !origin && self.recursive_origin(type_).is_some() {
			self.rollback(checkpoint);
			self.depth_exceeded = false;
			return self.type_(type_, mode);
		}

		output
	}

	/// Determine whether a type in the cycle that starts at an index has a definition in progress, which ends the cycle with a reference.
	fn is_defined(&self, index: usize, mode: Mode) -> bool {
		self.active[index..]
			.iter()
			.any(|active| self.names.contains_key(&(*active, mode)))
	}

	/// Record a cycle at its named types, so that its other types stay in place, returning whether the cycle has a named type.
	fn record_cycle(&mut self, index: usize, type_: P<tsrs_checker::Type>) -> bool {
		let mut recorded = false;
		for active in self.active[index..].iter().copied() {
			if named(self.checker, active).is_some() {
				self.recursive.insert(active);
				recorded = true;
			}
			if let Some(origin) = active.union_origin()
				&& origin.flags.get().intersects(TypeFlags::Union)
			{
				for member in origin.types() {
					if member.flags.get().intersects(TypeFlags::Union)
						&& named(self.checker, *member).is_some()
					{
						self.recursive.insert(*member);
						recorded = true;
					}
				}
			}
		}
		if recorded {
			return true;
		}

		// Record the named union that flattened the type out of its alias.
		for active in self.active[..index].iter().rev().copied() {
			if active.flags.get().intersects(TypeFlags::Union)
				&& active.types().contains(&type_)
				&& named(self.checker, active).is_some()
				&& self.recursive.insert(active)
			{
				return true;
			}
		}

		false
	}

	/// Get the members of a union as they were written, if the union flattens a recursive named union.
	fn recursive_origin(&self, type_: P<tsrs_checker::Type>) -> Option<Vec<P<tsrs_checker::Type>>> {
		let origin = type_.union_origin()?;
		if !origin.flags.get().intersects(TypeFlags::Union) {
			return None;
		}
		let members = origin.types();
		members
			.iter()
			.any(|member| self.recursive.contains(member))
			.then(|| members.to_vec())
	}

	/// Convert the structure of a composite type without reserving its name.
	fn structure(
		&mut self,
		type_: P<tsrs_checker::Type>,
		mode: Mode,
	) -> tg::Result<tg::interface::Type> {
		// Convert a union, merging the literals true and false into a boolean.
		if type_.flags.get().intersects(TypeFlags::Union) {
			let members = self
				.recursive_origin(type_)
				.unwrap_or_else(|| type_.types().to_vec());
			return self.union(members, mode);
		}

		// Convert the object types.
		if !is_object(type_) {
			return Err(self.error(type_));
		}
		if self.checker.is_array_type_exported(type_) {
			let item = self.checker.get_type_arguments_exported(type_)[0];
			let item = self.type_(item, mode)?;
			return Ok(tg::interface::Type::Array {
				item: Box::new(item),
			});
		}
		if tsrs_checker::is_tuple_type_exported(type_) {
			return self.tuple(type_, mode);
		}
		if !self.checker.get_call_signatures(type_).is_empty() {
			return Err(tg::error!(
				type = %self.checker.type_to_string_exported(type_),
				"a function is not a value"
			));
		}

		self.record(type_, mode)
	}

	/// Convert the type of an optional parameter or field, which may be absent but is never undefined.
	fn optional(
		&mut self,
		type_: P<tsrs_checker::Type>,
		optional: bool,
		mode: Mode,
	) -> tg::Result<tg::interface::Type> {
		let flags = type_.flags.get();
		if !optional || !flags.intersects(TypeFlags::Union) {
			return self.type_(type_, mode);
		}

		// Convert the only member that was written, so that a named union keeps its name.
		let defined =
			|member: &&P<tsrs_checker::Type>| !member.flags.get().intersects(TypeFlags::Undefined);
		if let Some(origin) = type_.union_origin()
			&& origin.flags.get().intersects(TypeFlags::Union)
			&& let [member] = origin
				.types()
				.iter()
				.filter(defined)
				.collect::<Vec<_>>()
				.as_slice()
		{
			return self.type_(**member, mode);
		}

		// Convert the members other than undefined.
		let members = self
			.recursive_origin(type_)
			.unwrap_or_else(|| type_.types().to_vec())
			.into_iter()
			.filter(|member| !member.flags.get().intersects(TypeFlags::Undefined))
			.collect::<Vec<_>>();
		if let [member] = members.as_slice() {
			return self.type_(*member, mode);
		}
		self.depth += 1;
		self.active.push(type_);
		let output = self.union(members, mode);
		self.active.pop();
		self.depth -= 1;

		output
	}

	fn union(
		&mut self,
		members: Vec<P<tsrs_checker::Type>>,
		mode: Mode,
	) -> tg::Result<tg::interface::Type> {
		let mut types = Vec::new();
		let mut booleans = 0;
		for member in members {
			// Omit known non-value input alternatives without hiding conversion failures.
			if mode == Mode::Input
				&& (member.flags.get().intersects(
					TypeFlags::BigIntLike
						| TypeFlags::ESSymbolLike
						| TypeFlags::Never | TypeFlags::Undefined
						| TypeFlags::Void,
				) || self.checker.get_promised_type_of_promise(member).is_some()
					|| !self.checker.get_call_signatures(member).is_empty())
			{
				continue;
			}
			if member.flags.get().intersects(TypeFlags::BooleanLiteral) {
				booleans += 1;
			}
			let member = self.type_(member, mode)?;
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
		if types.is_empty() {
			return Err(tg::error!("the union has no value alternatives"));
		}
		if types.len() == 1 {
			return Ok(types.pop().unwrap());
		}
		Ok(tg::interface::Type::Union { types })
	}

	fn tuple(
		&mut self,
		type_: P<tsrs_checker::Type>,
		mode: Mode,
	) -> tg::Result<tg::interface::Type> {
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
			let type_ = self.type_(argument, mode)?;
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
			.symbol()
			.or_else(|| type_.alias().and_then(|alias| alias.symbol()));
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
				let args = match arguments.first().copied() {
					Some(args) => {
						// Resolve a type parameter before converting the argument shape.
						let args = if args.flags.get().intersects(TypeFlags::TypeParameter) {
							self.checker
								.get_base_constraint_of_type_exported(args)
								.unwrap_or(args)
						} else {
							args
						};

						// Convert an array or a tuple in place, so that bindings can print the args without resolving a definition.
						if self.checker.is_array_type_exported(args)
							|| tsrs_checker::is_tuple_type_exported(args)
						{
							self.structure(args, Mode::Value)?
						} else {
							self.type_(args, Mode::Value)?
						}
					},
					None => tg::interface::Type::Array {
						item: Box::new(tg::interface::Type::Value),
					},
				};
				let output = match arguments.get(1) {
					Some(output) => self.type_(*output, Mode::Value)?,
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
			_ => return Ok(None),
		};
		Ok(Some(output))
	}

	/// Convert a named record or a recursive type, sharing a definition only for the same instantiated type and value boundary.
	fn definition(
		&mut self,
		symbol: P<Symbol>,
		type_: P<tsrs_checker::Type>,
		mode: Mode,
	) -> tg::Result<tg::interface::Type> {
		if let Some(name) = self.names.get(&(type_, mode)).cloned() {
			// Record a cycle that reaches a definition in progress, so that the named types in the cycle keep their names.
			if let Some(index) = self.active.iter().position(|active| *active == type_) {
				self.record_cycle(index, type_);
			}
			return Ok(tg::interface::Type::Reference { import: None, name });
		}

		// Convert a definition that is shared between the value boundaries.
		if mode == Mode::Value {
			return self.value_definition(symbol, type_);
		}

		// Reuse an unchanged value definition before creating a boundary-specific definition.
		let checkpoint = self.log.len();
		if let Ok(output) = self.type_(type_, Mode::Value) {
			return Ok(output);
		}
		self.rollback(checkpoint);
		self.depth_exceeded = false;

		// Create the boundary-specific definition.
		let name = self.unique_name(symbol);
		self.insert_name(type_, mode, name.clone());
		self.active.push(type_);
		let definition = self.structure(type_, mode);
		self.active.pop();
		self.insert_definition(name.clone(), definition?);

		Ok(tg::interface::Type::Reference { import: None, name })
	}

	/// Convert a named record or a recursive type to a value definition, or to a reference to the module that declares it, remembering the outcome so that each type is traversed once.
	fn value_definition(
		&mut self,
		symbol: P<Symbol>,
		type_: P<tsrs_checker::Type>,
	) -> tg::Result<tg::interface::Type> {
		// Return the remembered outcome.
		if let Some(error) = self.failures.get(&type_) {
			return Err(error.clone());
		}
		if let Some(target) = self.verified.get(&type_).cloned() {
			return Ok(self.import(target));
		}

		// Convert the definition, remembering a failure unless it depends on the current depth.
		let checkpoint = self.log.len();
		let name = self.unique_name(symbol);
		self.insert_name(type_, Mode::Value, name.clone());
		self.active.push(type_);
		let definition = self.structure(type_, Mode::Value);
		self.active.pop();
		if let Err(error) = &definition
			&& !self.depth_exceeded
		{
			self.rollback(checkpoint);
			self.failures.insert(type_, error.clone());
			return Err(error.clone());
		}

		// Replace a declared type with a reference to the module that exports it, which does not nest the type's fields in this interface.
		let target = if type_ == self.checker.get_declared_type_of_symbol_exported(symbol) {
			self.try_import_target(symbol)
		} else {
			None
		};
		if let Some(target) = target {
			self.depth_exceeded = false;
			self.rollback_retaining_verified(checkpoint);
			self.verified.insert(type_, target.clone());
			self.log.push(Entry::Verified(type_));
			return Ok(self.import(target));
		}

		// Propagate a failure that depends on the current depth.
		let definition = match definition {
			Ok(definition) => definition,
			Err(error) => {
				self.rollback(checkpoint);
				return Err(error);
			},
		};

		// Keep the definition.
		self.insert_definition(name.clone(), definition);

		Ok(tg::interface::Type::Reference { import: None, name })
	}

	/// Get a definition name for a symbol that no other definition or export uses.
	fn unique_name(&self, symbol: P<Symbol>) -> String {
		let base = symbol.name().to_owned();
		let mut name = base.clone();
		let mut suffix = 1;
		while self.definitions.contains_key(&name)
			|| self.reserved.contains(&name)
			|| self
				.exports
				.get(&name)
				.is_some_and(|export| *export != symbol)
		{
			suffix += 1;
			name = format!("{base}{suffix}");
		}
		name
	}

	/// Record a definition so that a rollback can remove it.
	fn insert_definition(&mut self, name: String, definition: tg::interface::Type) {
		self.definitions.insert(name.clone(), definition);
		self.log.push(Entry::Definition(name));
	}

	/// Reserve the name of a definition so that a rollback can release it.
	fn insert_name(&mut self, type_: P<tsrs_checker::Type>, mode: Mode, name: String) {
		self.reserved.insert(name.clone());
		self.names.insert((type_, mode), name);
		self.log.push(Entry::Name(type_, mode));
	}

	/// Undo the mutations since a checkpoint.
	fn rollback(&mut self, checkpoint: usize) {
		while self.log.len() > checkpoint {
			let entry = self.log.pop().unwrap();
			self.undo(entry);
		}
	}

	/// Undo the mutations since a checkpoint while keeping the verified types, which an enclosing rollback can still remove.
	fn rollback_retaining_verified(&mut self, checkpoint: usize) {
		let entries = self.log.split_off(checkpoint);
		for entry in entries.into_iter().rev() {
			if matches!(entry, Entry::Verified(_)) {
				self.log.push(entry);
			} else {
				self.undo(entry);
			}
		}
	}

	fn undo(&mut self, entry: Entry) {
		match entry {
			Entry::Definition(name) => {
				self.definitions.remove(&name);
			},
			Entry::Import(name) => {
				self.imports.remove(&name);
			},
			Entry::Name(type_, mode) => {
				if let Some(name) = self.names.remove(&(type_, mode)) {
					self.reserved.remove(&name);
				}
			},
			Entry::Verified(type_) => {
				self.verified.remove(&type_);
			},
		}
	}

	/// Find the export of another module that declares a named type.
	fn try_import_target(&mut self, symbol: P<Symbol>) -> Option<Target> {
		let declaration = symbol.declarations().first().copied()?;
		let source_file = tsrs_ast::get_source_file_of_node(declaration)?;
		let path = source_file.file_name().to_owned();
		if path == self.path || path.starts_with("/__library__/") {
			return None;
		}
		let name = self
			.module_exports(source_file)
			.into_iter()
			.find_map(|export| {
				let target = if export.flags().intersects(SymbolFlags::Alias) {
					self.checker.get_aliased_symbol(export)
				} else {
					export
				};
				(target == symbol).then(|| export.name().to_owned())
			})?;
		let module = self.host.try_module(&path).ok().flatten()?;
		let target = Target { module, name };
		Some(target)
	}

	/// Refer to a named type that another module exports, importing that module.
	fn import(&mut self, target: Target) -> tg::interface::Type {
		let count = self.imports.len();
		let import = crate::interface::import(&mut self.imports, &target.module);
		if self.imports.len() > count {
			self.log.push(Entry::Import(import.clone()));
		}
		tg::interface::Type::Reference {
			import: Some(import),
			name: target.name,
		}
	}

	/// Convert an object type to a record, or to a map with one value type for its string and optional number indexes.
	fn record(
		&mut self,
		type_: P<tsrs_checker::Type>,
		mode: Mode,
	) -> tg::Result<tg::interface::Type> {
		let properties = self.checker.get_properties_of_type(type_).to_vec();
		let infos = self
			.checker
			.get_index_infos_of_type_exported(type_)
			.to_vec();
		match (properties.is_empty(), infos.as_slice()) {
			(true, [info, rest @ ..])
				if infos
					.iter()
					.any(|info| info.key_type().flags.get().intersects(TypeFlags::String))
					&& infos.iter().all(|info| {
						info.key_type()
							.flags
							.get()
							.intersects(TypeFlags::String | TypeFlags::Number)
					}) =>
			{
				let value = self.type_(info.value_type(), mode)?;
				for other in rest {
					if other.value_type() != info.value_type()
						&& self.type_(other.value_type(), mode)? != value
					{
						return Err(self.error(type_));
					}
				}
				Ok(tg::interface::Type::Map {
					value: Box::new(value),
				})
			},
			(_, []) => {
				let mut fields = BTreeMap::new();
				for property in properties {
					let optional = property.flags().intersects(SymbolFlags::Optional);
					let type_ = self.checker.get_type_of_symbol(property);
					let type_ = self.optional(type_, optional, mode).map_err(
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

	/// Determine whether a type converts to a record or a map, rather than an array, a tuple, or a function.
	fn is_record(&mut self, type_: P<tsrs_checker::Type>) -> bool {
		is_object(type_)
			&& !self.checker.is_array_type_exported(type_)
			&& !tsrs_checker::is_tuple_type_exported(type_)
			&& self.checker.get_call_signatures(type_).is_empty()
	}

	fn error(&mut self, type_: P<tsrs_checker::Type>) -> tg::Error {
		let type_ = self.checker.type_to_string_exported(type_);
		tg::error!(%type_, "the type is not a value")
	}
}

/// Determine whether a type is an object or an intersection of objects.
fn is_object(type_: P<tsrs_checker::Type>) -> bool {
	let flags = type_.flags.get();
	flags.intersects(TypeFlags::Object)
		|| (flags.intersects(TypeFlags::Intersection)
			&& type_.types().iter().all(|type_| is_object(*type_)))
}

/// Get the symbol of a type that a module names with an interface or a type alias, which excludes an instantiation of the array interface.
fn named(checker: &Checker, type_: P<tsrs_checker::Type>) -> Option<P<Symbol>> {
	if let Some(alias) = type_.alias()
		&& let Some(symbol) = alias.symbol()
	{
		return Some(symbol);
	}
	if checker.is_array_type_exported(type_) {
		return None;
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
