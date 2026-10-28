use {
	super::{Project, range},
	crate::document,
	std::collections::{BTreeMap, HashMap, HashSet},
	tangram_client::{
		document::{
			ArrayType, Class, Comment, ConditionalType, Declaration, Enum, EnumMember, Function,
			FunctionType, IndexSignature, IndexedAccessType, Interface, Literal, Location,
			MappedType, Members, Namespace, OperatorType, Output, Parameter, PredicateType,
			Property, QueryType, ReferenceType, Signature, Tag, TemplateLiteralType,
			TemplateLiteralTypeSpan, TupleElement, TupleType, Type, TypeAlias, TypeParameter,
			Types, Variable,
		},
		prelude::*,
	},
	tsrs_ast::{Kind, ModifierFlags, Node, NodeFlags, SourceFile, Symbol, SymbolFlags},
	tsrs_checker::{
		Checker, ElementFlags, Flags, LiteralValue, ObjectFlags, SignatureKind, TypeFlags,
		TypePredicateKind,
	},
	tsrs_core::P,
	tsrs_ls::lsconv::Converters,
};

/// The maximum depth of nested types to expand.
const DEPTH: usize = 16;

/// The maximum number of members of an inferred object type to expand.
const MEMBERS: usize = 16;

/// The tags that describe types rather than documentation.
const TYPE_TAGS: [&str; 8] = [
	"callback",
	"import",
	"overload",
	"satisfies",
	"template",
	"this",
	"type",
	"typedef",
];

struct Converter<'a> {
	checker: &'a mut Checker,
	converted: HashSet<P<Symbol>>,
	converters: &'a Converters,
	declarations: Vec<Option<Declaration>>,
	module_ids: HashMap<String, usize>,
	modules: Vec<tg::module::Data>,
	project: &'a Project,
	slot_ids: HashMap<P<Symbol>, usize>,
	slots: Vec<Vec<usize>>,
	types: Vec<P<tsrs_checker::Type>>,
}

/// The documentation in a doc comment.
#[derive(Default)]
struct Documentation {
	comment: Option<Comment>,
	hidden: bool,
	parameters: BTreeMap<String, String>,
	returns: Option<String>,
}

impl Project {
	pub(super) fn document(&self, arg: &document::Arg) -> tg::Result<document::Output> {
		// Get the source file and a type checker.
		let context = Self::context();
		let program = self.program()?;
		let path = self.host.path(&arg.module);
		let source_file = program.get_source_file(&path).ok_or_else(|| {
			tg::error!("the tsrs program did not contain the requested module")
		})?;
		let mut checker = program.get_type_checker(&context);
		let converters = self.snapshot.borrow().0.converters();
		let mut converter = Converter {
			checker: &mut checker,
			converted: HashSet::new(),
			converters: &converters,
			declarations: Vec::new(),
			module_ids: HashMap::new(),
			modules: Vec::new(),
			project: self,
			slot_ids: HashMap::new(),
			slots: Vec::new(),
			types: Vec::new(),
		};

		// Convert the module's exports. Each list of indices holds a slot until the slots are resolved.
		let mut exports = BTreeMap::new();
		for export in converter.module_exports(source_file) {
			let slot = converter.symbol(export);
			exports.insert(export.name().to_owned(), vec![slot]);
		}

		// Create the output.
		let declarations = converter
			.declarations
			.into_iter()
			.collect::<Option<Vec<_>>>()
			.ok_or_else(|| tg::error!("failed to convert a declaration"))?;
		let mut output = Output {
			declarations,
			exports,
			modules: converter.modules,
		};

		// Replace the slots with the indices of the declarations.
		crate::document::resolve(&mut output, &converter.slots);

		Ok(output)
	}
}

impl Converter<'_> {
	fn module_exports(&mut self, source_file: P<SourceFile>) -> Vec<P<Symbol>> {
		// Get the exports of the module symbol.
		if let Some(symbol) = source_file.symbol() {
			let symbol = self.checker.get_merged_symbol(symbol);
			return self.checker.get_exports_of_module_exported(symbol);
		}

		// Otherwise, get the module members declared in the script.
		self.checker
			.get_symbols_in_scope_exported(source_file.as_node(), SymbolFlags::ModuleMember)
			.into_iter()
			.filter(|symbol| {
				symbol.declarations().iter().any(|declaration| {
					tsrs_ast::get_source_file_of_node(*declaration) == Some(source_file)
				})
			})
			.collect()
	}

	fn resolve(&mut self, symbol: P<Symbol>) -> P<Symbol> {
		if symbol.flags().intersects(SymbolFlags::Alias) {
			self.checker.get_aliased_symbol(symbol)
		} else {
			symbol
		}
	}

	fn slot(&mut self, symbol: P<Symbol>) -> usize {
		let symbol = self.resolve(symbol);
		if let Some(slot) = self.slot_ids.get(&symbol) {
			return *slot;
		}
		let slot = self.slots.len();
		self.slots.push(Vec::new());
		self.slot_ids.insert(symbol, slot);
		slot
	}

	fn push(&mut self, slot: usize, declaration: Declaration) {
		let id = self.declarations.len();
		self.declarations.push(Some(declaration));
		self.slots[slot].push(id);
	}

	fn symbol(&mut self, symbol: P<Symbol>) -> usize {
		// Convert each symbol once, through the target of an alias.
		let slot = self.slot(symbol);
		let symbol = self.resolve(symbol);
		if symbol.flags().intersects(SymbolFlags::Alias) || !self.converted.insert(symbol) {
			return slot;
		}

		// Skip a symbol whose declarations are all hidden.
		let declarations = symbol.declarations();
		if !declarations.is_empty()
			&& declarations
				.iter()
				.all(|declaration| documentation(*declaration).hidden)
		{
			return slot;
		}
		let flags = symbol.flags();
		// Name a declaration from a module generated for another language by its export.
		let name = symbol
			.declarations()
			.first()
			.and_then(|declaration| self.source(*declaration))
			.map_or_else(|| symbol.name().to_owned(), |source| source.name);

		// Convert a namespace.
		if flags.intersects(SymbolFlags::NamespaceModule | SymbolFlags::ValueModule) {
			self.namespace(symbol, slot);
		}

		// Convert a class.
		if flags.intersects(SymbolFlags::Class)
			&& let Some(class) = self.class(symbol)
		{
			self.push(slot, Declaration::Class(class));
		}

		// Convert a variable.
		if flags.intersects(SymbolFlags::Variable) {
			let declaration = symbol
				.declarations()
				.iter()
				.copied()
				.find(|declaration| declaration.kind() == Kind::VariableDeclaration)
				.or_else(|| symbol.value_declaration());
			if let Some(declaration) = declaration {
				let type_ = if let Some(node) = declaration.type_node() {
					self.type_node(node)
				} else {
					let type_ = self.checker.get_type_of_symbol(symbol);
					self.type_(type_)
				};
				let constant = declaration.kind() == Kind::VariableDeclaration
					&& tsrs_ast::is_var_const(declaration);
				self.value(slot, &name, declaration, type_, constant);
			}
		}

		// Convert a function.
		if flags.intersects(SymbolFlags::Function) {
			let nodes = symbol
				.declarations()
				.iter()
				.copied()
				.filter(|declaration| declaration.kind() == Kind::FunctionDeclaration)
				.collect::<Vec<_>>();
			let overloaded = nodes.len() > 1;
			for node in nodes {
				if (overloaded && node.body().is_some()) || documentation(node).hidden {
					continue;
				}
				let mut signature = self.signature_declaration(node);
				let function = Function {
					comment: signature.comment.take(),
					location: signature.location.take(),
					name: name.clone(),
					signature,
				};
				self.push(slot, Declaration::Function(function));
			}
		}

		// Convert a type alias.
		if flags.intersects(SymbolFlags::TypeAlias)
			&& let Some(alias) = self.type_alias(symbol)
		{
			self.push(slot, Declaration::TypeAlias(alias));
		}

		// Convert an enum.
		if flags.intersects(SymbolFlags::Enum) {
			for node in symbol.declarations() {
				if node.kind() == Kind::EnumDeclaration {
					let enum_ = self.enum_(*node, &name);
					self.push(slot, Declaration::Enum(enum_));
				}
			}
		}

		// Convert an interface.
		if flags.intersects(SymbolFlags::Interface) && !flags.intersects(SymbolFlags::Class) {
			for node in symbol.declarations() {
				if node.kind() == Kind::InterfaceDeclaration {
					let interface = self.interface(*node, &name);
					self.push(slot, Declaration::Interface(interface));
				}
			}
		}

		// Convert a default export of an expression.
		if flags.intersects(SymbolFlags::Property)
			&& let Some(declaration) = symbol.value_declaration()
			&& declaration.kind() == Kind::ExportAssignment
		{
			let type_ = self.checker.get_type_of_symbol(symbol);
			let type_ = self.type_(type_);
			self.value(slot, &name, declaration, type_, true);
		}

		slot
	}

	fn value(
		&mut self,
		slot: usize,
		name: &str,
		declaration: P<Node>,
		type_: Type,
		constant: bool,
	) {
		// Document a value with a function type as a function.
		let comment = documentation(declaration).comment;
		let location = self.location(declaration);
		match type_ {
			Type::Function(function) if !function.signatures.is_empty() => {
				for mut signature in function.signatures {
					let function = Function {
						comment: signature.comment.take().or_else(|| comment.clone()),
						location: location.clone(),
						name: name.to_owned(),
						signature,
					};
					self.push(slot, Declaration::Function(function));
				}
			},
			type_ => {
				let variable = Variable {
					comment,
					constant,
					location,
					name: name.to_owned(),
					type_,
				};
				self.push(slot, Declaration::Variable(variable));
			},
		}
	}

	fn namespace(&mut self, symbol: P<Symbol>, slot: usize) {
		// Find the declaration.
		let declaration = symbol.declarations().iter().copied().find(|declaration| {
			matches!(
				declaration.kind(),
				Kind::ModuleDeclaration | Kind::SourceFile
			)
		});
		let Some(declaration) = declaration else {
			return;
		};

		// Reserve the namespace's index, so that the exports may refer to it.
		let id = self.declarations.len();
		self.declarations.push(None);
		self.slots[slot].push(id);

		// Convert the exports.
		let mut exports = BTreeMap::new();
		for export in self.checker.get_exports_of_module_exported(symbol) {
			if !export.flags().intersects(SymbolFlags::ModuleMember) {
				continue;
			}
			let slot = self.symbol(export);
			exports.insert(export.name().to_owned(), vec![slot]);
		}

		// Create the namespace.
		let namespace = Namespace {
			comment: documentation(declaration).comment,
			exports,
			location: self.location(declaration),
			name: symbol.name().to_owned(),
		};
		self.declarations[id] = Some(Declaration::Namespace(namespace));
	}

	fn class(&mut self, symbol: P<Symbol>) -> Option<Class> {
		// Find the declaration.
		let declaration = symbol.declarations().iter().copied().find(|declaration| {
			matches!(
				declaration.kind(),
				Kind::ClassDeclaration | Kind::ClassExpression
			)
		})?;

		// Get the instance properties declared by this class.
		let mut properties = Vec::new();
		let instance = self.checker.get_declared_type_of_symbol_exported(symbol);
		for property in self.checker.get_properties_of_type(instance).to_vec() {
			if !is_member(property, declaration)
				|| !property.flags().intersects(SymbolFlags::ClassMember)
			{
				continue;
			}
			if let Some(property) = self.class_property(property, false) {
				properties.push(property);
			}
		}

		// Get the static properties.
		let static_ = self
			.checker
			.get_type_of_symbol_at_location(symbol, Some(declaration))
			.unwrap_or_else(|| self.checker.get_type_of_symbol(symbol));
		for property in self.checker.get_properties_of_type(static_).to_vec() {
			let flags = property.flags();
			if flags.intersects(SymbolFlags::ModuleMember | SymbolFlags::Prototype)
				|| !flags.intersects(SymbolFlags::ClassMember)
				|| !is_member(property, declaration)
			{
				continue;
			}
			if let Some(property) = self.class_property(property, true) {
				properties.push(property);
			}
		}

		// Get the explicitly declared constructors.
		let signatures = self
			.checker
			.get_signatures_of_type_exported(static_, SignatureKind::Construct)
			.to_vec();
		let nodes = signatures
			.iter()
			.filter_map(|signature| signature.declaration())
			.filter(|node| node.kind() == Kind::Constructor && node.parent() == Some(declaration))
			.collect::<Vec<_>>();
		let overloaded = nodes.len() > 1;
		let constructors = nodes
			.into_iter()
			.filter(|node| (!overloaded || node.body().is_none()) && !documentation(*node).hidden)
			.map(|node| self.signature_declaration(node))
			.collect();

		// Get the heritage.
		let extends = tsrs_ast::get_class_extends_heritage_element(declaration)
			.map(|node| self.type_node(node))
			.into_iter()
			.collect();
		let implements = tsrs_ast::get_implements_heritage_clause_elements(declaration)
			.iter()
			.map(|node| self.type_node(*node))
			.collect();

		// Get the type parameters.
		let type_parameters = self.type_parameter_nodes(declaration.type_parameters());

		let class = Class {
			abstract_: tsrs_ast::get_combined_modifier_flags(declaration)
				.intersects(ModifierFlags::Abstract),
			comment: documentation(declaration).comment,
			constructors,
			extends,
			implements,
			location: self.location(declaration),
			name: symbol.name().to_owned(),
			properties,
			type_parameters,
		};

		Some(class)
	}

	fn class_property(&mut self, symbol: P<Symbol>, static_: bool) -> Option<Property> {
		// Skip private and hidden members.
		let declaration = symbol
			.value_declaration()
			.or_else(|| symbol.declarations().first().copied())?;
		let flags = tsrs_ast::get_combined_modifier_flags(declaration);
		let private = declaration
			.name()
			.is_some_and(|name| name.kind() == Kind::PrivateIdentifier);
		let documentation = documentation(declaration);
		if private || flags.intersects(ModifierFlags::Private) || documentation.hidden {
			return None;
		}

		// Convert the property.
		let readonly = flags.intersects(ModifierFlags::Readonly)
			|| (symbol
				.declarations()
				.iter()
				.any(|node| node.kind() == Kind::GetAccessor)
				&& !symbol
					.declarations()
					.iter()
					.any(|node| node.kind() == Kind::SetAccessor));
		let type_ = if matches!(
			declaration.kind(),
			Kind::Parameter | Kind::PropertyDeclaration
		) && let Some(node) = declaration.type_node()
		{
			self.type_node(node)
		} else {
			let type_ = self.checker.get_type_of_symbol(symbol);
			self.type_(type_)
		};

		// Document a method by its signatures rather than the property.
		let method = is_method(declaration);
		let comment = if method { None } else { documentation.comment };

		let property = Property {
			abstract_: flags.intersects(ModifierFlags::Abstract),
			class_method: false,
			comment,
			method,
			name: symbol.name().to_owned(),
			optional: symbol.flags().intersects(SymbolFlags::Optional),
			protected: flags.intersects(ModifierFlags::Protected),
			readonly,
			static_,
			type_,
		};

		Some(property)
	}

	fn type_alias(&mut self, symbol: P<Symbol>) -> Option<TypeAlias> {
		let declaration = symbol
			.declarations()
			.iter()
			.copied()
			.find(|declaration| declaration.kind() == Kind::TypeAliasDeclaration)?;
		let type_ = if let Some(node) = declaration.type_node() {
			self.type_node(node)
		} else {
			let type_ = self.checker.get_declared_type_of_symbol_exported(symbol);
			self.type_(type_)
		};
		let type_parameters = self.type_parameter_nodes(declaration.type_parameters());
		let alias = TypeAlias {
			comment: documentation(declaration).comment,
			location: self.location(declaration),
			name: symbol.name().to_owned(),
			type_,
			type_parameters,
		};
		Some(alias)
	}

	fn enum_(&mut self, declaration: P<Node>, name: &str) -> Enum {
		let members = declaration
			.members()
			.iter()
			.filter_map(|member| {
				let documentation = documentation(*member);
				if documentation.hidden {
					return None;
				}
				let name = member_name(*member);
				let initializer = member.initializer().map(node_text);
				let value = self
					.checker
					.get_constant_value(*member)
					.and_then(literal_value);
				let member = EnumMember {
					comment: documentation.comment,
					initializer,
					name,
					value,
				};
				Some(member)
			})
			.collect();
		Enum {
			comment: documentation(declaration).comment,
			location: self.location(declaration),
			members,
			name: name.to_owned(),
		}
	}

	fn interface(&mut self, declaration: P<Node>, name: &str) -> Interface {
		let extends = tsrs_ast::get_extends_heritage_clause_elements(declaration)
			.iter()
			.map(|node| self.type_node(*node))
			.collect();
		let members = self.member_nodes(declaration.members());
		let type_parameters = self.type_parameter_nodes(declaration.type_parameters());
		Interface {
			comment: documentation(declaration).comment,
			extends,
			location: self.location(declaration),
			members,
			name: name.to_owned(),
			type_parameters,
		}
	}

	fn member_nodes(&mut self, nodes: &[P<Node>]) -> Members {
		let mut members = Members::default();
		for node in nodes.iter().copied() {
			let documentation = documentation(node);
			if documentation.hidden {
				continue;
			}
			match node.kind() {
				Kind::CallSignature => {
					let signature = self.signature_declaration(node);
					members.call_signatures.push(signature);
				},
				Kind::ConstructSignature => {
					let signature = self.signature_declaration(node);
					members.construct_signatures.push(signature);
				},
				Kind::IndexSignature => {
					let signature = self.index_signature_declaration(node);
					members.index_signatures.push(signature);
				},
				Kind::MethodSignature => {
					// Merge the overloads of a method into a single property.
					let name = member_name(node);
					let signature = self.signature_declaration(node);
					if let Some(Property {
						type_: Type::Function(function),
						..
					}) = members
						.properties
						.last_mut()
						.filter(|property| property.method && property.name == name)
					{
						function.signatures.push(signature);
						continue;
					}
					let property = Property {
						abstract_: false,
						class_method: false,
						comment: None,
						method: true,
						name,
						optional: node.question_token().is_some(),
						protected: false,
						readonly: false,
						static_: false,
						type_: Type::Function(FunctionType {
							signatures: vec![signature],
						}),
					};
					members.properties.push(property);
				},
				_ => {
					let Some(symbol) = node.symbol() else {
						continue;
					};
					let type_ = if node.kind() == Kind::PropertySignature
						&& let Some(type_) = node.type_node()
					{
						self.type_node(type_)
					} else {
						let type_ = self.checker.get_type_of_symbol(symbol);
						self.type_(type_)
					};
					let flags = tsrs_ast::get_combined_modifier_flags(node);
					let property = Property {
						abstract_: false,
						class_method: false,
						comment: documentation.comment,
						method: false,
						name: member_name(node),
						optional: symbol.flags().intersects(SymbolFlags::Optional),
						protected: false,
						readonly: flags.intersects(ModifierFlags::Readonly)
							|| node.kind() == Kind::GetAccessor,
						static_: false,
						type_,
					};
					members.properties.push(property);
				},
			}
		}
		members
	}

	fn index_signature_declaration(&mut self, node: P<Node>) -> IndexSignature {
		let parameter = node.parameters().first().copied();
		let name = parameter
			.and_then(|parameter| parameter.name())
			.map_or_else(|| "key".to_owned(), node_text);
		let key = match parameter.and_then(|parameter| parameter.type_node()) {
			Some(node) => self.type_node(node),
			None => Type::Keyword("string".to_owned()),
		};
		let type_ = match node.type_node() {
			Some(node) => self.type_node(node),
			None => Type::Keyword("any".to_owned()),
		};
		let readonly =
			tsrs_ast::get_combined_modifier_flags(node).intersects(ModifierFlags::Readonly);
		IndexSignature {
			key,
			name,
			readonly,
			type_,
		}
	}

	fn signature_declaration(&mut self, node: P<Node>) -> Signature {
		// Get the documentation.
		let mut documentation = documentation(node);

		// Convert the parameters and type parameters.
		let parameters = node
			.parameters()
			.iter()
			.map(|parameter| {
				let mut parameter = self.parameter_declaration(*parameter);
				parameter.comment = documentation.parameters.remove(&parameter.name);
				parameter
			})
			.collect();
		let type_parameters = self.type_parameter_nodes(node.type_parameters());

		// Convert the return type, inferring it when it is not declared.
		let return_ = if let Some(type_) = node.type_node() {
			self.type_node(type_)
		} else if is_parsed(node) {
			let signature = self.checker.get_signature_from_declaration_exported(node);
			self.return_type(signature)
		} else {
			Type::Keyword("any".to_owned())
		};

		Signature {
			async_: tsrs_ast::get_combined_modifier_flags(node).intersects(ModifierFlags::Async),
			comment: documentation.comment,
			location: self.location(node),
			parameters,
			return_,
			returns: documentation.returns,
			type_parameters,
		}
	}

	fn signature(&mut self, signature: P<tsrs_checker::Signature>) -> Signature {
		// Use the declaration's syntax when the signature is not an instantiation.
		let declaration = signature.declaration();
		if let Some(declaration) = declaration
			&& is_parsed(declaration)
			&& signature.target().is_none()
			&& !matches!(
				declaration.kind(),
				Kind::ClassDeclaration | Kind::ClassExpression | Kind::JSDocSignature
			) {
			return self.signature_declaration(declaration);
		}

		// Convert the parameters.
		let parameters = signature
			.parameters()
			.iter()
			.map(|parameter| {
				let type_ = self.checker.get_type_of_symbol(*parameter);
				let type_ = self.type_(type_);
				let declaration = parameter.value_declaration();
				let rest = declaration.is_some_and(|declaration| {
					declaration.kind() == Kind::Parameter
						&& declaration
							.as_parameter_declaration()
							.dot_dot_dot_token()
							.is_some()
				});
				Parameter {
					comment: None,
					default: None,
					kind: None,
					name: parameter.name().to_owned(),
					optional: parameter.flags().intersects(SymbolFlags::Optional),
					rest,
					type_,
				}
			})
			.collect();

		// Convert the type parameters.
		let type_parameters = signature
			.type_parameters()
			.iter()
			.map(|type_parameter| self.type_parameter(*type_parameter))
			.collect();

		// Convert the return type.
		let return_ = self.return_type(signature);

		Signature {
			async_: false,
			comment: None,
			location: declaration.and_then(|declaration| self.location(declaration)),
			parameters,
			return_,
			returns: None,
			type_parameters,
		}
	}

	fn return_type(&mut self, signature: P<tsrs_checker::Signature>) -> Type {
		if let Some(predicate) = self.checker.get_type_predicate_of_signature(signature) {
			let asserts = matches!(
				predicate.kind(),
				TypePredicateKind::AssertsIdentifier | TypePredicateKind::AssertsThis
			);
			let name = match predicate.kind() {
				TypePredicateKind::AssertsThis | TypePredicateKind::This => "this".to_owned(),
				TypePredicateKind::AssertsIdentifier | TypePredicateKind::Identifier => {
					predicate.parameter_name().to_owned()
				},
			};
			let type_ = predicate.type_().map(|type_| self.type_(type_));
			return Type::Predicate(Box::new(PredicateType {
				asserts,
				name,
				type_,
			}));
		}
		let type_ = self.checker.get_return_type_of_signature(signature);
		self.type_(type_)
	}

	fn parameter_declaration(&mut self, node: P<Node>) -> Parameter {
		let name = node.name().map(node_text).unwrap_or_default();
		let (optional, rest) = if node.kind() == Kind::Parameter {
			let declaration = node.as_parameter_declaration();
			let optional = node.question_token().is_some() || node.initializer().is_some();
			let rest = declaration.dot_dot_dot_token().is_some();
			(optional, rest)
		} else {
			(false, false)
		};
		let default = node.initializer().map(node_text);
		let type_ = if let Some(type_) = node.type_node() {
			self.type_node(type_)
		} else if is_parsed(node) {
			let type_ = self.checker.get_type_at_location(node);
			self.type_(type_)
		} else {
			Type::Keyword("any".to_owned())
		};
		Parameter {
			comment: None,
			default,
			kind: None,
			name,
			optional,
			rest,
			type_,
		}
	}

	fn type_parameter_nodes(&mut self, nodes: &[P<Node>]) -> Vec<TypeParameter> {
		nodes
			.iter()
			.map(|node| self.type_parameter_node(*node))
			.collect()
	}

	fn type_parameter_node(&mut self, node: P<Node>) -> TypeParameter {
		let declaration = node.as_type_parameter_declaration();
		let name = node_text(declaration.name);
		let constraint = declaration.constraint.map(|node| self.type_node(node));
		let default = declaration.default_type.map(|node| self.type_node(node));
		TypeParameter {
			constraint,
			default,
			name,
		}
	}

	fn type_parameter(&mut self, type_: P<tsrs_checker::Type>) -> TypeParameter {
		// Prefer the declaration's syntax.
		let declaration = type_.symbol().and_then(|symbol| {
			symbol
				.declarations()
				.iter()
				.copied()
				.find(|node| node.kind() == Kind::TypeParameter)
		});
		if let Some(declaration) = declaration {
			return self.type_parameter_node(declaration);
		}

		// Otherwise, use the checker.
		let name = type_
			.symbol()
			.map(|symbol| symbol.name().to_owned())
			.unwrap_or_default();
		let constraint = self
			.checker
			.get_constraint_of_type_parameter_exported(type_)
			.map(|type_| self.type_(type_));
		let default = self
			.checker
			.get_default_from_type_parameter_exported(type_)
			.map(|type_| self.type_(type_));
		TypeParameter {
			constraint,
			default,
			name,
		}
	}

	fn type_node(&mut self, node: P<Node>) -> Type {
		match node.kind() {
			Kind::ArrayType => {
				let type_ = self.type_node(node.as_array_type_node().element_type);
				Type::Array(Box::new(ArrayType { type_ }))
			},
			Kind::ConditionalType => {
				let node = node.as_conditional_type_node();
				let conditional = ConditionalType {
					check_type: self.type_node(node.check_type),
					extends_type: self.type_node(node.extends_type),
					false_type: self.type_node(node.false_type),
					true_type: self.type_node(node.true_type),
				};
				Type::Conditional(Box::new(conditional))
			},
			Kind::ConstructorType => {
				let signature = self.signature_declaration(node);
				let members = Members {
					construct_signatures: vec![signature],
					..Default::default()
				};
				Type::Object(members)
			},
			Kind::ExpressionWithTypeArguments => {
				let expression = node.expression();
				let Some(expression) = expression else {
					return Type::Other(node_text(node));
				};
				self.reference_node(expression, node.type_arguments())
			},
			Kind::FunctionType => {
				let signature = self.signature_declaration(node);
				Type::Function(FunctionType {
					signatures: vec![signature],
				})
			},
			Kind::IndexedAccessType => {
				let node = node.as_indexed_access_type_node();
				let indexed_access = IndexedAccessType {
					index_type: self.type_node(node.index_type),
					object_type: self.type_node(node.object_type),
				};
				Type::IndexedAccess(Box::new(indexed_access))
			},
			Kind::InferType => {
				let type_parameter =
					self.type_parameter_node(node.as_infer_type_node().type_parameter);
				Type::Infer(Box::new(type_parameter))
			},
			Kind::IntersectionType => {
				let types = node
					.as_intersection_type_node()
					.union_or_intersection_type_node_base
					.types
					.nodes()
					.iter()
					.map(|node| self.type_node(*node))
					.collect();
				Type::Intersection(Types { types })
			},
			Kind::LiteralType => literal_node(node.as_literal_type_node().literal)
				.map_or_else(|| Type::Other(node_text(node)), Type::Literal),
			Kind::MappedType => {
				let node = node.as_mapped_type_node();
				let type_parameter = node.type_parameter.as_type_parameter_declaration();
				let mapped = MappedType {
					constraint: type_parameter.constraint.map(|node| self.type_node(node)),
					name_type: node.name_type.map(|node| self.type_node(node)),
					optional: node.question_token.is_some(),
					readonly: node.readonly_token.is_some(),
					type_: node.type_.map(|node| self.type_node(node)),
					type_parameter_name: node_text(type_parameter.name),
				};
				Type::Mapped(Box::new(mapped))
			},
			Kind::ParenthesizedType => self.type_node(node.as_parenthesized_type_node().type_),
			Kind::TemplateLiteralType => {
				let node = node.as_template_literal_type_node();
				let template_spans = node
					.template_spans
					.nodes()
					.iter()
					.map(|span| {
						let span = span.as_template_literal_type_span();
						TemplateLiteralTypeSpan {
							literal: span.literal.text().to_owned(),
							type_: self.type_node(span.type_()),
						}
					})
					.collect();
				Type::TemplateLiteral(TemplateLiteralType {
					head: node.head.text().to_owned(),
					template_spans,
				})
			},
			Kind::ThisType => Type::Keyword("this".to_owned()),
			Kind::TupleType => {
				let elements = node
					.as_tuple_type_node()
					.elements()
					.nodes()
					.iter()
					.map(|element| self.tuple_element_node(*element))
					.collect();
				Type::Tuple(TupleType { elements })
			},
			Kind::TypeLiteral => {
				let members = self.member_nodes(node.members());
				Type::Object(members)
			},
			Kind::TypeOperator => {
				let node = node.as_type_operator_node();
				let operator = match node.operator {
					Kind::KeyOfKeyword => "keyof",
					Kind::ReadonlyKeyword => "readonly",
					Kind::UniqueKeyword => "unique",
					_ => "",
				};
				let operator = OperatorType {
					operator: operator.to_owned(),
					type_: self.type_node(node.type_),
				};
				Type::Operator(Box::new(operator))
			},
			Kind::TypePredicate => {
				let node = node.as_type_predicate_node();
				let predicate = PredicateType {
					asserts: node.asserts_modifier.is_some(),
					name: node_text(node.parameter_name),
					type_: node.type_.map(|node| self.type_node(node)),
				};
				Type::Predicate(Box::new(predicate))
			},
			Kind::TypeQuery => Type::Query(QueryType {
				name: node_text(node.as_type_query_node().expr_name),
			}),
			Kind::TypeReference => {
				let type_name = node.as_type_reference_node().type_name;
				self.reference_node(type_name, node.type_arguments())
			},
			Kind::UnionType => {
				let types = node
					.as_union_type_node()
					.union_or_intersection_type_node_base
					.types
					.nodes()
					.iter()
					.map(|node| self.type_node(*node))
					.collect();
				Type::Union(Types { types })
			},
			kind => keyword(kind).map_or_else(
				|| Type::Other(node_text(node)),
				|keyword| Type::Keyword(keyword.to_owned()),
			),
		}
	}

	fn tuple_element_node(&mut self, node: P<Node>) -> TupleElement {
		match node.kind() {
			Kind::NamedTupleMember => {
				let member = node.as_named_tuple_member();
				TupleElement {
					name: Some(node_text(member.name())),
					optional: member.question_token.is_some(),
					rest: member.dot_dot_dot_token.is_some(),
					type_: self.type_node(member.type_),
				}
			},
			Kind::OptionalType => TupleElement {
				name: None,
				optional: true,
				rest: false,
				type_: self.type_node(node.as_optional_type_node().type_()),
			},
			Kind::RestType => TupleElement {
				name: None,
				optional: false,
				rest: true,
				type_: self.type_node(node.as_rest_type_node().type_()),
			},
			_ => TupleElement {
				name: None,
				optional: false,
				rest: false,
				type_: self.type_node(node),
			},
		}
	}

	fn reference_node(&mut self, name: P<Node>, type_arguments: &[P<Node>]) -> Type {
		let type_arguments = type_arguments
			.iter()
			.map(|node| self.type_node(*node))
			.collect();
		let text = node_text(name);
		let symbol = self
			.checker
			.get_symbol_at_location_exported(name)
			.map(|symbol| self.resolve(symbol))
			.filter(|symbol| !self.checker.is_unknown_symbol(*symbol));
		let Some(resolved) = symbol else {
			return Type::Reference(ReferenceType {
				declarations: Vec::new(),
				fully_qualified_name: None,
				is_type_parameter: false,
				name: text,
				runtime: false,
				type_arguments,
			});
		};
		let declaration = resolved.declarations().first().copied();
		let fully_qualified_name =
			Some(fully_qualified_name(declaration, resolved)).filter(|name| *name != text);
		let reference = ReferenceType {
			declarations: vec![self.slot(resolved)],
			fully_qualified_name,
			is_type_parameter: declaration
				.is_some_and(|declaration| declaration.kind() == Kind::TypeParameter),
			name: text,
			runtime: is_runtime_declaration(declaration),
			type_arguments,
		};
		Type::Reference(reference)
	}

	fn type_(&mut self, type_: P<tsrs_checker::Type>) -> Type {
		// Avoid expanding recursive or deeply nested types.
		if self.types.len() >= DEPTH || self.types.contains(&type_) {
			return self.other(type_);
		}
		self.types.push(type_);
		let output = self.type_inner(type_);
		self.types.pop();
		output
	}

	fn type_inner(&mut self, type_: P<tsrs_checker::Type>) -> Type {
		// Document an anonymous object type that only has call signatures as a function.
		let flags = type_.flags.get();
		if flags.intersects(TypeFlags::Object)
			&& type_.object_flags().intersects(ObjectFlags::Anonymous)
			&& type_.alias().is_none()
			&& !self.checker.get_call_signatures(type_).is_empty()
			&& self
				.checker
				.get_signatures_of_type_exported(type_, SignatureKind::Construct)
				.is_empty()
			&& self.checker.get_properties_of_type(type_).is_empty()
			&& self
				.checker
				.get_index_infos_of_type_exported(type_)
				.is_empty()
		{
			let signatures = self
				.checker
				.get_call_signatures(type_)
				.to_vec()
				.into_iter()
				.map(|signature| self.signature(signature))
				.collect();
			return Type::Function(FunctionType { signatures });
		}

		// Classify the type by the syntax the checker would use to print it.
		let node = self
			.checker
			.type_to_type_node(type_, None, Flags::IgnoreErrors, None);
		let Some(node) = node else {
			return self.other(type_);
		};
		match node.kind() {
			Kind::ArrayType if is_reference(type_) => {
				let element = self
					.checker
					.get_type_arguments_exported(type_)
					.first()
					.copied();
				let Some(element) = element else {
					return self.other(type_);
				};
				let type_ = self.type_(element);
				Type::Array(Box::new(ArrayType { type_ }))
			},
			Kind::IndexedAccessType if flags.intersects(TypeFlags::IndexedAccess) => {
				let indexed_access = type_.as_indexed_access_type();
				let (Some(object_type), Some(index_type)) =
					(indexed_access.object_type(), indexed_access.index_type())
				else {
					return self.other(type_);
				};
				let indexed_access = IndexedAccessType {
					index_type: self.type_(index_type),
					object_type: self.type_(object_type),
				};
				Type::IndexedAccess(Box::new(indexed_access))
			},
			Kind::IntersectionType | Kind::UnionType
				if flags.intersects(TypeFlags::UnionOrIntersection) =>
			{
				let types = type_
					.types()
					.iter()
					.map(|type_| self.type_(*type_))
					.collect();
				let types = Types { types };
				if flags.intersects(TypeFlags::Union) {
					Type::Union(types)
				} else {
					Type::Intersection(types)
				}
			},
			Kind::LiteralType => literal_node(node.as_literal_type_node().literal)
				.map_or_else(|| self.other(type_), Type::Literal),
			Kind::TemplateLiteralType if flags.intersects(TypeFlags::TemplateLiteral) => {
				let template_literal = type_.as_template_literal_type();
				let texts = template_literal.texts();
				let template_spans = type_
					.types()
					.iter()
					.zip(texts.iter().skip(1))
					.map(|(type_, literal)| TemplateLiteralTypeSpan {
						literal: (*literal).to_owned(),
						type_: self.type_(*type_),
					})
					.collect();
				Type::TemplateLiteral(TemplateLiteralType {
					head: texts.first().copied().unwrap_or_default().to_owned(),
					template_spans,
				})
			},
			Kind::ThisType => Type::Keyword("this".to_owned()),
			Kind::TupleType if is_reference(type_) => {
				let count = node.as_tuple_type_node().elements().nodes().len();
				let elements = self.tuple_elements(type_, count);
				Type::Tuple(TupleType { elements })
			},
			Kind::TypeLiteral => {
				// Print a large inferred object type rather than expanding it.
				let count = self.checker.get_properties_of_type(type_).len()
					+ self.checker.get_call_signatures(type_).len()
					+ self
						.checker
						.get_signatures_of_type_exported(type_, SignatureKind::Construct)
						.len() + self.checker.get_index_infos_of_type_exported(type_).len();
				if count > MEMBERS {
					return self.other(type_);
				}
				let members = self.members(type_);
				Type::Object(members)
			},
			Kind::TypeReference => {
				let count = node.type_arguments().len();
				self.reference(type_, count)
			},
			kind => keyword(kind).map_or_else(
				|| self.other(type_),
				|keyword| Type::Keyword(keyword.to_owned()),
			),
		}
	}

	fn tuple_elements(&mut self, type_: P<tsrs_checker::Type>, count: usize) -> Vec<TupleElement> {
		// Get the element flags and infos of the tuple target.
		let tuple = type_
			.target()
			.filter(|target| target.object_flags().intersects(ObjectFlags::Tuple))
			.map(|target| target.as_tuple_type());
		let element_flags = tuple
			.map(tsrs_checker::TupleType::element_flags)
			.unwrap_or_default();
		let infos = tuple
			.map(tsrs_checker::TupleType::element_infos)
			.unwrap_or_default();

		// Convert the elements.
		let types = self
			.checker
			.get_type_arguments_exported(type_)
			.iter()
			.take(count)
			.copied()
			.collect::<Vec<_>>();
		types
			.into_iter()
			.enumerate()
			.map(|(index, type_)| {
				let flags = element_flags.get(index).copied().unwrap_or_default();
				let name = infos
					.get(index)
					.and_then(|info| info.labeled_declaration())
					.and_then(|declaration| declaration.name())
					.map(node_text);
				TupleElement {
					name,
					optional: flags.intersects(ElementFlags::Optional),
					rest: flags.intersects(ElementFlags::Rest | ElementFlags::Variadic),
					type_: self.type_(type_),
				}
			})
			.collect()
	}

	fn other(&mut self, type_: P<tsrs_checker::Type>) -> Type {
		let text = self.checker.type_to_string_exported(type_);
		Type::Other(text)
	}

	fn members(&mut self, type_: P<tsrs_checker::Type>) -> Members {
		// Convert the properties.
		let mut properties = Vec::new();
		for symbol in self.checker.get_properties_of_type(type_).to_vec() {
			let declaration = symbol
				.value_declaration()
				.or_else(|| symbol.declarations().first().copied());
			let documentation = declaration.map(documentation).unwrap_or_default();
			if documentation.hidden {
				continue;
			}
			let type_ = if let Some(declaration) = declaration
				&& declaration.kind() == Kind::PropertySignature
				&& let Some(node) = declaration.type_node()
			{
				self.type_node(node)
			} else {
				let type_ = self.checker.get_type_of_symbol(symbol);
				self.type_(type_)
			};
			let readonly = declaration.is_some_and(|declaration| {
				tsrs_ast::get_combined_modifier_flags(declaration)
					.intersects(ModifierFlags::Readonly)
			});
			let method = declaration.is_some_and(is_method);
			let comment = if method { None } else { documentation.comment };
			let property = Property {
				abstract_: false,
				class_method: false,
				comment,
				method,
				name: symbol.name().to_owned(),
				optional: symbol.flags().intersects(SymbolFlags::Optional),
				protected: false,
				readonly,
				static_: false,
				type_,
			};
			properties.push(property);
		}

		// Convert the signatures.
		let call_signatures = self
			.checker
			.get_call_signatures(type_)
			.to_vec()
			.into_iter()
			.map(|signature| self.signature(signature))
			.collect();
		let construct_signatures = self
			.checker
			.get_signatures_of_type_exported(type_, SignatureKind::Construct)
			.to_vec()
			.into_iter()
			.map(|signature| self.signature(signature))
			.collect();

		// Convert the index signatures.
		let index_signatures = self
			.checker
			.get_index_infos_of_type_exported(type_)
			.to_vec()
			.into_iter()
			.map(|info| {
				if let Some(declaration) = info.declaration()
					&& is_parsed(declaration)
				{
					return self.index_signature_declaration(declaration);
				}
				let key = self.type_(info.key_type());
				let type_ = self.type_(info.value_type());
				IndexSignature {
					key,
					name: "key".to_owned(),
					readonly: info.is_readonly(),
					type_,
				}
			})
			.collect();

		Members {
			call_signatures,
			construct_signatures,
			index_signatures,
			properties,
		}
	}

	fn reference(&mut self, type_: P<tsrs_checker::Type>, count: usize) -> Type {
		// Get the symbol and the type arguments.
		let (symbol, type_arguments) = if let Some(alias) = type_.alias()
			&& let Some(symbol) = alias.symbol()
		{
			(symbol, alias.type_arguments().to_vec())
		} else if let Some(symbol) = type_.symbol() {
			let type_arguments = if is_reference(type_) {
				self.checker.get_type_arguments_exported(type_).to_vec()
			} else {
				Vec::new()
			};
			(symbol, type_arguments)
		} else {
			return self.other(type_);
		};
		let type_arguments = type_arguments
			.into_iter()
			.take(count)
			.map(|type_| self.type_(type_))
			.collect();

		// Create the reference, naming a type parameter by its own name and any other type by its fully qualified name.
		let resolved = self.resolve(symbol);
		let declaration = resolved.declarations().first().copied();
		let is_type_parameter = type_.flags.get().intersects(TypeFlags::TypeParameter);
		let (name, fully_qualified_name) = if is_type_parameter {
			let name = resolved.name().to_owned();
			let fully_qualified_name =
				Some(fully_qualified_name(declaration, resolved)).filter(|fqn| *fqn != name);
			(name, fully_qualified_name)
		} else {
			(fully_qualified_name(declaration, resolved), None)
		};
		let reference = ReferenceType {
			declarations: vec![self.slot(resolved)],
			fully_qualified_name,
			is_type_parameter,
			name,
			runtime: is_runtime_declaration(declaration),
			type_arguments,
		};
		Type::Reference(reference)
	}

	/// Get the source of a node in a module generated for another language.
	fn source(&self, node: P<Node>) -> Option<crate::load::Source> {
		let source_file = tsrs_ast::get_source_file_of_node(node)?;
		let module = self
			.project
			.host
			.try_module(source_file.file_name())
			.ok()??;
		if module.kind != tg::module::Kind::Python {
			return None;
		}
		if !is_parsed(node) {
			return None;
		}
		let offset = tsrs_scanner::get_token_pos_of_node(node, source_file, false);
		crate::load::source(source_file.text(), offset as usize)
	}

	fn location(&mut self, node: P<Node>) -> Option<Location> {
		// Get the source file.
		if !is_parsed(node) {
			return None;
		}
		let source_file = tsrs_ast::get_source_file_of_node(node)?;

		// Get the module's index, adding the module without its tokens if necessary.
		let file_name = source_file.file_name();
		let module = if let Some(module) = self.module_ids.get(file_name) {
			*module
		} else {
			let module = self
				.project
				.host
				.try_module(file_name)
				.ok()??
				.without_token();
			let index = self.modules.len();
			self.modules.push(module);
			self.module_ids.insert(file_name.to_owned(), index);
			index
		};

		// Get the range, using the source of an export in a module generated for another language.
		let start = tsrs_scanner::get_token_pos_of_node(node, source_file, false);
		let end = node.loc().source_end()?;
		let range_ = if let Some(source) = self.source(node) {
			let encoding = *self.project.host.compiler.position_encoding.read().unwrap();
			source.range(encoding)
		} else {
			let (start, _) = self.converters.to_lsp_position(&source_file, start.into());
			let (end, _) = self.converters.to_lsp_position(&source_file, end.into());
			range(tsrs_lsproto::Range { end, start })
		};
		let range = [
			range_.start.line,
			range_.start.character,
			range_.end.line,
			range_.end.character,
		];

		let location = Location { module, range };
		Some(location)
	}
}

fn documentation(node: P<Node>) -> Documentation {
	// Get the node that hosts the documentation comment.
	let mut host = node;
	if matches!(
		host.kind(),
		Kind::ArrowFunction | Kind::ClassExpression | Kind::FunctionExpression
	) && let Some(parent) = host.parent()
		&& matches!(
			parent.kind(),
			Kind::ExportAssignment
				| Kind::PropertyAssignment
				| Kind::PropertyDeclaration
				| Kind::VariableDeclaration
		) {
		host = parent;
	}
	if host.kind() == Kind::VariableDeclaration
		&& let Some(list) = host.parent()
		&& let Some(statement) = list.parent()
		&& statement.kind() == Kind::VariableStatement
	{
		host = statement;
	}
	if !is_parsed(host) {
		return Documentation::default();
	}

	// Get the closest documentation comment.
	let Some(jsdoc) = host.jsdoc(None).last().copied() else {
		return Documentation::default();
	};

	// Sort the tags into the documentation.
	let mut documentation = Documentation::default();
	let mut comment = Comment {
		text: tsrs_scanner::get_text_of_jsdoc_comment(jsdoc.comment_list()),
		..Default::default()
	};
	let tags = jsdoc
		.as_jsdoc()
		.tags
		.map(|tags| tags.nodes())
		.unwrap_or_default();
	for tag in tags {
		let name = tag.tag_name().text();
		let text = tsrs_scanner::get_text_of_jsdoc_comment(tag.comment_list());
		match name {
			"deprecated" => {
				comment.deprecated = Some(text);
			},
			"example" => {
				comment.examples.push(text);
			},
			"hidden" | "internal" => {
				documentation.hidden = true;
			},
			"return" | "returns" => {
				documentation.returns = Some(text);
			},
			_ if tag.kind() == Kind::JSDocParameterTag => {
				if let Some(parameter) = tag.name() {
					let text = text.strip_prefix("- ").map_or(text.clone(), str::to_owned);
					documentation.parameters.insert(node_text(parameter), text);
				}
			},
			_ if TYPE_TAGS.contains(&name) => {},
			_ => {
				let name = name.to_owned();
				comment.tags.push(Tag { name, text });
			},
		}
	}
	let empty = comment.deprecated.is_none()
		&& comment.examples.is_empty()
		&& comment.tags.is_empty()
		&& comment.text.is_empty();
	if !empty {
		documentation.comment = Some(comment);
	}

	documentation
}

fn fully_qualified_name(declaration: Option<P<Node>>, symbol: P<Symbol>) -> String {
	// Collect the names of the enclosing declarations.
	let mut names = Vec::new();
	let mut parent = declaration.and_then(|declaration| declaration.parent());
	while let Some(node) = parent {
		if matches!(
			node.kind(),
			Kind::ClassDeclaration
				| Kind::FunctionDeclaration
				| Kind::InterfaceDeclaration
				| Kind::ModuleDeclaration
				| Kind::TypeAliasDeclaration
		) && let Some(name) = node.name()
			&& matches!(name.kind(), Kind::Identifier)
			&& !tsrs_ast::is_global_scope_augmentation(node)
		{
			names.push(name.text().to_owned());
		}
		parent = node.parent();
	}

	// Qualify a declaration from the runtime with the tg namespace.
	if is_runtime_declaration(declaration) {
		names.push("tg".to_owned());
	}

	names.reverse();
	names.push(symbol.name().to_owned());
	names.join(".")
}

fn is_member(symbol: P<Symbol>, class: P<Node>) -> bool {
	symbol.declarations().iter().any(|node| {
		let parent = node.parent();
		parent == Some(class)
			|| (node.kind() == Kind::Parameter
				&& parent.and_then(|parent| parent.parent()) == Some(class))
	})
}

fn is_method(node: P<Node>) -> bool {
	matches!(node.kind(), Kind::MethodDeclaration | Kind::MethodSignature)
}

fn is_parsed(node: P<Node>) -> bool {
	!node.flags().intersects(NodeFlags::Synthesized) && node.loc().source_pos().is_some()
}

fn is_reference(type_: P<tsrs_checker::Type>) -> bool {
	type_.flags.get().intersects(TypeFlags::Object)
		&& type_.object_flags().intersects(ObjectFlags::Reference)
}

fn is_runtime_declaration(declaration: Option<P<Node>>) -> bool {
	declaration
		.and_then(tsrs_ast::get_source_file_of_node)
		.is_some_and(|source_file| {
			source_file
				.file_name()
				.strip_prefix("/__library__/")
				.is_some_and(|path| !path.starts_with("lib."))
		})
}

fn keyword(kind: Kind) -> Option<&'static str> {
	let keyword = match kind {
		Kind::AnyKeyword => "any",
		Kind::BigIntKeyword => "bigint",
		Kind::BooleanKeyword => "boolean",
		Kind::IntrinsicKeyword => "intrinsic",
		Kind::NeverKeyword => "never",
		Kind::NullKeyword => "null",
		Kind::NumberKeyword => "number",
		Kind::ObjectKeyword => "object",
		Kind::StringKeyword => "string",
		Kind::SymbolKeyword => "symbol",
		Kind::UndefinedKeyword => "undefined",
		Kind::UnknownKeyword => "unknown",
		Kind::VoidKeyword => "void",
		_ => return None,
	};
	Some(keyword)
}

fn literal_node(node: P<Node>) -> Option<Literal> {
	let literal = match node.kind() {
		Kind::BigIntLiteral => Literal::Bigint(node.text().to_owned()),
		Kind::FalseKeyword => Literal::Boolean(false),
		Kind::NoSubstitutionTemplateLiteral | Kind::StringLiteral => {
			Literal::String(node.text().to_owned())
		},
		Kind::NullKeyword => Literal::Null,
		Kind::NumericLiteral => Literal::Number(number(node.text().parse().ok()?)?),
		Kind::PrefixUnaryExpression => {
			let expression = node.as_prefix_unary_expression();
			let operand = expression.operand;
			if expression.operator != Kind::MinusToken {
				return None;
			}
			match operand.kind() {
				Kind::NumericLiteral => {
					Literal::Number(number(-operand.text().parse::<f64>().ok()?)?)
				},
				Kind::BigIntLiteral => Literal::Bigint(format!("-{}", operand.text())),
				_ => return None,
			}
		},
		Kind::TrueKeyword => Literal::Boolean(true),
		Kind::UndefinedKeyword => Literal::Undefined,
		_ => return None,
	};
	Some(literal)
}

fn literal_value(value: LiteralValue) -> Option<Literal> {
	let literal = match value {
		LiteralValue::BigInt(value) => Literal::Bigint(value.to_string()),
		LiteralValue::Boolean(value) => Literal::Boolean(value),
		LiteralValue::Number(value) => Literal::Number(number(value.0)?),
		LiteralValue::String(value) => Literal::String(value.to_owned()),
	};
	Some(literal)
}

fn member_name(node: P<Node>) -> String {
	let Some(name) = node.name() else {
		return String::new();
	};
	match name.kind() {
		Kind::Identifier
		| Kind::NoSubstitutionTemplateLiteral
		| Kind::NumericLiteral
		| Kind::PrivateIdentifier
		| Kind::StringLiteral => name.text().to_owned(),
		_ => node_text(name),
	}
}

fn node_text(node: P<Node>) -> String {
	if matches!(node.kind(), Kind::Identifier | Kind::PrivateIdentifier) {
		return node.text().to_owned();
	}
	if !is_parsed(node) || tsrs_ast::get_source_file_of_node(node).is_none() {
		return String::new();
	}
	tsrs_scanner::get_text_of_node(node)
}

fn number(value: f64) -> Option<serde_json::Number> {
	#[expect(clippy::cast_possible_truncation)]
	if value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
		return Some(serde_json::Number::from(value as i64));
	}
	serde_json::Number::from_f64(value)
}
