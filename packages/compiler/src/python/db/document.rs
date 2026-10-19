use {
	self::docstring::Docstring,
	super::Database,
	ruff_db::{files::File, parsed::parsed_module, source::source_text},
	ruff_python_ast::{self as ast, Expr, Stmt},
	std::collections::{BTreeMap, HashMap, HashSet},
	tangram_client::{
		document::{
			Class, Comment, Declaration, Enum, EnumMember, Function, FunctionType, Literal,
			Location, Namespace, Output, Parameter, ParameterKind, Property, ReferenceType,
			Signature, TupleElement, TupleType, Type, TypeAlias, TypeParameter, Types, Variable,
		},
		prelude::*,
	},
	ty_python_semantic::{Db as _, HasType as _, SemanticModel, types::Type as TyType},
	ty_text_size::{Ranged as _, TextRange, TextSize},
};

mod docstring;

/// The bases that make a class an enum.
const ENUM_BASES: [&str; 5] = ["Enum", "Flag", "IntEnum", "IntFlag", "StrEnum"];

struct Converter<'a> {
	converted: HashSet<(File, String)>,
	database: &'a Database,
	declarations: Vec<Option<Declaration>>,
	encoding: tg::position::Encoding,
	module_ids: HashMap<File, usize>,
	modules: Vec<tg::module::Data>,
	slot_ids: HashMap<(File, String), usize>,
	slots: Vec<Vec<usize>>,
}

/// A binding of a name at the top level of a module.
enum Binding<'a> {
	Assignment(&'a Stmt, Option<&'a Stmt>),
	Class(&'a ast::StmtClassDef),
	Functions(Vec<&'a ast::StmtFunctionDef>),
	Import(&'a ast::Alias),
	TypeAlias(&'a ast::StmtTypeAlias),
}

/// The file whose declarations are being converted.
struct Context<'a> {
	file: File,
	model: SemanticModel<'a>,
	text: ruff_db::source::SourceText,
}

impl Database {
	pub(in crate::python) fn document(
		&self,
		file: File,
		encoding: tg::position::Encoding,
	) -> tg::Result<Output> {
		let mut converter = Converter {
			converted: HashSet::new(),
			database: self,
			declarations: Vec::new(),
			encoding,
			module_ids: HashMap::new(),
			modules: Vec::new(),
			slot_ids: HashMap::new(),
			slots: Vec::new(),
		};

		// Convert the module's exports. Each list of indices holds a slot until the slots are resolved.
		let exports = converter.module(file);

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

impl<'a> Converter<'a> {
	fn context(&self, file: File) -> Context<'a> {
		let program_file = self.database.program_file(file);
		let model = SemanticModel::new(self.database, program_file);
		let text = source_text(self.database, file);
		Context { file, model, text }
	}

	fn parsed(&self, file: File) -> ruff_db::parsed::ParsedModuleRef {
		let python_file = self.database.program_file(file).python_file(self.database);
		parsed_module(self.database, python_file).load(self.database)
	}

	fn module(&mut self, file: File) -> BTreeMap<String, Vec<usize>> {
		// Parse the module.
		let context = self.context(file);
		let parsed = self.parsed(file);
		let body = &parsed.syntax().body;

		// Collect the bindings and the exported names.
		let bindings = bindings(body);
		let names = all(body).unwrap_or_else(|| {
			bindings
				.iter()
				.filter(|(name, binding)| {
					!name.starts_with('_')
						&& match binding {
							Binding::Import(alias) => alias.asname.as_ref().is_some_and(|asname| {
								asname.as_str()
									== alias.name.as_str().rsplit('.').next().unwrap_or_default()
							}),
							_ => true,
						}
				})
				.map(|(name, _)| name.clone())
				.collect()
		});

		// Convert the exports.
		let mut exports = BTreeMap::new();
		for name in names {
			let Some(binding) = bindings.get(&name) else {
				continue;
			};
			let slot = match binding {
				Binding::Import(alias) => {
					let Some(slot) = self.import(&context, alias) else {
						continue;
					};
					slot
				},
				binding => self.binding(&context, &name, binding),
			};
			exports.insert(name, vec![slot]);
		}

		exports
	}

	fn slot(&mut self, file: File, name: &str) -> usize {
		let key = (file, name.to_owned());
		if let Some(slot) = self.slot_ids.get(&key) {
			return *slot;
		}
		let slot = self.slots.len();
		self.slots.push(Vec::new());
		self.slot_ids.insert(key, slot);
		slot
	}

	fn push(&mut self, slot: usize, declaration: Declaration) {
		let id = self.declarations.len();
		self.declarations.push(Some(declaration));
		self.slots[slot].push(id);
	}

	fn import(&mut self, context: &Context<'a>, alias: &ast::Alias) -> Option<usize> {
		// Document an imported module as a namespace.
		if let Some(TyType::ModuleLiteral(module)) = alias.inferred_type(&context.model) {
			let file = module.module(self.database).file(self.database)?;
			let name = alias
				.asname
				.as_ref()
				.unwrap_or(&alias.name)
				.as_str()
				.to_owned();
			return self.namespace(file, &name);
		}

		// Otherwise, document the imported definition.
		let name = alias.asname.as_ref().unwrap_or(&alias.name);
		let (file, range) = self.definition(context, name.range().start())?;
		self.database.entry(file)?;
		let context = self.context(file);
		let parsed = self.parsed(file);
		let bindings = bindings(&parsed.syntax().body);
		let (name, binding) = bindings
			.iter()
			.find(|(_, binding)| binding_name_ranges(binding).contains(&range))?;
		let slot = match binding {
			Binding::Import(alias) => self.import(&context, alias)?,
			binding => self.binding(&context, name, binding),
		};
		Some(slot)
	}

	fn namespace(&mut self, file: File, name: &str) -> Option<usize> {
		// Document only modules from the package.
		self.database.entry(file)?;
		let slot = self.slot(file, "");
		if !self.converted.insert((file, String::new())) {
			return Some(slot);
		}

		// Reserve the namespace's index, so that the exports may refer to it.
		let id = self.declarations.len();
		self.declarations.push(None);
		self.slots[slot].push(id);

		// Convert the module.
		let exports = self.module(file);
		let context = self.context(file);
		let parsed = self.parsed(file);
		let comment = body_docstring(&parsed.syntax().body).and_then(|docstring| docstring.comment);
		let location = self.location(&context, TextRange::up_to(TextSize::from(0)));
		let namespace = Namespace {
			comment,
			exports,
			location,
			name: name.to_owned(),
		};
		self.declarations[id] = Some(Declaration::Namespace(namespace));

		Some(slot)
	}

	fn binding(&mut self, context: &Context<'a>, name: &str, binding: &Binding<'_>) -> usize {
		// Convert each binding once.
		let slot = self.slot(context.file, name);
		if !self.converted.insert((context.file, name.to_owned())) {
			return slot;
		}

		// Name a declaration from a module generated for another language by its export.
		let source = binding_name_ranges(binding)
			.first()
			.and_then(|range| self.source(context, range.start()));
		let name = source.as_ref().map_or(name, |source| source.name.as_str());

		match binding {
			Binding::Assignment(statement, docstring) => {
				let declaration = self.assignment(context, name, statement, *docstring);
				self.push(slot, declaration);
			},
			Binding::Class(class) => {
				let declaration = self.class(context, class);
				self.push(slot, declaration);
			},
			Binding::Functions(functions) => {
				for function in overloads(functions) {
					let mut signature = self.function(context, function, false);
					let function = Function {
						comment: signature.comment.take(),
						location: signature.location.take(),
						name: name.to_owned(),
						signature,
					};
					self.push(slot, Declaration::Function(function));
				}
			},
			Binding::Import(_) => {},
			Binding::TypeAlias(alias) => {
				let type_parameters = self.type_parameters(context, alias.type_params.as_deref());
				let alias_ = TypeAlias {
					comment: None,
					location: self.location(context, alias.range()),
					name: name.to_owned(),
					type_: self.annotation(context, &alias.value),
					type_parameters,
				};
				self.push(slot, Declaration::TypeAlias(alias_));
			},
		}

		slot
	}

	fn assignment(
		&mut self,
		context: &Context<'a>,
		name: &str,
		statement: &Stmt,
		docstring: Option<&Stmt>,
	) -> Declaration {
		let comment = docstring
			.and_then(statement_docstring)
			.and_then(|docstring| Docstring::parse(&docstring).comment);
		let location = self.location(context, statement.range());
		match statement {
			Stmt::AnnAssign(assignment) => {
				// Document an assignment annotated with `TypeAlias` as a type alias.
				if is_named(&assignment.annotation, "TypeAlias")
					&& let Some(value) = &assignment.value
				{
					let alias = TypeAlias {
						comment,
						location,
						name: name.to_owned(),
						type_: self.annotation(context, value),
						type_parameters: Vec::new(),
					};
					return Declaration::TypeAlias(alias);
				}
				let (annotation, constant) = qualified(&assignment.annotation, "Final");
				let type_ = match (annotation, assignment.value.as_deref()) {
					(Some(annotation), _) => self.annotation(context, annotation),
					(None, Some(value)) => {
						literal(value).map_or_else(|| self.inferred(context, value), Type::Literal)
					},
					(None, None) => Type::Keyword("Any".to_owned()),
				};
				let variable = Variable {
					comment,
					constant,
					location,
					name: name.to_owned(),
					type_,
				};
				Declaration::Variable(variable)
			},
			Stmt::Assign(assignment) => {
				let type_ = self.inferred(context, &assignment.value);
				let variable = Variable {
					comment,
					constant: false,
					location,
					name: name.to_owned(),
					type_,
				};
				Declaration::Variable(variable)
			},
			_ => {
				let variable = Variable {
					comment,
					constant: false,
					location,
					name: name.to_owned(),
					type_: Type::Keyword("Any".to_owned()),
				};
				Declaration::Variable(variable)
			},
		}
	}

	fn class(&mut self, context: &Context<'a>, class: &ast::StmtClassDef) -> Declaration {
		let mut docstring = body_docstring(&class.body).unwrap_or_default();
		let comment = docstring.comment.take();
		let location = self.location(context, class.range());
		let name = class.name.as_str().to_owned();
		let bases = class
			.arguments
			.as_deref()
			.map(|arguments| arguments.args.iter().collect::<Vec<_>>())
			.unwrap_or_default();

		// Document a subclass of an enum as an enum.
		if bases
			.iter()
			.any(|base| ENUM_BASES.iter().any(|name| is_named(base, name)))
		{
			let members = enum_members(context, class);
			let enum_ = Enum {
				comment,
				location,
				members,
				name,
			};
			return Declaration::Enum(enum_);
		}

		// Convert the bases and type parameters.
		let extends = bases
			.iter()
			.map(|base| self.annotation(context, base))
			.collect();
		let type_parameters = self.type_parameters(context, class.type_params.as_deref());

		// Convert the members.
		let mut constructors = Vec::new();
		let mut properties = Vec::<Property>::new();
		let mut abstract_ = bases.iter().any(|base| is_named(base, "ABC"));
		let mut functions = BTreeMap::<&str, Vec<&ast::StmtFunctionDef>>::new();
		let mut order = Vec::new();
		for (index, statement) in class.body.iter().enumerate() {
			match statement {
				Stmt::FunctionDef(function) => {
					let name = function.name.as_str();
					if !functions.contains_key(name) {
						order.push(name);
					}
					functions.entry(name).or_default().push(function);
				},
				Stmt::AnnAssign(assignment) => {
					let Expr::Name(target) = &*assignment.target else {
						continue;
					};
					if is_private(&target.id) {
						continue;
					}
					let (annotation, static_) = qualified(&assignment.annotation, "ClassVar");
					let (annotation, readonly) = match annotation {
						Some(annotation) => qualified(annotation, "Final"),
						None => (None, false),
					};
					let type_ = match annotation {
						Some(annotation) => self.annotation(context, annotation),
						None => assignment.value.as_deref().map_or_else(
							|| Type::Keyword("Any".to_owned()),
							|value| self.inferred(context, value),
						),
					};
					let comment = class
						.body
						.get(index + 1)
						.and_then(statement_docstring)
						.and_then(|docstring| Docstring::parse(&docstring).comment);
					let property = Property {
						abstract_: false,
						class_method: false,
						comment,
						method: false,
						name: target.id.to_string(),
						optional: false,
						protected: false,
						readonly,
						static_,
						type_,
					};
					properties.push(property);
				},
				Stmt::Assign(assignment) => {
					let [Expr::Name(target)] = assignment.targets.as_slice() else {
						continue;
					};
					if is_private(&target.id) {
						continue;
					}
					let type_ = self.inferred(context, &assignment.value);
					let comment = class
						.body
						.get(index + 1)
						.and_then(statement_docstring)
						.and_then(|docstring| Docstring::parse(&docstring).comment);
					let property = Property {
						abstract_: false,
						class_method: false,
						comment,
						method: false,
						name: target.id.to_string(),
						optional: false,
						protected: false,
						readonly: false,
						static_: true,
						type_,
					};
					properties.push(property);
				},
				_ => {},
			}
		}
		for name in order {
			let functions = &functions[name];

			// Convert the constructor.
			if name == "__init__" {
				for function in overloads(functions) {
					// Document the parameters with the class's docstring when the constructor does not.
					let mut signature = self.function(context, function, true);
					signature.return_ = Type::Keyword("None".to_owned());
					for parameter in &mut signature.parameters {
						if parameter.comment.is_none() {
							parameter.comment = docstring.parameters.get(&parameter.name).cloned();
						}
					}
					constructors.push(signature);
				}
				continue;
			}
			if is_private(name) {
				continue;
			}

			// Convert a property, which is writable if it has a setter.
			if let Some(getter) = functions
				.iter()
				.find(|function| has_decorator(function, "property"))
			{
				let setter = functions.iter().any(|function| {
					function.decorator_list.iter().any(|decorator| {
						expression_text(&context.text, &decorator.expression).ends_with(".setter")
					})
				});
				let signature = self.function(context, getter, true);
				let property = Property {
					abstract_: has_decorator(getter, "abstractmethod"),
					class_method: false,
					comment: signature.comment,
					method: false,
					name: name.to_owned(),
					optional: false,
					protected: false,
					readonly: !setter,
					static_: false,
					type_: signature.return_,
				};
				abstract_ |= property.abstract_;
				properties.push(property);
				continue;
			}

			// Convert a method and its overloads.
			let functions = overloads(functions);
			let first = functions[0];
			let static_ = has_decorator(first, "staticmethod");
			let class_method = has_decorator(first, "classmethod");
			let method_abstract = functions
				.iter()
				.any(|function| has_decorator(function, "abstractmethod"));
			abstract_ |= method_abstract;
			let signatures = functions
				.iter()
				.map(|function| self.function(context, function, !static_))
				.collect();
			let property = Property {
				abstract_: method_abstract,
				class_method,
				comment: None,
				method: true,
				name: name.to_owned(),
				optional: false,
				protected: false,
				readonly: false,
				static_,
				type_: Type::Function(FunctionType { signatures }),
			};
			properties.push(property);
		}

		let class = Class {
			abstract_,
			comment,
			constructors,
			extends,
			implements: Vec::new(),
			location,
			name,
			properties,
			type_parameters,
		};
		Declaration::Class(class)
	}

	fn function(
		&mut self,
		context: &Context<'a>,
		function: &ast::StmtFunctionDef,
		method: bool,
	) -> Signature {
		// Parse the docstring.
		let mut docstring = body_docstring(&function.body).unwrap_or_default();

		// Add the message of a deprecated decorator to the comment.
		if let Some(message) = function.decorator_list.iter().find_map(|decorator| {
			let Expr::Call(call) = &decorator.expression else {
				return None;
			};
			if !is_named(&call.func, "deprecated") {
				return None;
			}
			let message = call.arguments.args.first().and_then(|argument| {
				argument
					.as_string_literal_expr()
					.map(|literal| literal.value.to_str().to_owned())
			});
			Some(message.unwrap_or_default())
		}) {
			docstring
				.comment
				.get_or_insert_with(Comment::default)
				.deprecated = Some(message);
		}

		// Convert the parameters, skipping the receiver of a method.
		let parameters = &function.parameters;
		let mut converted = Vec::new();
		let positional = parameters
			.posonlyargs
			.iter()
			.map(|parameter| (parameter, Some(ParameterKind::PositionalOnly)))
			.chain(parameters.args.iter().map(|parameter| (parameter, None)));
		for (index, (parameter, kind)) in positional.enumerate() {
			if method && index == 0 {
				continue;
			}
			let parameter = self.parameter(
				context,
				&parameter.parameter,
				parameter.default.as_deref(),
				kind,
				false,
			);
			converted.push(parameter);
		}
		if let Some(parameter) = &parameters.vararg {
			converted.push(self.parameter(context, parameter, None, None, true));
		}
		for parameter in &parameters.kwonlyargs {
			let parameter = self.parameter(
				context,
				&parameter.parameter,
				parameter.default.as_deref(),
				Some(ParameterKind::KeywordOnly),
				false,
			);
			converted.push(parameter);
		}
		if let Some(parameter) = &parameters.kwarg {
			let parameter = self.parameter(
				context,
				parameter,
				None,
				Some(ParameterKind::KeywordRest),
				false,
			);
			converted.push(parameter);
		}
		for parameter in &mut converted {
			parameter.comment = docstring.parameters.remove(&parameter.name);
		}

		// Convert the return type, inferring it when it is not annotated.
		let return_ = match &function.returns {
			Some(returns) => self.annotation(context, returns),
			None => self.inferred_return(context, function),
		};

		// Convert the type parameters.
		let type_parameters = self.type_parameters(context, function.type_params.as_deref());

		Signature {
			async_: function.is_async,
			comment: docstring.comment,
			location: self.location(context, function.range()),
			parameters: converted,
			return_,
			returns: docstring.returns,
			type_parameters,
		}
	}

	fn parameter(
		&mut self,
		context: &Context<'a>,
		parameter: &ast::Parameter,
		default: Option<&Expr>,
		kind: Option<ParameterKind>,
		rest: bool,
	) -> Parameter {
		let type_ = match &parameter.annotation {
			Some(annotation) => self.annotation(context, annotation),
			None => match default {
				Some(default) => self.inferred(context, default),
				None => Type::Keyword("Any".to_owned()),
			},
		};
		Parameter {
			comment: None,
			default: default.map(|default| expression_text(&context.text, default).to_owned()),
			kind,
			name: parameter.name.as_str().to_owned(),
			optional: default.is_some(),
			rest,
			type_,
		}
	}

	fn type_parameters(
		&mut self,
		context: &Context<'a>,
		type_parameters: Option<&ast::TypeParams>,
	) -> Vec<TypeParameter> {
		let Some(type_parameters) = type_parameters else {
			return Vec::new();
		};
		type_parameters
			.type_params
			.iter()
			.map(|type_parameter| match type_parameter {
				ast::TypeParam::ParamSpec(type_parameter) => TypeParameter {
					constraint: None,
					default: type_parameter
						.default
						.as_deref()
						.map(|default| self.annotation(context, default)),
					name: format!("**{}", type_parameter.name),
				},
				ast::TypeParam::TypeVar(type_parameter) => TypeParameter {
					constraint: type_parameter
						.bound
						.as_deref()
						.map(|bound| self.annotation(context, bound)),
					default: type_parameter
						.default
						.as_deref()
						.map(|default| self.annotation(context, default)),
					name: type_parameter.name.to_string(),
				},
				ast::TypeParam::TypeVarTuple(type_parameter) => TypeParameter {
					constraint: None,
					default: type_parameter
						.default
						.as_deref()
						.map(|default| self.annotation(context, default)),
					name: format!("*{}", type_parameter.name),
				},
			})
			.collect()
	}

	fn annotation(&mut self, context: &Context<'a>, expression: &Expr) -> Type {
		match expression {
			Expr::Attribute(_) | Expr::Name(_) => self.reference(context, expression, Vec::new()),
			Expr::BinOp(binary) if binary.op == ast::Operator::BitOr => {
				let mut types = Vec::new();
				for operand in [&*binary.left, &*binary.right] {
					match self.annotation(context, operand) {
						Type::Union(union) => types.extend(union.types),
						type_ => types.push(type_),
					}
				}
				Type::Union(Types { types })
			},
			Expr::EllipsisLiteral(_) => Type::Other("...".to_owned()),
			Expr::NoneLiteral(_) => Type::Keyword("None".to_owned()),
			Expr::StringLiteral(literal) => {
				// Link a forward reference to a declaration at the top level of the same module.
				let name = literal.value.to_str().to_owned();
				let declarations = if self.database.entry(context.file).is_some()
					&& self.is_top_level(context.file, &name)
				{
					vec![self.slot(context.file, &name)]
				} else {
					Vec::new()
				};
				Type::Reference(ReferenceType {
					declarations,
					fully_qualified_name: None,
					is_type_parameter: false,
					name,
					runtime: false,
					type_arguments: Vec::new(),
				})
			},
			Expr::Subscript(subscript) => self.subscript(context, subscript),
			_ => Type::Other(expression_text(&context.text, expression).to_owned()),
		}
	}

	fn subscript(&mut self, context: &Context<'a>, subscript: &ast::ExprSubscript) -> Type {
		// Get the arguments.
		let arguments = match &*subscript.slice {
			Expr::Tuple(tuple) => tuple.elts.iter().collect::<Vec<_>>(),
			slice => vec![slice],
		};
		let base = &*subscript.value;

		// Convert the special forms.
		if is_named(base, "Optional")
			&& let [argument] = arguments.as_slice()
		{
			let type_ = self.annotation(context, argument);
			let types = vec![type_, Type::Keyword("None".to_owned())];
			return Type::Union(Types { types });
		}
		if is_named(base, "Union") {
			let types = arguments
				.iter()
				.map(|argument| self.annotation(context, argument))
				.collect();
			return Type::Union(Types { types });
		}
		if is_named(base, "Literal") {
			let mut types = arguments
				.iter()
				.map(|argument| {
					literal(argument).map_or_else(
						|| Type::Other(expression_text(&context.text, argument).to_owned()),
						Type::Literal,
					)
				})
				.collect::<Vec<_>>();
			if types.len() == 1 {
				return types.remove(0);
			}
			return Type::Union(Types { types });
		}
		if [
			"Annotated",
			"ClassVar",
			"Final",
			"NotRequired",
			"ReadOnly",
			"Required",
		]
		.iter()
		.any(|name| is_named(base, name))
			&& let Some(argument) = arguments.first()
		{
			return self.annotation(context, argument);
		}
		if is_named(base, "Callable")
			&& let [parameters, return_] = arguments.as_slice()
		{
			let parameters = match parameters {
				Expr::List(list) => list
					.elts
					.iter()
					.enumerate()
					.map(|(index, parameter)| Parameter {
						comment: None,
						default: None,
						kind: Some(ParameterKind::PositionalOnly),
						name: format!("arg{index}"),
						optional: false,
						rest: false,
						type_: self.annotation(context, parameter),
					})
					.collect(),
				_ => Vec::new(),
			};
			let signature = Signature {
				async_: false,
				comment: None,
				location: None,
				parameters,
				return_: self.annotation(context, return_),
				returns: None,
				type_parameters: Vec::new(),
			};
			return Type::Function(FunctionType {
				signatures: vec![signature],
			});
		}
		if is_named(base, "tuple") || is_named(base, "Tuple") {
			let elements = arguments
				.iter()
				.map(|argument| {
					let rest = matches!(argument, Expr::EllipsisLiteral(_));
					TupleElement {
						name: None,
						optional: false,
						rest,
						type_: self.annotation(context, argument),
					}
				})
				.collect();
			return Type::Tuple(TupleType { elements });
		}

		// Convert a generic reference.
		let type_arguments = arguments
			.iter()
			.map(|argument| self.annotation(context, argument))
			.collect();
		self.reference(context, base, type_arguments)
	}

	fn reference(
		&mut self,
		context: &Context<'a>,
		expression: &Expr,
		type_arguments: Vec<Type>,
	) -> Type {
		// Resolve the definition of the name, or of the attribute's member.
		let name = expression_text(&context.text, expression).to_owned();
		let offset = match expression {
			Expr::Attribute(attribute) => attribute.attr.range().start(),
			_ => expression.range().start(),
		};
		let Some((file, range)) = self.definition(context, offset) else {
			return Type::Reference(ReferenceType {
				declarations: Vec::new(),
				fully_qualified_name: None,
				is_type_parameter: false,
				name,
				runtime: false,
				type_arguments,
			});
		};

		// Get the qualified name of the definition.
		let text = source_text(self.database, file);
		let definition = text
			.get(usize::from(range.start())..usize::from(range.end()))
			.unwrap_or_default()
			.to_owned();
		let runtime = self.database.is_runtime(file);
		let is_type_parameter =
			self.database.entry(file).is_some() && self.is_type_parameter(file, range);
		let qualified_name = self
			.qualified_name(file, range)
			.unwrap_or_else(|| definition.clone());
		let fully_qualified_name = if runtime {
			Some(format!("tg.{qualified_name}"))
		} else if self.database.entry(file).is_some() {
			Some(qualified_name)
		} else {
			None
		};
		let fully_qualified_name = fully_qualified_name.filter(|fqn| *fqn != name);

		// Link a definition at the top level of a module from the package.
		let declarations = if self.database.entry(file).is_some() {
			vec![self.slot(file, &definition)]
		} else {
			Vec::new()
		};

		let reference = ReferenceType {
			declarations,
			fully_qualified_name,
			is_type_parameter,
			name,
			runtime,
			type_arguments,
		};
		Type::Reference(reference)
	}

	/// Get the name of the definition at a range, qualified by its enclosing classes.
	fn qualified_name(&self, file: File, range: TextRange) -> Option<String> {
		let parsed = self.parsed(file);
		let mut finder = NameFinder {
			classes: Vec::new(),
			name: None,
			range,
		};
		ast::visitor::source_order::walk_body(&mut finder, &parsed.syntax().body);
		finder.name
	}

	fn is_top_level(&self, file: File, name: &str) -> bool {
		let parsed = self.parsed(file);
		bindings(&parsed.syntax().body).contains_key(name)
	}

	fn is_type_parameter(&self, file: File, range: TextRange) -> bool {
		let parsed = self.parsed(file);
		let mut found = false;
		ast::visitor::source_order::walk_body(
			&mut TypeParameterFinder {
				found: &mut found,
				range,
			},
			&parsed.syntax().body,
		);
		found
	}

	fn definition(&self, context: &Context<'a>, offset: TextSize) -> Option<(File, TextRange)> {
		let program_file = self.database.program_file(context.file);
		let targets = ty_ide::goto_definition(self.database, program_file, offset)?;
		let target = targets.value.into_iter().next()?;
		Some((target.file(), target.focus_range()))
	}

	fn inferred(&self, context: &Context<'a>, expression: &Expr) -> Type {
		let Some(type_) = expression.inferred_type(&context.model) else {
			return Type::Keyword("Any".to_owned());
		};
		let environment = context.model.program_environment();
		let text = type_.display(self.database, &environment).to_string();
		Type::Other(text)
	}

	fn inferred_return(&self, context: &Context<'a>, function: &ast::StmtFunctionDef) -> Type {
		// Take the return type from the display of the function's type, of the form `def name(...) -> type`.
		let Some(type_) = function.inferred_type(&context.model) else {
			return Type::Keyword("Any".to_owned());
		};
		let environment = context.model.program_environment();
		let text = type_.display(self.database, &environment).to_string();
		let return_ = text
			.rsplit_once(") -> ")
			.map_or("Unknown", |(_, return_)| return_);
		if return_ == "Unknown" {
			return Type::Keyword("Any".to_owned());
		}
		Type::Other(return_.to_owned())
	}

	/// Get the source of a position in a module generated for another language.
	fn source(&self, context: &Context<'a>, offset: TextSize) -> Option<crate::load::Source> {
		let entry = self.database.entry(context.file)?;
		if !matches!(
			entry.module.kind,
			tg::module::Kind::JavaScript | tg::module::Kind::TypeScript
		) {
			return None;
		}
		crate::load::source(&context.text, usize::from(offset))
	}

	fn location(&mut self, context: &Context<'a>, range: TextRange) -> Option<Location> {
		// Get the module's index, adding the module without its tokens if necessary.
		let module = if let Some(module) = self.module_ids.get(&context.file) {
			*module
		} else {
			let entry = self.database.entry(context.file)?;
			let index = self.modules.len();
			self.modules.push(entry.module.without_token());
			self.module_ids.insert(context.file, index);
			index
		};

		// Get the range, using the source of an export in a module generated for another language.
		let range = if let Some(source) = self.source(context, range.start()) {
			source.range(self.encoding)
		} else {
			self.database
				.range(context.file, range, self.encoding)
				.ok()?
		};
		let range = [
			range.start.line,
			range.start.character,
			range.end.line,
			range.end.character,
		];

		let location = Location { module, range };
		Some(location)
	}
}

/// A visitor that finds the name of a definition at a range, qualified by its enclosing classes.
struct NameFinder<'a> {
	classes: Vec<&'a str>,
	name: Option<String>,
	range: TextRange,
}

impl<'a> ast::visitor::source_order::SourceOrderVisitor<'a> for NameFinder<'a> {
	fn visit_stmt(&mut self, statement: &'a Stmt) {
		if self.name.is_some() {
			return;
		}

		// Check the names that the statement defines.
		let names = match statement {
			Stmt::AnnAssign(assignment) => vec![&*assignment.target],
			Stmt::Assign(assignment) => assignment.targets.iter().collect(),
			Stmt::TypeAlias(alias) => vec![&*alias.name],
			_ => Vec::new(),
		};
		let names = names
			.into_iter()
			.filter_map(|target| target.as_name_expr())
			.map(|name| (name.id.as_str(), name.range()))
			.chain(match statement {
				Stmt::ClassDef(class) => Some((class.name.as_str(), class.name.range())),
				Stmt::FunctionDef(function) => {
					Some((function.name.as_str(), function.name.range()))
				},
				_ => None,
			});
		for (name, range) in names {
			if range == self.range {
				let name = self
					.classes
					.iter()
					.copied()
					.chain(std::iter::once(name))
					.collect::<Vec<_>>()
					.join(".");
				self.name = Some(name);
				return;
			}
		}

		// Visit the statement's body within its class.
		if let Stmt::ClassDef(class) = statement {
			self.classes.push(class.name.as_str());
			ast::visitor::source_order::walk_stmt(self, statement);
			self.classes.pop();
		} else {
			ast::visitor::source_order::walk_stmt(self, statement);
		}
	}
}

/// A visitor that finds a type parameter declared at a range.
struct TypeParameterFinder<'a> {
	found: &'a mut bool,
	range: TextRange,
}

impl<'b> ast::visitor::source_order::SourceOrderVisitor<'b> for TypeParameterFinder<'_> {
	fn visit_type_param(&mut self, type_param: &'b ast::TypeParam) {
		if type_param.name().range() == self.range {
			*self.found = true;
		}
	}
}

/// Collect the bindings at the top level of a module, keyed by name.
fn bindings(body: &[Stmt]) -> BTreeMap<String, Binding<'_>> {
	let mut bindings = BTreeMap::new();
	for (index, statement) in body.iter().enumerate() {
		let docstring = body.get(index + 1);
		match statement {
			Stmt::AnnAssign(assignment) => {
				if let Expr::Name(target) = &*assignment.target {
					bindings.insert(
						target.id.to_string(),
						Binding::Assignment(statement, docstring),
					);
				}
			},
			Stmt::Assign(assignment) => {
				for target in &assignment.targets {
					if let Expr::Name(target) = target {
						bindings.insert(
							target.id.to_string(),
							Binding::Assignment(statement, docstring),
						);
					}
				}
			},
			Stmt::ClassDef(class) => {
				bindings.insert(class.name.to_string(), Binding::Class(class));
			},
			Stmt::FunctionDef(function) => {
				let name = function.name.to_string();
				match bindings.get_mut(&name) {
					Some(Binding::Functions(functions)) => functions.push(function),
					_ => {
						bindings.insert(name, Binding::Functions(vec![function]));
					},
				}
			},
			Stmt::Import(import) => {
				for alias in &import.names {
					let name = alias.asname.as_ref().map_or_else(
						|| alias.name.as_str().split('.').next().unwrap_or_default(),
						|asname| asname.as_str(),
					);
					bindings.insert(name.to_owned(), Binding::Import(alias));
				}
			},
			Stmt::ImportFrom(import) => {
				for alias in &import.names {
					let name = alias.asname.as_ref().unwrap_or(&alias.name);
					bindings.insert(name.to_string(), Binding::Import(alias));
				}
			},
			Stmt::TypeAlias(alias) => {
				if let Expr::Name(name) = &*alias.name {
					bindings.insert(name.id.to_string(), Binding::TypeAlias(alias));
				}
			},
			_ => {},
		}
	}
	bindings
}

/// Get the ranges of the names that a binding defines.
fn binding_name_ranges(binding: &Binding<'_>) -> Vec<TextRange> {
	match binding {
		Binding::Assignment(statement, _) => match statement {
			Stmt::AnnAssign(assignment) => vec![assignment.target.range()],
			Stmt::Assign(assignment) => assignment.targets.iter().map(ast::Expr::range).collect(),
			_ => Vec::new(),
		},
		Binding::Class(class) => vec![class.name.range()],
		Binding::Functions(functions) => functions
			.iter()
			.map(|function| function.name.range())
			.collect(),
		Binding::Import(alias) => vec![alias.asname.as_ref().unwrap_or(&alias.name).range()],
		Binding::TypeAlias(alias) => vec![alias.name.range()],
	}
}

/// Get the names in a module's `__all__`, if it is defined.
fn all(body: &[Stmt]) -> Option<Vec<String>> {
	let mut names = None::<Vec<String>>;
	for statement in body {
		let (target, value, extend) = match statement {
			Stmt::AnnAssign(assignment) => {
				(&*assignment.target, assignment.value.as_deref(), false)
			},
			Stmt::Assign(assignment) => match assignment.targets.as_slice() {
				[target] => (target, Some(&*assignment.value), false),
				_ => continue,
			},
			Stmt::AugAssign(assignment) => (&*assignment.target, Some(&*assignment.value), true),
			_ => continue,
		};
		if !matches!(target, Expr::Name(name) if name.id.as_str() == "__all__") {
			continue;
		}
		let elements = match value {
			Some(Expr::List(list)) => &list.elts,
			Some(Expr::Tuple(tuple)) => &tuple.elts,
			_ => continue,
		};
		let values = elements
			.iter()
			.filter_map(|element| element.as_string_literal_expr())
			.map(|literal| literal.value.to_str().to_owned());
		match (&mut names, extend) {
			(Some(names), true) => names.extend(values),
			_ => names = Some(values.collect()),
		}
	}
	names
}

/// Get the overloads of a function, or the function itself if it has no overloads.
fn overloads<'a>(functions: &[&'a ast::StmtFunctionDef]) -> Vec<&'a ast::StmtFunctionDef> {
	let overloads = functions
		.iter()
		.copied()
		.filter(|function| has_decorator(function, "overload"))
		.collect::<Vec<_>>();
	if overloads.is_empty() {
		functions.last().copied().into_iter().collect()
	} else {
		overloads
	}
}

fn body_docstring(body: &[Stmt]) -> Option<Docstring> {
	body.first()
		.and_then(statement_docstring)
		.map(|docstring| Docstring::parse(&docstring))
}

fn statement_docstring(statement: &Stmt) -> Option<String> {
	let Stmt::Expr(expression) = statement else {
		return None;
	};
	let literal = expression.value.as_string_literal_expr()?;
	Some(literal.value.to_str().to_owned())
}

fn enum_members(context: &Context<'_>, class: &ast::StmtClassDef) -> Vec<EnumMember> {
	let mut members = Vec::new();
	for (index, statement) in class.body.iter().enumerate() {
		let (target, value) = match statement {
			Stmt::Assign(assignment) => match assignment.targets.as_slice() {
				[Expr::Name(target)] => (target, Some(&*assignment.value)),
				_ => continue,
			},
			Stmt::AnnAssign(assignment) => match &*assignment.target {
				Expr::Name(target) => (target, assignment.value.as_deref()),
				_ => continue,
			},
			_ => continue,
		};
		if is_private(&target.id) {
			continue;
		}
		let comment = class
			.body
			.get(index + 1)
			.and_then(statement_docstring)
			.and_then(|docstring| Docstring::parse(&docstring).comment);
		let member = EnumMember {
			comment,
			initializer: value.map(|value| expression_text(&context.text, value).to_owned()),
			name: target.id.to_string(),
			value: value.and_then(literal),
		};
		members.push(member);
	}
	members
}

fn expression_text<'a>(text: &'a str, expression: &Expr) -> &'a str {
	let range = expression.range();
	text.get(usize::from(range.start())..usize::from(range.end()))
		.unwrap_or_default()
}

fn has_decorator(function: &ast::StmtFunctionDef, name: &str) -> bool {
	function
		.decorator_list
		.iter()
		.any(|decorator| is_named(&decorator.expression, name))
}

/// Determine whether an expression is a name or an attribute whose final segment is the given name.
fn is_named(expression: &Expr, name: &str) -> bool {
	match expression {
		Expr::Attribute(attribute) => attribute.attr.as_str() == name,
		Expr::Name(expression) => expression.id.as_str() == name,
		Expr::Subscript(subscript) => is_named(&subscript.value, name),
		_ => false,
	}
}

fn is_private(name: &str) -> bool {
	name.starts_with('_') && !(name.starts_with("__") && name.ends_with("__"))
}

/// Remove a qualifier such as `Final[T]`, returning the qualified annotation and whether the qualifier was present.
fn qualified<'a>(annotation: &'a Expr, qualifier: &str) -> (Option<&'a Expr>, bool) {
	match annotation {
		Expr::Subscript(subscript) if is_named(&subscript.value, qualifier) => {
			(Some(&*subscript.slice), true)
		},
		annotation if is_named(annotation, qualifier) => (None, true),
		annotation => (Some(annotation), false),
	}
}

fn literal(expression: &Expr) -> Option<Literal> {
	let literal = match expression {
		Expr::BooleanLiteral(literal) => Literal::Boolean(literal.value),
		Expr::NoneLiteral(_) => Literal::Null,
		Expr::NumberLiteral(literal) => match &literal.value {
			ast::Number::Float(value) => Literal::Number(serde_json::Number::from_f64(*value)?),
			ast::Number::Int(value) => Literal::Number(value.as_i64()?.into()),
			ast::Number::Complex { .. } => return None,
		},
		Expr::StringLiteral(literal) => Literal::String(literal.value.to_str().to_owned()),
		Expr::UnaryOp(unary) if unary.op == ast::UnaryOp::USub => match literal(&unary.operand)? {
			Literal::Number(number) => {
				let number = number
					.as_i64()
					.map(|number| serde_json::Number::from(-number))
					.or_else(|| {
						number
							.as_f64()
							.and_then(|number| serde_json::Number::from_f64(-number))
					})?;
				Literal::Number(number)
			},
			_ => return None,
		},
		_ => return None,
	};
	Some(literal)
}
