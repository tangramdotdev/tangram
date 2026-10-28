use {
	super::Database,
	ruff_db::files::File,
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	ty_python_core::Program,
	ty_python_semantic::{
		ProgramEnvironment,
		types::{
			KnownClass, Type,
			describe::{self, Description, Kind, Literal},
		},
	},
};

/// The maximum depth of nested types to convert.
const DEPTH: usize = 32;

struct Converter<'a, 'db> {
	database: &'db Database,
	definitions: BTreeMap<String, tg::interface::Type>,
	depth: usize,
	environment: &'a ProgramEnvironment<'db>,
	names: BTreeMap<(Option<File>, String), String>,
}

impl Database {
	/// Get the interface of a module, giving each export the type that the checker infers for it.
	pub(in crate::python) fn interface(
		&self,
		module: tg::module::Data,
	) -> tg::Result<tg::Interface> {
		// Discover the exports from the module's source.
		let entry = self.register(module)?;
		let mut interface = crate::interface::module(&entry.module, &entry.text)?;

		// Get the module's file and its environment.
		let program = Program::from_settings(self, &self.settings);
		let program_file = program.program_file(self, entry.file);
		let environment = ProgramEnvironment::from_file(program_file);
		let mut converter = Converter {
			database: self,
			definitions: BTreeMap::new(),
			depth: 0,
			environment: &environment,
			names: BTreeMap::new(),
		};

		// Give each export its type.
		for (name, export) in &mut interface.exports {
			let type_ = describe::module_member(self, program_file, name)
				.ok_or_else(|| tg::error!(%name, "failed to find the export"))?;
			export.type_ = converter
				.export(type_)
				.map_err(|error| tg::error!(!error, %name, "failed to get the type of the export"))?;
		}
		interface.definitions = converter.definitions;

		Ok(interface)
	}
}

impl<'db> Converter<'_, 'db> {
	fn describe(&self, type_: Type<'db>) -> Description<'db> {
		describe::describe(self.database, self.environment, type_)
	}

	/// Get the type of the command that runs an export, which calls a function and returns any other value.
	fn export(&mut self, type_: Type<'db>) -> tg::Result<tg::interface::Type> {
		// Get the type of a command that returns a value.
		let Description::Callable(signatures) = self.describe(type_) else {
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
		};

		// Get the type of a command that calls a function.
		let [signature] = signatures.as_slice() else {
			return Err(tg::error!("an overloaded function cannot be exported"));
		};
		let mut elements = Vec::new();
		let mut rest = None;
		for parameter in &signature.parameters {
			let name = parameter.name.as_deref().unwrap_or_default();
			match parameter.kind {
				Kind::PositionalOnly | Kind::PositionalOrKeyword => {
					if rest.is_some() {
						return Err(tg::error!(%name, "a parameter cannot follow a variadic parameter"));
					}
					let type_ = self.type_(parameter.ty).map_err(|error| {
						tg::error!(!error, %name, "failed to get the type of the parameter")
					})?;
					let optional = parameter.has_default;
					elements.push(tg::interface::Element { optional, type_ });
				},
				Kind::Variadic => {
					let type_ = self.type_(parameter.ty).map_err(|error| {
						tg::error!(!error, %name, "failed to get the type of the parameter")
					})?;
					rest = Some(Box::new(type_));
				},
				Kind::KeywordOnly | Kind::KeywordVariadic if parameter.has_default || parameter.kind == Kind::KeywordVariadic => {},
				Kind::KeywordOnly | Kind::KeywordVariadic => {
					return Err(tg::error!(%name, "a required keyword-only parameter cannot be exported"));
				},
			}
		}
		let args = tg::interface::Type::Tuple { elements, rest };
		let output = self
			.output(signature.return_ty)
			.map_err(|error| tg::error!(!error, "failed to get the type of the output"))?;
		let type_ = tg::interface::Type::Command {
			args: Box::new(args),
			output: Box::new(output),
		};

		Ok(type_)
	}

	/// Convert the return type of a function, awaiting the coroutine of an async function.
	fn output(&mut self, type_: Type<'db>) -> tg::Result<tg::interface::Type> {
		if let Description::Instance(instance) = self.describe(type_)
			&& instance.known == Some(KnownClass::CoroutineType)
			&& let Some(output) = instance.arguments.get(2)
		{
			return self.type_(*output);
		}
		self.type_(type_)
	}

	fn type_(&mut self, type_: Type<'db>) -> tg::Result<tg::interface::Type> {
		if self.depth >= DEPTH {
			return Err(tg::error!("the type is too deeply nested"));
		}
		self.depth += 1;
		let output = self.type_inner(type_);
		self.depth -= 1;
		output
	}

	fn type_inner(&mut self, type_: Type<'db>) -> tg::Result<tg::interface::Type> {
		match self.describe(type_) {
			Description::Dynamic => Ok(tg::interface::Type::Value),
			Description::Never => Ok(tg::interface::Type::Union { types: Vec::new() }),
			Description::Alias { name, file, value } => {
				if self.is_library(file) && matches!(name.as_str(), "Type" | "ValueType") {
					return Ok(tg::interface::Type::Value);
				}
				self.type_(value)
			},
			Description::Literal(literal) => {
				let value = match literal {
					Literal::Bool(value) => tg::interface::Literal::Boolean(value),
					#[expect(clippy::cast_precision_loss)]
					Literal::Int(value) => tg::interface::Literal::Number(value as f64),
					Literal::String(value) => tg::interface::Literal::String(value),
					Literal::Other => return Err(self.error(type_)),
				};
				Ok(tg::interface::Type::Literal { value })
			},
			Description::Instance(instance) => {
				match instance.known {
					Some(KnownClass::NoneType) => return Ok(tg::interface::Type::Null),
					Some(KnownClass::Bool) => return Ok(tg::interface::Type::Boolean),
					Some(KnownClass::Int | KnownClass::Float) => {
						return Ok(tg::interface::Type::Number);
					},
					Some(KnownClass::Str) => return Ok(tg::interface::Type::String),
					Some(KnownClass::Bytes) => return Ok(tg::interface::Type::Bytes),
					Some(KnownClass::Object) => return Ok(tg::interface::Type::Value),
					Some(KnownClass::List) => {
						let item = instance
							.arguments
							.first()
							.map(|item| self.type_(*item))
							.transpose()?
							.unwrap_or(tg::interface::Type::Value);
						return Ok(tg::interface::Type::Array {
							item: Box::new(item),
						});
					},
					Some(KnownClass::Dict) => {
						let key = instance.arguments.first().copied();
						if let Some(key) = key
							&& !matches!(
								self.type_(key)?,
								tg::interface::Type::String | tg::interface::Type::Value
							) {
							return Err(tg::error!("the keys of a map must be strings"));
						}
						let value = instance
							.arguments
							.get(1)
							.map(|value| self.type_(*value))
							.transpose()?
							.unwrap_or(tg::interface::Type::Value);
						return Ok(tg::interface::Type::Map {
							value: Box::new(value),
						});
					},
					_ => (),
				}
				if !self.is_library(instance.file) {
					return Err(self.error(type_));
				}
				let output = match instance.name.as_str() {
					"Artifact" => tg::interface::Type::Artifact,
					"Blob" => tg::interface::Type::Blob,
					"Command" => {
						let args = instance
							.arguments
							.first()
							.map(|args| self.type_(*args))
							.transpose()?
							.unwrap_or(tg::interface::Type::Array {
								item: Box::new(tg::interface::Type::Value),
							});
						let output = instance
							.arguments
							.get(1)
							.map(|output| self.type_(*output))
							.transpose()?
							.unwrap_or(tg::interface::Type::Value);
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
					_ => return Err(self.error(type_)),
				};
				Ok(output)
			},
			Description::Tuple {
				prefix,
				variable,
				suffix,
			} => {
				if !suffix.is_empty() {
					return Err(tg::error!("a tuple element cannot follow a variadic element"));
				}
				let elements = prefix
					.into_iter()
					.map(|element| {
						let type_ = self.type_(element)?;
						let element = tg::interface::Element {
							optional: false,
							type_,
						};
						Ok::<_, tg::Error>(element)
					})
					.collect::<tg::Result<_>>()?;
				let rest = variable
					.map(|variable| self.type_(variable))
					.transpose()?
					.map(Box::new);
				Ok(tg::interface::Type::Tuple { elements, rest })
			},
			Description::TypedDict(typed_dict) => {
				// Name the typed dict with a definition, so that a recursive type refers to itself.
				let key = typed_dict.name.clone().map(|name| (typed_dict.file, name));
				if let Some(key) = &key
					&& let Some(name) = self.names.get(key)
				{
					return Ok(tg::interface::Type::Reference { name: name.clone() });
				}
				let name = key.as_ref().map(|key| {
					let mut name = key.1.clone();
					let mut suffix = 1;
					while self.names.values().any(|other| *other == name) {
						suffix += 1;
						name = format!("{}{suffix}", key.1);
					}
					self.names.insert(key.clone(), name.clone());
					name
				});
				let mut fields = BTreeMap::new();
				for field in typed_dict.fields {
					let type_ = self.type_(field.ty).map_err(|error| {
						tg::error!(!error, name = %field.name, "failed to get the type of the field")
					})?;
					let optional = !field.required;
					fields.insert(field.name, tg::interface::Field { optional, type_ });
				}
				let record = tg::interface::Type::Record { fields };
				let Some(name) = name else {
					return Ok(record);
				};
				self.definitions.insert(name.clone(), record);
				Ok(tg::interface::Type::Reference { name })
			},
			Description::Union(members) => {
				let mut types = Vec::new();
				for member in members {
					let member = self.type_(member)?;
					if !types.contains(&member) {
						types.push(member);
					}
				}
				if types.contains(&tg::interface::Type::Value) {
					return Ok(tg::interface::Type::Value);
				}
				if let [type_] = types.as_slice() {
					return Ok(type_.clone());
				}
				Ok(tg::interface::Type::Union { types })
			},
			Description::Callable(_) => Err(tg::error!(
				type = %type_.display(self.database, self.environment),
				"a function is not a value"
			)),
			Description::Other => Err(self.error(type_)),
		}
	}

	/// Determine whether a file belongs to the Tangram Python client library.
	fn is_library(&self, file: impl Into<Option<File>>) -> bool {
		file.into().is_some_and(|file| self.database.is_runtime(file))
	}

	fn error(&self, type_: Type<'db>) -> tg::Error {
		let type_ = type_.display(self.database, self.environment).to_string();
		tg::error!(%type_, "the type is not a value")
	}
}
