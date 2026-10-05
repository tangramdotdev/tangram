use {
	super::{Module, StateHandle},
	rquickjs::{self as qjs, IntoJs as _},
	sourcemap::SourceMap,
	tangram_client::prelude::*,
	tangram_quickjs::Serde,
};

pub struct Resolver;

impl qjs::loader::Resolver for Resolver {
	fn resolve(
		&mut self,
		ctx: &qjs::Ctx<'_>,
		base: &str,
		name: &str,
		attributes: Option<qjs::loader::ImportAttributes<'_>>,
	) -> qjs::Result<String> {
		// Get the state from the context's userdata.
		let state = ctx.userdata::<StateHandle>().unwrap().clone();

		// Get the referrer.
		let referrer = if base == "main" {
			None
		} else {
			let module = base
				.parse::<tg::module::Data>()
				.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;
			let module = state
				.modules
				.borrow()
				.iter()
				.find(|entry| entry.module.has_same_identity(&module))
				.map_or(module, |entry| entry.module.clone());
			Some(module)
		};

		// Parse the import attributes.
		let attributes = if let Some(attributes) = attributes {
			let mut map = std::collections::BTreeMap::new();
			for key in attributes.keys() {
				let key = key.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;
				if let Some(value) = attributes
					.get(&key)
					.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?
				{
					map.insert(key, value);
				}
			}
			Some(map)
		} else {
			None
		};

		// Parse the import specifier with attributes.
		let import = tg::module::Import::with_specifier_and_attributes(name, attributes)
			.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;

		// Resolve the module.
		let (sender, receiver) = std::sync::mpsc::channel();
		state.main_runtime_handle.spawn({
			let instance = state.instance.clone();
			let referrer = referrer.clone();
			let import = import.clone();
			async move {
				let arg = tg::module::resolve::Arg {
					referrer: referrer.clone(),
					import,
				};
				let result = instance
					.resolve_module(arg)
					.await
					.map(|output| output.module);
				sender.send(result).unwrap();
			}
		});
		let module = receiver
			.recv()
			.unwrap()
			.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;
		let name = module.without_token().to_string();
		let index = state
			.modules
			.borrow()
			.iter()
			.position(|entry| entry.module.has_same_identity(&module));
		if let Some(index) = index {
			state.modules.borrow_mut()[index]
				.module
				.referent
				.options
				.tokens
				.inherit(&module.referent.options.tokens);
		} else {
			state.modules.borrow_mut().push(Module {
				module,
				source_map: None,
			});
		}

		Ok(name)
	}
}

pub struct Loader;

impl qjs::loader::Loader for Loader {
	fn load<'javascript>(
		&mut self,
		ctx: &qjs::Ctx<'javascript>,
		name: &str,
		_attributes: Option<qjs::loader::ImportAttributes<'javascript>>,
	) -> qjs::Result<qjs::Module<'javascript>> {
		let state = ctx.userdata::<StateHandle>().unwrap().clone();

		let module = name
			.parse::<tg::module::Data>()
			.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;
		let mut module_data = state
			.modules
			.borrow()
			.iter()
			.find(|entry| entry.module.has_same_identity(&module))
			.map_or(module, |entry| entry.module.clone());

		// Load the module.
		let (sender, receiver) = std::sync::mpsc::channel();
		state.main_runtime_handle.spawn({
			let instance = state.instance.clone();
			let module = module_data.clone();
			async move {
				let arg = tg::module::load::Arg {
					language: Some(tg::module::load::Language::JavaScript),
					module,
				};
				let result = instance.load_module(arg).await;
				sender.send(result).unwrap();
			}
		});
		let loaded = receiver
			.recv()
			.unwrap()
			.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;

		module_data.referent.options.tokens.inherit(&loaded.tokens);

		// Transpile the module.
		let output = tangram_compiler::Compiler::transpile(&loaded.text, &module_data);
		if !output.diagnostics.is_empty() {
			return Err(qjs::Error::Io(std::io::Error::other(tg::error!(
				"failed to transpile module"
			))));
		}

		// Parse the source map.
		let source_map = SourceMap::from_slice(output.source_map.as_bytes()).ok();

		// Register the module.
		let index = state
			.modules
			.borrow()
			.iter()
			.position(|entry| entry.module.has_same_identity(&module_data));
		if let Some(index) = index {
			let mut modules = state.modules.borrow_mut();
			modules[index]
				.module
				.referent
				.options
				.tokens
				.inherit(&loaded.tokens);
			modules[index].source_map = source_map;
		} else {
			state.modules.borrow_mut().push(Module {
				module: module_data.clone(),
				source_map,
			});
		}

		// Compile the module. Use the module name as the identifier.
		let module = qjs::Module::declare(ctx.clone(), name, output.text)?;

		// Set import.meta.module.
		let module_value = Serde(&module_data)
			.into_js(ctx)
			.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;
		let globals = ctx.globals();
		let from_data_function = globals
			.get::<_, qjs::Object>("Tangram")
			.unwrap()
			.get::<_, qjs::Object>("Module")
			.unwrap()
			.get::<_, qjs::Function>("fromData")
			.unwrap();
		let module_value = from_data_function
			.call::<_, qjs::Value>((module_value,))
			.unwrap();
		let meta = module.meta()?;
		meta.set("module", module_value).unwrap();

		Ok(module)
	}
}
