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
				.module_index(&module)
				.map_or(module, |index| state.modules.borrow()[index].module.clone());
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

		// Get the module that a preload resolved the import to. QuickJS has no asynchronous hook for dynamic imports, so block on a preload of an import that has not been preloaded.
		let resolution = state
			.preloaded
			.borrow()
			.resolution(referrer.as_ref(), &import);
		let module = if let Some(module) = resolution {
			module
		} else {
			let arg = crate::module::Arg {
				graphs: state.graphs.clone(),
				import,
				instance: state.instance.clone(),
				known: state.preloaded.borrow().known(),
				referrer,
			};
			let (sender, receiver) = std::sync::mpsc::channel();
			state.main_runtime_handle.spawn(async move {
				sender.send(crate::module::preload(arg).await).unwrap();
			});
			let output = receiver
				.recv()
				.unwrap()
				.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;
			state.preloaded.borrow_mut().extend(output.modules);
			output.module
		};
		let name = module.without_token().to_string();
		let index = state.module_index(&module);
		if let Some(index) = index {
			state.modules.borrow_mut()[index]
				.module
				.referent
				.options
				.tokens
				.inherit(&module.referent.options.tokens);
		} else {
			state.push_module(Module {
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
			.module_index(&module)
			.map_or(module, |index| state.modules.borrow()[index].module.clone());

		// Get the preloaded module.
		let entry = state
			.preloaded
			.borrow()
			.entry(&module_data)
			.ok_or_else(|| {
				let error = tg::error!(
					module = ?module_data.without_token(),
					"failed to find the preloaded module"
				);
				qjs::Error::Io(std::io::Error::other(error))
			})?;
		let tokens = &entry.module.referent.options.tokens;
		module_data.referent.options.tokens.inherit(tokens);

		// Parse the source map.
		let source_map = SourceMap::from_slice(entry.source_map.as_bytes()).ok();

		// Register the module.
		let index = state.module_index(&module_data);
		if let Some(index) = index {
			let mut modules = state.modules.borrow_mut();
			modules[index]
				.module
				.referent
				.options
				.tokens
				.inherit(tokens);
			modules[index].source_map = source_map;
		} else {
			state.push_module(Module {
				module: module_data.clone(),
				source_map,
			});
		}

		// Compile the module. Use the module name as the identifier.
		let module = qjs::Module::declare(ctx.clone(), name, entry.text.clone())?;

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
