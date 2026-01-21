use {
	super::{Module, StateHandle, serde::Serde},
	rquickjs::{self as qjs, IntoJs as _},
	sourcemap::SourceMap,
	tangram_client::prelude::*,
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
		// Handle the root module.
		if name == "!" {
			return Ok("!".to_string());
		}

		// Get the state from the context's userdata.
		let state = ctx.userdata::<StateHandle>().unwrap().clone();

		// Get the referrer.
		let referrer = if base.is_empty() || base == "main" || base == "!" {
			state.root.clone()
		} else {
			base.parse::<tg::module::Data>()
				.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?
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
			let handle = state.handle.clone();
			let referrer = referrer.clone();
			let import = import.clone();
			async move {
				let arg = tg::module::resolve::Arg { referrer, import };
				let result = handle.resolve_module(arg).await.map(|output| output.module);
				sender.send(result).unwrap();
			}
		});
		let module = receiver
			.recv()
			.unwrap()
			.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;

		Ok(module.to_string())
	}
}

pub struct Loader;

impl qjs::loader::Loader for Loader {
	fn load<'js>(
		&mut self,
		ctx: &qjs::Ctx<'js>,
		name: &str,
		_attributes: Option<qjs::loader::ImportAttributes<'js>>,
	) -> qjs::Result<qjs::Module<'js>> {
		let state = ctx.userdata::<StateHandle>().unwrap().clone();

		let module_data = if name == "!" {
			state.root.clone()
		} else {
			name.parse()
				.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?
		};

		// Load the module.
		let (sender, receiver) = std::sync::mpsc::channel();
		state.main_runtime_handle.spawn({
			let handle = state.handle.clone();
			let module = module_data.clone();
			async move {
				let arg = tg::module::load::Arg { module };
				let result = handle.load_module(arg).await.map(|output| output.text);
				sender.send(result).unwrap();
			}
		});
		let source = receiver
			.recv()
			.unwrap()
			.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;

		// Transpile the module.
		let output = tangram_compiler::Compiler::transpile(&source, &module_data);
		if !output.diagnostics.is_empty() {
			return Err(qjs::Error::Io(std::io::Error::other(tg::error!(
				"failed to transpile module"
			))));
		}

		// Parse the source map.
		let source_map = SourceMap::from_slice(output.source_map.as_bytes()).ok();

		// Register the module.
		state.modules.borrow_mut().push(Module {
			module: module_data,
			source_map,
		});

		// Compile the module. Use the module name as the identifier.
		let module = qjs::Module::declare(ctx.clone(), name, output.text)?;

		// Set import.meta.module.
		let Module {
			module: module_data,
			..
		} = &state.modules.borrow()[state.modules.borrow().len() - 1];
		let module_value = Serde(module_data)
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
