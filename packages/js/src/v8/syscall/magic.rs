use {
	super::State,
	std::rc::Rc,
	tangram_client::prelude::*,
	tangram_v8::{Serde, Serialize as _},
};

pub fn magic<'s>(
	scope: &mut v8::HandleScope<'s>,
	args: &v8::FunctionCallbackArguments,
) -> tg::Result<v8::Local<'s, v8::Value>> {
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<Rc<State>>().unwrap().clone();

	// Get the function.
	let arg = args.get(1);
	let function = v8::Local::<v8::Function>::try_from(arg)
		.ok()
		.ok_or_else(|| tg::error!("expected a function"))?;

	// Get the module.
	let modules = state.modules.borrow();
	let mut module = None;
	for module_ in modules.iter() {
		if let Some(v8_module) = &module_.v8 {
			let v8_module = v8::Local::new(scope, v8_module);
			if v8_module.script_id() == Some(function.get_script_origin().script_id()) {
				module = Some(module_.module.clone());
			}
		}
	}
	let module = module.ok_or_else(|| tg::error!("failed to find the module for the function"))?;

	// Get the export.
	let export = Some(function.get_name(scope).to_rust_string_lossy(scope));

	// Create the executable.
	let executable = tg::command::data::Executable::Module(tg::command::data::ModuleExecutable {
		module,
		export,
	});

	let value = Serde(executable).serialize(scope)?;

	Ok(value)
}
