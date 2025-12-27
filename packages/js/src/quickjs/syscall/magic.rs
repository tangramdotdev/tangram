use {
	super::Result,
	crate::quickjs::{StateHandle, serde::Serde},
	rquickjs as qjs,
	tangram_client::prelude::*,
};

pub fn magic<'js>(
	ctx: qjs::Ctx<'js>,
	function: qjs::Function<'js>,
) -> Result<Serde<tg::command::data::Executable>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();

	// Get the function name.
	let name: Option<String> = function.get("name").ok();

	// Try to find the module this function belongs to.
	let file_name: Option<String> = function.get("fileName").ok();

	// Find the module in our modules list.
	let modules = state.modules.borrow();
	let mut found_module = None;

	if let Some(ref file) = file_name {
		// Try to match against loaded modules.
		for module_info in modules.iter() {
			let module_id = serde_json::to_string(&module_info.module).unwrap_or_default();
			if file == &module_id || file.contains(&module_id) || file == "!" {
				found_module = Some(module_info.module.clone());
				break;
			}
		}
	}

	// If no module is found, then use the root module.
	let module = found_module.unwrap_or_else(|| state.root.clone());

	// Create the executable.
	let executable = tg::command::data::Executable::Module(tg::command::data::ModuleExecutable {
		module,
		export: name,
	});

	Result(Ok(Serde(executable)))
}
