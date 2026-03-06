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
	let module = file_name
		.as_deref()
		.and_then(|file_name| {
			if file_name == "!" {
				Some(state.root.clone())
			} else {
				file_name.parse::<tg::module::Data>().ok().or_else(|| {
					let modules = state.modules.borrow();
					modules.iter().find_map(|module_info| {
						let name = module_info.module.to_string();
						if file_name == name || file_name.contains(&name) {
							Some(module_info.module.clone())
						} else {
							None
						}
					})
				})
			}
		})
		.unwrap_or_else(|| state.root.clone());

	// Create the executable.
	let executable = tg::command::data::Executable::Module(tg::command::data::ModuleExecutable {
		module,
		export: name,
	});

	Result(Ok(Serde(executable)))
}
