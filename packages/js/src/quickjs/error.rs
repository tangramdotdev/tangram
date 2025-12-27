use {
	super::{State, StateHandle, serde::Serde},
	num::ToPrimitive as _,
	rquickjs::{self as qjs, FromJs as _, IntoJs as _, function::This},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

/// Convert an error to a JavaScript exception.
pub fn to_exception<'js>(ctx: &qjs::Ctx<'js>, error: &tg::Error) -> tg::Result<qjs::Value<'js>> {
	let globals = ctx.globals();

	// Get Tangram.Error constructor.
	let tangram = globals.get::<_, qjs::Object>("Tangram").unwrap();
	let error_constructor = tangram.get::<_, qjs::Object>("Error").unwrap();

	match error.to_data_or_id() {
		tg::Either::Left(data) => {
			let from_data = error_constructor
				.get::<_, qjs::Function>("fromData")
				.unwrap();
			let data_value = Serde(&data).into_js(ctx).map_err(|source| {
				tg::error!(!source, "failed to serialize error data to JavaScript")
			})?;
			let exception = from_data
				.call((data_value,))
				.map_err(|source| tg::error!(!source, "failed to call Tangram.Error.fromData"))?;
			Ok(exception)
		},
		tg::Either::Right(id) => {
			let with_id = error_constructor.get::<_, qjs::Function>("withId").unwrap();
			let id_value = Serde(&id).into_js(ctx).map_err(|source| {
				tg::error!(!source, "failed to serialize error ID to JavaScript")
			})?;
			let exception = with_id
				.call((id_value,))
				.map_err(|source| tg::error!(!source, "failed to call Tangram.Error.withId"))?;
			Ok(exception)
		},
	}
}

pub fn from_catch<'js>(
	state: &State,
	ctx: &qjs::Ctx<'js>,
	error: qjs::CaughtError<'js>,
) -> tg::Error {
	match error {
		qjs::CaughtError::Error(error) => tg::error!(!error, "a javascript error occurred"),

		qjs::CaughtError::Exception(exception) => {
			from_exception(state, ctx, &exception.into_value())
				.unwrap_or_else(|| tg::error!("unknown exception"))
		},

		qjs::CaughtError::Value(value) => from_exception(state, ctx, &value)
			.unwrap_or_else(|| tg::error!("unknown value exception")),
	}
}

#[expect(clippy::only_used_in_recursion)]
pub fn from_exception<'js>(
	state: &State,
	ctx: &qjs::Ctx<'js>,
	exception: &qjs::Value<'js>,
) -> Option<tg::Error> {
	let globals = ctx.globals();
	let tangram = globals.get::<_, qjs::Object>("Tangram").unwrap();
	let error_constructor = tangram.get::<_, qjs::Object>("Error").unwrap();

	// Check if this is a Tangram.Error instance.
	if let Some(obj) = exception.as_object()
		&& obj.is_instance_of(&error_constructor)
	{
		let to_data = error_constructor.get::<_, qjs::Function>("toData").unwrap();
		if let Ok(data) = to_data.call::<_, qjs::Value>((exception.clone(),))
			&& let Ok(data) = Serde::<tg::error::Data>::from_js(ctx, data)
			&& let Ok(error) = tg::error::Object::try_from_data(data.0)
		{
			return Some(tg::Error::with_object(error));
		}
		// Clear any pending exception from the failed toData call.
		let _ = ctx.catch();

		// Try to get error info from the internal state.
		let message = (|| {
			let state_obj = obj.get::<_, qjs::Object>("state").ok()?;
			let object = state_obj.get::<_, qjs::Object>("object").ok()?;
			let value = object.get::<_, qjs::Object>("value").ok()?;
			value.get::<_, String>("message").ok()
		})();

		let stack = (|| {
			let state_obj = obj.get::<_, qjs::Object>("state").ok()?;
			let object = state_obj.get::<_, qjs::Object>("object").ok()?;
			let value = object.get::<_, qjs::Object>("value").ok()?;
			let stack_array = value.get::<_, qjs::Array>("stack").ok()?;

			let mut locations = Vec::new();
			let location_ns = error_constructor.get::<_, qjs::Object>("Location").ok()?;
			let to_data_fn = location_ns.get::<_, qjs::Function>("toData").ok()?;
			for i in 0..stack_array.len() {
				if let Ok(item) = stack_array.get::<qjs::Value>(i)
					&& let Ok(data) = to_data_fn.call::<_, qjs::Value>((item,))
					&& let Ok(loc_data) = Serde::<tg::error::data::Location>::from_js(ctx, data)
					&& let Ok(loc) = tg::error::Location::try_from_data(loc_data.0)
				{
					locations.push(loc);
				}
			}
			if locations.is_empty() {
				None
			} else {
				Some(locations)
			}
		})();

		let location = stack.as_ref().and_then(|s| s.first().cloned());

		return Some(tg::Error::with_object(tg::error::Object {
			code: None,
			message,
			location,
			stack,
			source: None,
			values: BTreeMap::new(),
			diagnostics: None,
		}));
	}

	// Handle as a native JavaScript Error.
	let object = exception.as_object()?;

	// Get the message.
	let message = object.get::<_, String>("message").ok();

	// Get the stack - it should be an array of Location objects if prepareStackTrace worked.
	let stack = object
		.get::<_, qjs::Array>("stack")
		.ok()
		.and_then(|stack_array| {
			let location_namespace = error_constructor.get::<_, qjs::Object>("Location").ok()?;
			let to_data = location_namespace.get::<_, qjs::Function>("toData").ok()?;

			let mut locations = Vec::new();
			for i in 0..stack_array.len() {
				if let Ok(item) = stack_array.get::<qjs::Value>(i)
					&& let Ok(data) = to_data.call::<_, qjs::Value>((item,))
					&& let Ok(loc_data) = Serde::<tg::error::data::Location>::from_js(ctx, data)
					&& let Ok(loc) = tg::error::Location::try_from_data(loc_data.0)
				{
					locations.push(loc);
				}
			}
			if locations.is_empty() {
				None
			} else {
				Some(locations)
			}
		});

	// Get the location from the first stack frame.
	let location = stack.as_ref().and_then(|s| s.first().cloned());

	// Get the cause for the source.
	let source = object
		.get::<_, qjs::Value>("cause")
		.ok()
		.filter(|value| !value.is_null() && !value.is_undefined())
		.and_then(|cause| from_exception(state, ctx, &cause))
		.map(|error| {
			let item = error
				.to_data_or_id()
				.map_left(|data| {
					Box::new(tg::error::Object::try_from_data(data).unwrap_or_else(|_| {
						tg::error::Object {
							message: Some("invalid error".to_owned()),
							..Default::default()
						}
					}))
				})
				.map_right(|id| Box::new(tg::Error::with_id(id)));
			tg::Referent::with_item(item)
		});

	Some(tg::Error::with_object(tg::error::Object {
		code: None,
		message,
		location,
		stack,
		source,
		values: BTreeMap::new(),
		diagnostics: None,
	}))
}

#[expect(clippy::needless_pass_by_value)]
pub fn prepare_stack_trace<'js>(
	ctx: qjs::Ctx<'js>,
	_error: qjs::Value<'js>,
	call_sites: qjs::Array<'js>,
) -> qjs::Result<qjs::Array<'js>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let globals = ctx.globals();

	// Get Tangram.Error.Location.fromData.
	let tangram = globals.get::<_, qjs::Object>("Tangram").unwrap();
	let error_constructor = tangram.get::<_, qjs::Object>("Error").unwrap();
	let location_ns = error_constructor.get::<_, qjs::Object>("Location").unwrap();
	let location_from_data = location_ns.get::<_, qjs::Function>("fromData").unwrap();

	let length = call_sites.len();
	let stack = qjs::Array::new(ctx.clone())?;
	let mut stack_index = 0usize;

	// Process call sites in reverse order (bottom to top).
	for index in (0..length).rev() {
		let call_site: qjs::Object = call_sites.get(index)?;

		// Get function name - must call with call_site as `this`.
		let get_function_name: qjs::Function = call_site.get("getFunctionName")?;
		let function_name: qjs::Value = get_function_name.call((This(call_site.clone()),))?;

		// Get type name (optional - may not exist in QuickJS).
		let type_name: Option<qjs::Value> = call_site
			.get::<_, qjs::Function>("getTypeName")
			.ok()
			.and_then(|f| f.call::<_, qjs::Value>((This(call_site.clone()),)).ok());

		let symbol = if !function_name.is_null() && !function_name.is_undefined() {
			let function_name = function_name
				.as_string()
				.and_then(|s: &qjs::String| s.to_string().ok());
			if let Some(function_name) = function_name {
				if let Some(ref type_name) = type_name {
					if !type_name.is_null() && !type_name.is_undefined() {
						let type_name = type_name
							.as_string()
							.and_then(|s: &qjs::String| s.to_string().ok());
						if let Some(type_name) = type_name {
							Some(format!("{type_name}.{function_name}"))
						} else {
							Some(function_name)
						}
					} else {
						Some(function_name)
					}
				} else {
					Some(function_name)
				}
			} else {
				None
			}
		} else {
			None
		};

		// Get file name (which is a module name string in QuickJS).
		let get_file_name: qjs::Function = call_site.get("getFileName")?;
		let file_name: qjs::Value = get_file_name.call((This(call_site.clone()),))?;
		let file_name_str = if !file_name.is_null() && !file_name.is_undefined() {
			file_name
				.as_string()
				.and_then(|s: &qjs::String| s.to_string().ok())
		} else {
			None
		};

		// Get line number (1-based, convert to 0-based).
		let get_line_number: qjs::Function = call_site.get("getLineNumber")?;
		let line_number: qjs::Value = get_line_number.call((This(call_site.clone()),))?;
		let line_number = if !line_number.is_null() && !line_number.is_undefined() {
			line_number
				.as_int()
				.and_then(|i: i32| i.to_u32())
				.map(|n: u32| n.saturating_sub(1))
		} else {
			None
		};

		// Get column number.
		let get_column_number: qjs::Function = call_site.get("getColumnNumber")?;
		let column_number: qjs::Value = get_column_number.call((This(call_site.clone()),))?;
		let column_number = if !column_number.is_null() && !column_number.is_undefined() {
			column_number.as_int().and_then(|i: i32| i.to_u32())
		} else {
			None
		};

		if let Some(location) = get_location(
			&state,
			symbol,
			file_name_str.as_deref(),
			line_number,
			column_number,
			column_number,
		) {
			let data = Serde(location).into_js(&ctx)?;
			let location: qjs::Value = location_from_data.call((data,))?;
			stack.set(stack_index, location)?;
			stack_index += 1;
		}
	}

	Ok(stack)
}

fn get_location(
	state: &State,
	symbol: Option<String>,
	file_name: Option<&str>,
	line: Option<u32>,
	start_column: Option<u32>,
	end_column: Option<u32>,
) -> Option<tg::error::data::Location> {
	let file_name = file_name?;

	// If file_name is "main", use the global source map.
	if file_name == "main" {
		let line = line?;
		let start_column = start_column?;
		let end_column = end_column?;
		let global_source_map = state.global_source_map.as_ref()?;
		let start_token = global_source_map.lookup_token(line, start_column)?;
		let start_line = start_token.get_src_line();
		let start_column = start_token.get_src_col();
		let end_token = global_source_map.lookup_token(line, end_column)?;
		let end_line = end_token.get_src_line();
		let end_column = end_token.get_src_col();
		let symbol = start_token.get_name().map(String::from);
		let source = tg::error::data::File::Internal(start_token.get_source()?.parse().ok()?);
		let location = tg::error::data::Location {
			symbol,
			file: source,
			range: tg::Range {
				start: tg::Position {
					line: start_line,
					character: start_column,
				},
				end: tg::Position {
					line: end_line,
					character: end_column,
				},
			},
		};
		return Some(location);
	}

	// Handle "!" which is the root module.
	// Otherwise, file_name should be a module data string.
	let module_data = if file_name == "!" {
		state.root.clone()
	} else {
		file_name.parse().ok()?
	};

	// Find the module in state.modules by matching the module data.
	let modules = state.modules.borrow();
	let module = modules.iter().find(|m| m.module == module_data)?;

	// Get the source.
	let source = tg::error::data::File::Module(module.module.clone());

	// Get the line and column and apply a source map if one is available.
	let mut start_line = line?;
	let mut start_column = start_column?;
	let mut end_line = line?;
	let mut end_column = end_column?;
	if let Some(source_map) = module.source_map.as_ref() {
		if let Some(start_token) = source_map.lookup_token(start_line, start_column) {
			start_line = start_token.get_src_line();
			start_column = start_token.get_src_col();
		}
		if let Some(end_token) = source_map.lookup_token(end_line, end_column) {
			end_line = end_token.get_src_line();
			end_column = end_token.get_src_col();
		}
	}

	// Create the location.
	let location = tg::error::data::Location {
		symbol,
		file: source,
		range: tg::Range {
			start: tg::Position {
				line: start_line,
				character: start_column,
			},
			end: tg::Position {
				line: end_line,
				character: end_column,
			},
		},
	};

	Some(location)
}
