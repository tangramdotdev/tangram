use {
	super::State,
	num::ToPrimitive as _,
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_v8::{Deserialize as _, Serde, Serialize as _},
};

pub(super) fn to_exception<'s>(
	scope: &mut v8::PinScope<'s, '_>,
	error: &tg::Error,
) -> Option<v8::Local<'s, v8::Value>> {
	let context = scope.get_current_context();
	let global = context.global(scope);
	let tangram = v8::String::new_external_onebyte_static(scope, b"Tangram").unwrap();
	let tangram = global.get(scope, tangram.into()).unwrap();
	let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

	let error_constructor = v8::String::new_external_onebyte_static(scope, b"Error").unwrap();
	let error_constructor = tangram.get(scope, error_constructor.into()).unwrap();
	let error_constructor = v8::Local::<v8::Object>::try_from(error_constructor).unwrap();

	match error.to_data_or_id() {
		tg::Either::Left(data) => {
			let from_data = v8::String::new_external_onebyte_static(scope, b"fromData").unwrap();
			let from_data = error_constructor.get(scope, from_data.into()).unwrap();
			let from_data = v8::Local::<v8::Function>::try_from(from_data).unwrap();

			let data = Serde(data).serialize(scope).unwrap();
			let undefined = v8::undefined(scope);
			let exception = from_data.call(scope, undefined.into(), &[data])?;
			Some(exception)
		},
		tg::Either::Right(id) => {
			let with_id = v8::String::new_external_onebyte_static(scope, b"withId").unwrap();
			let with_id = error_constructor.get(scope, with_id.into()).unwrap();
			let with_id = v8::Local::<v8::Function>::try_from(with_id).unwrap();

			let id = Serde(id).serialize(scope).unwrap();
			let undefined = v8::undefined(scope);
			let exception = with_id.call(scope, undefined.into(), &[id])?;
			Some(exception)
		},
	}
}

pub(super) fn from_exception<'s>(
	state: &State,
	scope: &mut v8::PinScope<'s, '_>,
	exception: v8::Local<'s, v8::Value>,
) -> Option<tg::Error> {
	let context = scope.get_current_context();
	let global = context.global(scope);
	let tangram = v8::String::new_external_onebyte_static(scope, b"Tangram").unwrap();
	let tangram = global.get(scope, tangram.into()).unwrap();
	let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

	let error_constructor = v8::String::new_external_onebyte_static(scope, b"Error").unwrap();
	let error_constructor = tangram.get(scope, error_constructor.into()).unwrap();
	let error_constructor = v8::Local::<v8::Function>::try_from(error_constructor).unwrap();

	if exception.instance_of(scope, error_constructor.into())? {
		let to_data = v8::String::new_external_onebyte_static(scope, b"toData").unwrap();
		let to_data = error_constructor.get(scope, to_data.into()).unwrap();
		let to_data = v8::Local::<v8::Function>::try_from(to_data).unwrap();

		let undefined = v8::undefined(scope);
		let data = to_data.call(scope, undefined.into(), &[exception])?;

		let error = Serde::<tg::error::Data>::deserialize(scope, data)
			.and_then(|data| data.0.try_into())
			.unwrap();

		return Some(error);
	}

	let v8_message = v8::Exception::create_message(scope, exception);

	// Get the location.
	let id = v8_message
		.get_script_resource_name(scope)
		.and_then(|resource_name| <v8::Local<v8::Integer>>::try_from(resource_name).ok())
		.map(|resource_name| resource_name.value().to_usize().unwrap());
	let line = if id.is_some() {
		Some(v8_message.get_line_number(scope).unwrap().to_u32().unwrap() - 1)
	} else {
		None
	};
	let start_column = if id.is_some() {
		Some(v8_message.get_start_column().to_u32().unwrap())
	} else {
		None
	};
	let end_column = if id.is_some() {
		Some(v8_message.get_end_column().to_u32().unwrap())
	} else {
		None
	};
	let location = get_location(state, None, id, line, start_column, end_column)
		.and_then(|location| location.try_into().ok());

	// Get the message.
	let message = Some(v8_message.get(scope).to_rust_string_lossy(scope));

	// Get the source.
	let cause_string = v8::String::new_external_onebyte_static(scope, b"cause").unwrap();
	let source = if let Some(source) = exception
		.is_native_error()
		.then(|| exception.to_object(scope).unwrap())
		.and_then(|exception| exception.get(scope, cause_string.into()))
		.and_then(|value| value.to_object(scope))
	{
		let item = from_exception(state, scope, source.into())?
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
		let referent = tg::Referent::with_item(item);
		Some(referent)
	} else {
		None
	};

	// Get the stack.
	let stack = v8::String::new_external_onebyte_static(scope, b"stack").unwrap();
	let stack = if let Some(stack) = exception
		.is_native_error()
		.then(|| exception.to_object(scope).unwrap())
		.and_then(|exception| exception.get(scope, stack.into()))
	{
		let location_namespace =
			v8::String::new_external_onebyte_static(scope, b"Location").unwrap();
		let location_namespace = error_constructor
			.get(scope, location_namespace.into())
			.unwrap();
		let location_namespace = v8::Local::<v8::Object>::try_from(location_namespace).unwrap();
		let to_data = v8::String::new_external_onebyte_static(scope, b"toData").unwrap();
		let to_data = location_namespace.get(scope, to_data.into()).unwrap();
		let to_data = v8::Local::<v8::Function>::try_from(to_data).unwrap();
		let value = v8::Local::<v8::Array>::try_from(stack).unwrap();
		let len = value.length().to_usize().unwrap();
		let mut output = Vec::with_capacity(len);
		for i in 0..len {
			let value = value.get_index(scope, i.to_u32().unwrap()).unwrap();
			let undefined = v8::undefined(scope);
			let data = to_data.call(scope, undefined.into(), &[value])?;
			let data = Serde::<tg::error::data::Location>::deserialize(scope, data).unwrap();
			let data = data.0.try_into().unwrap();
			output.push(data);
		}
		Some(output)
	} else {
		None
	};

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

pub fn prepare_stack_trace_callback<'s>(
	scope: &mut v8::PinScope<'s, '_>,
	_error: v8::Local<v8::Value>,
	call_sites: v8::Local<v8::Array>,
) -> v8::Local<'s, v8::Value> {
	let context = scope.get_current_context();
	let state = context.get_slot::<State>().unwrap().clone();
	let global = context.global(scope);

	// Get Tangram.Error.Location.fromData
	let tangram = v8::String::new_external_onebyte_static(scope, b"Tangram").unwrap();
	let tangram = global.get(scope, tangram.into()).unwrap();
	let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

	let error_constructor = v8::String::new_external_onebyte_static(scope, b"Error").unwrap();
	let error_constructor = tangram.get(scope, error_constructor.into()).unwrap();
	let error_constructor = v8::Local::<v8::Object>::try_from(error_constructor).unwrap();

	let location_namespace = v8::String::new_external_onebyte_static(scope, b"Location").unwrap();
	let location_namespace = error_constructor
		.get(scope, location_namespace.into())
		.unwrap();
	let location_namespace = v8::Local::<v8::Object>::try_from(location_namespace).unwrap();

	let location_from_data = v8::String::new_external_onebyte_static(scope, b"fromData").unwrap();
	let location_from_data = location_namespace
		.get(scope, location_from_data.into())
		.unwrap();
	let location_from_data = v8::Local::<v8::Function>::try_from(location_from_data).unwrap();

	let length = call_sites.length();
	let stack = v8::Array::new(scope, 0);
	let mut stack_index = 0;

	for index in (0..length).rev() {
		let call_site = call_sites.get_index(scope, index).unwrap();
		let call_site = v8::Local::<v8::Object>::try_from(call_site).unwrap();

		let get_function_name =
			v8::String::new_external_onebyte_static(scope, b"getFunctionName").unwrap();
		let get_function_name = call_site.get(scope, get_function_name.into()).unwrap();
		let get_function_name = v8::Local::<v8::Function>::try_from(get_function_name).unwrap();
		let function_name = get_function_name
			.call(scope, call_site.into(), &[])
			.unwrap();

		let get_type_name = v8::String::new_external_onebyte_static(scope, b"getTypeName").unwrap();
		let get_type_name = call_site.get(scope, get_type_name.into()).unwrap();
		let get_type_name = v8::Local::<v8::Function>::try_from(get_type_name).unwrap();
		let type_name = get_type_name.call(scope, call_site.into(), &[]).unwrap();
		let symbol = if !function_name.is_null() && !function_name.is_undefined() {
			let function_name = function_name.to_rust_string_lossy(scope);
			if !type_name.is_null() && !type_name.is_undefined() {
				let type_name = type_name.to_rust_string_lossy(scope);
				Some(format!("{type_name}.{function_name}"))
			} else {
				Some(function_name)
			}
		} else {
			None
		};

		let get_file_name = v8::String::new_external_onebyte_static(scope, b"getFileName").unwrap();
		let get_file_name = call_site.get(scope, get_file_name.into()).unwrap();
		let get_file_name = v8::Local::<v8::Function>::try_from(get_file_name).unwrap();
		let file_name = get_file_name.call(scope, call_site.into(), &[]).unwrap();
		let file_name = if !file_name.is_null() && !file_name.is_undefined() {
			file_name
				.to_integer(scope)
				.map(|i| i.value().to_usize().unwrap())
		} else {
			None
		};

		let get_line_number =
			v8::String::new_external_onebyte_static(scope, b"getLineNumber").unwrap();
		let get_line_number = call_site.get(scope, get_line_number.into()).unwrap();
		let get_line_number = v8::Local::<v8::Function>::try_from(get_line_number).unwrap();
		let line_number = get_line_number.call(scope, call_site.into(), &[]).unwrap();
		let line_number = if !line_number.is_null() && !line_number.is_undefined() {
			line_number
				.to_integer(scope)
				.map(|i| i.value().to_u32().unwrap() - 1)
		} else {
			None
		};

		let get_column_number =
			v8::String::new_external_onebyte_static(scope, b"getColumnNumber").unwrap();
		let get_column_number = call_site.get(scope, get_column_number.into()).unwrap();
		let get_column_number = v8::Local::<v8::Function>::try_from(get_column_number).unwrap();
		let column_number = get_column_number
			.call(scope, call_site.into(), &[])
			.unwrap();
		let column_number = if !column_number.is_null() && !column_number.is_undefined() {
			column_number
				.to_integer(scope)
				.map(|i| i.value().to_u32().unwrap())
		} else {
			None
		};

		if let Some(location) = get_location(
			state.as_ref(),
			symbol,
			file_name,
			line_number,
			column_number,
			column_number,
		) {
			let data = Serde(location).serialize(scope).unwrap();
			let undefined = v8::undefined(scope);
			let Some(location) = location_from_data.call(scope, undefined.into(), &[data]) else {
				return v8::undefined(scope).into();
			};
			stack.set_index(scope, stack_index, location);
			stack_index += 1;
		}
	}

	stack.into()
}

fn get_location(
	state: &State,
	symbol: Option<String>,
	id: Option<usize>,
	line: Option<u32>,
	start_column: Option<u32>,
	end_column: Option<u32>,
) -> Option<tg::error::data::Location> {
	match id {
		Some(0) => {
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
			let source =
				tg::error::data::File::Internal(start_token.get_source().unwrap().parse().unwrap());
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
		},

		Some(id) => {
			// Get the module.
			let modules = state.modules.borrow();
			let module = modules.get(id - 1)?;

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
		},

		None => None,
	}
}
