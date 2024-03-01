use super::{
	convert::{from_v8, ToV8},
	State,
};
use num::ToPrimitive;
use std::{str::FromStr, sync::Arc};
use tangram_client as tg;
use tangram_error::Error;

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct V8StackTrace {
	call_sites: Vec<V8CallSite>,
}

#[allow(dead_code, clippy::struct_excessive_bools)]
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct V8CallSite {
	type_name: Option<String>,
	function_name: Option<String>,
	method_name: Option<String>,
	file_name: Option<String>,
	line_number: Option<u32>,
	column_number: Option<u32>,
	is_eval: bool,
	is_native: bool,
	is_constructor: bool,
	is_async: bool,
	is_promise_all: bool,
	// is_promise_any: bool,
	promise_index: Option<u32>,
}

pub(super) fn to_exception<'s>(
	scope: &mut v8::HandleScope<'s>,
	error: &Error,
) -> v8::Local<'s, v8::Value> {
	error.to_v8(scope).unwrap()
}

pub(super) fn from_exception<'s>(
	state: &State,
	scope: &mut v8::HandleScope<'s>,
	exception: v8::Local<'s, v8::Value>,
) -> Error {
	let context = scope.get_current_context();
	let global = context.global(scope);
	let tg = v8::String::new_external_onebyte_static(scope, "tg".as_bytes()).unwrap();
	let tg = global.get(scope, tg.into()).unwrap();
	let tg = v8::Local::<v8::Object>::try_from(tg).unwrap();

	let error = v8::String::new_external_onebyte_static(scope, "Error".as_bytes()).unwrap();
	let error = tg.get(scope, error.into()).unwrap();
	let error = v8::Local::<v8::Function>::try_from(error).unwrap();

	if exception
		.instance_of(scope, error.into())
		.unwrap_or_default()
	{
		return from_v8(scope, exception).unwrap();
	}

	// Get the message.
	let message_ = v8::Exception::create_message(scope, exception);
	let message = message_.get(scope).to_rust_string_lossy(scope);

	// Get the location.
	let resource_name = message_
		.get_script_resource_name(scope)
		.and_then(|resource_name| <v8::Local<v8::String>>::try_from(resource_name).ok())
		.map(|resource_name| resource_name.to_rust_string_lossy(scope));
	let line = if resource_name.is_some() {
		Some(message_.get_line_number(scope).unwrap().to_u32().unwrap() - 1)
	} else {
		None
	};
	let column = if resource_name.is_some() {
		Some(message_.get_start_column().to_u32().unwrap())
	} else {
		None
	};
	let location = get_location(state, resource_name.as_deref(), line, column);

	// Get the stack trace.
	let stack = v8::String::new_external_onebyte_static(scope, "stack".as_bytes()).unwrap();
	let stack = if let Some(stack) = exception
		.is_native_error()
		.then(|| exception.to_object(scope).unwrap())
		.and_then(|exception| exception.get(scope, stack.into()))
		.and_then(|value| serde_v8::from_v8::<V8StackTrace>(scope, value).ok())
	{
		let stack = stack
			.call_sites
			.iter()
			.rev()
			.filter_map(|call_site| {
				let file_name = call_site.file_name.as_deref();
				let line: u32 = call_site.line_number? - 1;
				let column: u32 = call_site.column_number?;
				let location = get_location(state, file_name, Some(line), Some(column))?;
				Some(location)
			})
			.collect();
		Some(stack)
	} else {
		None
	};

	// Get the source.
	let cause_string = v8::String::new_external_onebyte_static(scope, "cause".as_bytes()).unwrap();
	let source = exception
		.is_native_error()
		.then(|| exception.to_object(scope).unwrap())
		.and_then(|exception| exception.get(scope, cause_string.into()))
		.and_then(|value| value.to_object(scope))
		.map(|cause| from_exception(state, scope, cause.into()))
		.map(|error| Arc::new(error) as _);

	// Create the error.
	Error {
		message,
		location,
		stack,
		source,
	}
}

fn get_location(
	state: &State,
	file: Option<&str>,
	line: Option<u32>,
	column: Option<u32>,
) -> Option<tangram_error::Location> {
	if file.map_or(false, |resource_name| resource_name == "[global]") {
		let Some(global_source_map) = state.global_source_map.as_ref() else {
			return None;
		};
		let Some(line) = line else {
			return None;
		};
		let Some(column) = column else {
			return None;
		};
		let token = global_source_map.lookup_token(line, column).unwrap();
		let source = token.get_source().unwrap().to_owned();
		let source = format!("[global]:{source}");
		let line = token.get_src_line();
		let column = token.get_src_col();
		let location = tangram_error::Location {
			source,
			line,
			column,
		};
		return Some(location);
	}

	if let Some(module) = file.and_then(|resource_name| tg::Module::from_str(resource_name).ok()) {
		let Some(line) = line else {
			return None;
		};
		let Some(column) = column else {
			return None;
		};
		let modules = state.modules.borrow();
		let module = modules.iter().find(|m| m.module == module);
		let name = module
			.as_ref()
			.and_then(|module| module.metadata.as_ref())
			.and_then(|metadata| metadata.name.as_deref())
			.unwrap_or("<unknown>");
		let version = module
			.as_ref()
			.and_then(|module| module.metadata.as_ref())
			.and_then(|metadata| metadata.version.as_deref())
			.unwrap_or("<unknown>");
		let path = module.as_ref().map_or_else(
			|| "<unknown>".to_owned(),
			|module| module.module.unwrap_normal_ref().path.to_string(),
		);
		let source = format!("{name}@{version}:{path}");
		let (line, column) =
			if let Some(source_map) = module.and_then(|module| module.source_map.as_ref()) {
				let token = source_map.lookup_token(line, column).unwrap();
				(token.get_src_line(), token.get_src_col())
			} else {
				(line, column)
			};
		let location = tangram_error::Location {
			source,
			line,
			column,
		};
		return Some(location);
	}

	None
}
