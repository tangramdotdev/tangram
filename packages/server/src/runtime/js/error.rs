use super::{
	convert::{from_v8, ToV8},
	State,
};
use either::Either;
use num::ToPrimitive as _;
use std::{collections::BTreeMap, rc::Rc, sync::Arc};
use tangram_client::{self as tg, error::Error};

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StackTrace {
	call_sites: Vec<CallSite>,
}

#[allow(dead_code, clippy::struct_excessive_bools)]
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct CallSite {
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
	let message = Some(message_.get(scope).to_rust_string_lossy(scope));

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

	let location = get_location(state, None, resource_name.as_deref(), line, column);

	// Get the stack trace.
	let stack = v8::String::new_external_onebyte_static(scope, "stack".as_bytes()).unwrap();
	let stack = if let Some(stack) = exception
		.is_native_error()
		.then(|| exception.to_object(scope).unwrap())
		.and_then(|exception| exception.get(scope, stack.into()))
		.and_then(|value| serde_v8::from_v8::<StackTrace>(scope, value).ok())
	{
		let stack = stack
			.call_sites
			.iter()
			.rev()
			.filter_map(|call_site| {
				let symbol = call_site.function_name.as_ref().map(|function_name| {
					if let Some(type_name) = call_site.type_name.as_ref() {
						format!("{type_name}.{function_name}")
					} else {
						function_name.clone()
					}
				});
				let file_name = call_site.file_name.as_deref();
				let line: u32 = call_site.line_number? - 1;
				let column: u32 = call_site.column_number?;
				let location = get_location(state, symbol, file_name, Some(line), Some(column))?;
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
	let values = BTreeMap::new();
	// Create the error.
	Error {
		message,
		location,
		stack,
		source,
		values,
	}
}

pub fn capture_stack_trace(scope: &mut v8::HandleScope<'_>) -> Option<Vec<tg::error::Location>> {
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<Rc<State>>(scope).unwrap().clone();

	// Get the current stack trace.
	let stack = v8::StackTrace::current_stack_trace(scope, 1024 * 1024)?;

	// Collect the stack frames.
	let stack = (0..stack.get_frame_count())
		.rev()
		.filter_map(|index| {
			let frame = stack.get_frame(scope, index)?;
			let file = frame
				.get_script_name_or_source_url(scope)
				.map(|name| name.to_rust_string_lossy(scope))?;
			let line = frame.get_line_number().to_u32().unwrap() - 1;
			let column = frame.get_column().to_u32().unwrap() - 1;
			let symbol = frame
				.get_function_name(scope)
				.map(|name| name.to_rust_string_lossy(scope));
			get_location(
				state.as_ref(),
				symbol,
				Some(&file),
				Some(line),
				Some(column),
			)
		})
		.collect();

	Some(stack)
}

fn get_location(
	state: &State,
	symbol: Option<String>,
	file: Option<&str>,
	line: Option<u32>,
	column: Option<u32>,
) -> Option<tg::error::Location> {
	if file.map_or(false, |resource_name| resource_name == "[global]") {
		let line = line?;
		let column = column?;
		let global_source_map = state.global_source_map.as_ref()?;
		let token = global_source_map.lookup_token(line, column)?;
		let line = token.get_src_line();
		let column = token.get_src_col();
		let symbol = token.get_name().map(String::from);
		let source = tg::error::Source::Internal(token.get_source().unwrap().parse().unwrap());
		let location = tg::error::Location {
			symbol,
			source,
			line,
			column,
		};
		return Some(location);
	}

	if let Some(module) = file.and_then(|resource_name| resource_name.parse().ok()) {
		// Get the module.
		let modules = state.modules.borrow();
		let module = modules.iter().find(|m| m.module == module)?;

		// Get the source.
		let source = match &module.module {
			tg::Module {
				kind: tg::module::Kind::Js | tg::module::Kind::Ts,
				package: Either::Right(package),
				..
			} => tg::error::Source::Package(package.clone()),

			_ => return None,
		};

		// Get the line and column and apply a source map if one is available.
		let mut line = line?;
		let mut column = column?;
		if let Some(source_map) = module.source_map.as_ref() {
			if let Some(token) = source_map.lookup_token(line, column) {
				line = token.get_src_line();
				column = token.get_src_col();
			}
		}

		// Create the location.
		let location = tg::error::Location {
			symbol,
			source,
			line,
			column,
		};

		return Some(location);
	}

	None
}

pub fn prepare_stack_trace_callback<'s>(
	scope: &mut v8::HandleScope<'s>,
	_error: v8::Local<v8::Value>,
	call_sites: v8::Local<v8::Array>,
) -> v8::Local<'s, v8::Value> {
	let length = call_sites.length();
	let output_call_sites = v8::Array::new(scope, length.to_i32().unwrap());
	for index in 0..length {
		let call_site = call_sites.get_index(scope, index).unwrap();
		let call_site = v8::Local::<v8::Object>::try_from(call_site).unwrap();
		let output_call_site = v8::Object::new(scope);
		for (function, key) in [
			("getTypeName", "typeName"),
			("getFunctionName", "functionName"),
			("getMethodName", "methodName"),
			("getFileName", "fileName"),
			("getLineNumber", "lineNumber"),
			("getColumnNumber", "columnNumber"),
			("isEval", "isEval"),
			("isNative", "isNative"),
			("isConstructor", "isConstructor"),
			("isAsync", "isAsync"),
			("isPromiseAll", "isPromiseAll"),
			("getPromiseIndex", "promiseIndex"),
		] {
			let function =
				v8::String::new_external_onebyte_static(scope, function.as_bytes()).unwrap();
			let function = call_site.get(scope, function.into()).unwrap();
			let function = v8::Local::<v8::Function>::try_from(function).unwrap();
			let key = v8::String::new_external_onebyte_static(scope, key.as_bytes()).unwrap();
			let value = function.call(scope, call_site.into(), &[]).unwrap();
			output_call_site.set(scope, key.into(), value);
		}
		output_call_sites.set_index(scope, index, output_call_site.into());
	}
	let output = v8::Object::new(scope);
	let key = v8::String::new_external_onebyte_static(scope, "callSites".as_bytes()).unwrap();
	output.set(scope, key.into(), output_call_sites.into());
	output.into()
}
