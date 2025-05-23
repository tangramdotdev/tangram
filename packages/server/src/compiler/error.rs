use super::SOURCE_MAP;
use num::ToPrimitive as _;
use sourcemap::SourceMap;
use std::{collections::BTreeMap, sync::Arc};
use tangram_client as tg;
use tangram_v8::{FromV8 as _, Serde, ToV8 as _};

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StackTrace {
	call_sites: Vec<CallSite>,
}

#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
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
	error: &tg::Error,
) -> v8::Local<'s, v8::Value> {
	error.to_v8(scope).unwrap()
}

pub(super) fn from_exception<'s>(
	scope: &mut v8::HandleScope<'s>,
	exception: v8::Local<'s, v8::Value>,
) -> tg::Error {
	let context = scope.get_current_context();
	let global = context.global(scope);
	let tangram = v8::String::new_external_onebyte_static(scope, "Tangram".as_bytes()).unwrap();
	let tangram = global.get(scope, tangram.into()).unwrap();
	let tangram = v8::Local::<v8::Object>::try_from(tangram).unwrap();

	let error = v8::String::new_external_onebyte_static(scope, "Error".as_bytes()).unwrap();
	let error = tangram.get(scope, error.into()).unwrap();
	let error = v8::Local::<v8::Function>::try_from(error).unwrap();

	if exception
		.instance_of(scope, error.into())
		.unwrap_or_default()
	{
		return <_>::from_v8(scope, exception).unwrap();
	}

	// Get the message.
	let message_ = v8::Exception::create_message(scope, exception);
	let message = Some(message_.get(scope).to_rust_string_lossy(scope));

	// Get the location.
	let line = message_.get_line_number(scope).unwrap().to_u32().unwrap() - 1;
	let column = message_.get_start_column().to_u32().unwrap();
	let location = get_location(line, column);

	// Get the stack trace.
	let stack = v8::String::new_external_onebyte_static(scope, "stack".as_bytes()).unwrap();
	let stack = if let Some(stack) = exception
		.is_native_error()
		.then(|| exception.to_object(scope).unwrap())
		.and_then(|exception| exception.get(scope, stack.into()))
		.and_then(|value| {
			<Serde<StackTrace>>::from_v8(scope, value)
				.map(Serde::into_inner)
				.ok()
		}) {
		let stack = stack
			.call_sites
			.iter()
			.rev()
			.filter_map(|call_site| {
				let line: u32 = call_site.line_number? - 1;
				let column: u32 = call_site.column_number?;
				get_location(line, column)
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
		.map(|cause| from_exception(scope, cause.into()))
		.map(|error| tg::error::Source {
			error: Arc::new(error),
		});
	let values = BTreeMap::new();

	// Create the error.
	tg::Error {
		code: None,
		message,
		location,
		stack,
		source,
		values,
	}
}

fn get_location(line: u32, column: u32) -> Option<tg::error::Location> {
	let source_map = SourceMap::from_slice(SOURCE_MAP).unwrap();
	let token = source_map.lookup_token(line, column)?;
	let symbol = token.get_name().map(String::from);
	let path = token.get_source().unwrap().parse().unwrap();
	let source = tg::error::File::Internal(path);
	let line = token.get_src_line();
	let column = token.get_src_col();
	let location = tg::error::Location {
		symbol,
		file: source,
		line,
		column,
	};
	Some(location)
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
