use super::SOURCE_MAP;
use itertools::Itertools as _;
use num::ToPrimitive as _;
use sourcemap::SourceMap;
use std::collections::BTreeMap;
use tangram_client as tg;
use tangram_v8::{FromV8 as _, Serde, ToV8 as _};

pub(super) fn to_exception<'s>(
	scope: &mut v8::HandleScope<'s>,
	error: &tg::Error,
) -> v8::Local<'s, v8::Value> {
	Serde(error.to_data()).to_v8(scope).unwrap()
}

pub(super) fn from_exception<'s>(
	scope: &mut v8::HandleScope<'s>,
	exception: v8::Local<'s, v8::Value>,
) -> tg::Error {
	if exception.is_object() && !exception.is_native_error() {
		return Serde::from_v8(scope, exception).unwrap().0;
	}

	// Get the message.
	let message_ = v8::Exception::create_message(scope, exception);
	let message = Some(message_.get(scope).to_rust_string_lossy(scope));

	// Get the location.
	let line = message_.get_line_number(scope).unwrap().to_u32().unwrap() - 1;
	let column = message_.get_start_column().to_u32().unwrap();
	let location = get_location(line, column).and_then(|location| location.try_into().ok());

	// Get the source.
	let cause_string = v8::String::new_external_onebyte_static(scope, b"cause").unwrap();
	let source = exception
		.is_native_error()
		.then(|| exception.to_object(scope).unwrap())
		.and_then(|exception| exception.get(scope, cause_string.into()))
		.and_then(|value| value.to_object(scope))
		.map(|cause| from_exception(scope, cause.into()))
		.map(|error| tg::Referent {
			item: Box::new(error),
			path: None,
			tag: None,
		});

	// Get the stack trace.
	let stack = v8::String::new_external_onebyte_static(scope, b"stack").unwrap();
	let stack = exception
		.is_native_error()
		.then(|| exception.to_object(scope).unwrap())
		.and_then(|exception| exception.get(scope, stack.into()))
		.and_then(|value| Serde::<Vec<tg::error::data::Location>>::from_v8(scope, value).ok())
		.and_then(|stack| {
			stack
				.0
				.into_iter()
				.map(TryInto::try_into)
				.try_collect()
				.ok()
		});

	let values = BTreeMap::new();

	tg::Error {
		code: None,
		message,
		location,
		stack,
		source,
		values,
	}
}

pub fn prepare_stack_trace_callback<'s>(
	scope: &mut v8::HandleScope<'s>,
	_error: v8::Local<v8::Value>,
	call_sites: v8::Local<v8::Array>,
) -> v8::Local<'s, v8::Value> {
	let length = call_sites.length();
	let mut stack = Vec::with_capacity(length.to_usize().unwrap());
	for index in (0..length).rev() {
		let call_site = call_sites.get_index(scope, index).unwrap();
		let call_site = v8::Local::<v8::Object>::try_from(call_site).unwrap();

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

		// Convert to location using the get_location function.
		if let (Some(line), Some(column)) = (line_number, column_number) {
			if let Some(location) = get_location(line, column) {
				stack.push(location);
			}
		}
	}

	// Return the stack as a serialized value.
	Serde(stack).to_v8(scope).unwrap()
}

fn get_location(line: u32, column: u32) -> Option<tg::error::data::Location> {
	let source_map = SourceMap::from_slice(SOURCE_MAP).unwrap();
	let token = source_map.lookup_token(line, column)?;
	let symbol = token.get_name().map(String::from);
	let path = token.get_source().unwrap().parse().unwrap();
	let file = tg::error::data::File::Internal(path);
	let line = token.get_src_line();
	let column = token.get_src_col();
	let location = tg::error::data::Location {
		symbol,
		file,
		line,
		column,
	};
	Some(location)
}
