#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use {super::Compiler, tangram_client::prelude::*};

mod document;
mod encoding;
mod log;
mod module;

pub fn syscall<'s>(
	scope: &mut v8::HandleScope<'s>,
	args: v8::FunctionCallbackArguments<'s>,
	mut return_value: v8::ReturnValue,
) {
	// Get the syscall name.
	let name = <String as tangram_v8::Deserialize>::deserialize(scope, args.get(0)).unwrap();

	// Invoke the syscall.
	let result = match name.as_str() {
		"document_list" => sync(scope, &args, self::document::list),
		"encoding_base64_decode" => sync(scope, &args, self::encoding::base64_decode),
		"encoding_base64_encode" => sync(scope, &args, self::encoding::base64_encode),
		"encoding_hex_decode" => sync(scope, &args, self::encoding::hex_decode),
		"encoding_hex_encode" => sync(scope, &args, self::encoding::hex_encode),
		"encoding_json_decode" => sync(scope, &args, self::encoding::json_decode),
		"encoding_json_encode" => sync(scope, &args, self::encoding::json_encode),
		"encoding_toml_decode" => sync(scope, &args, self::encoding::toml_decode),
		"encoding_toml_encode" => sync(scope, &args, self::encoding::toml_encode),
		"encoding_utf8_decode" => sync(scope, &args, self::encoding::utf8_decode),
		"encoding_utf8_encode" => sync(scope, &args, self::encoding::utf8_encode),
		"encoding_yaml_decode" => sync(scope, &args, self::encoding::yaml_decode),
		"encoding_yaml_encode" => sync(scope, &args, self::encoding::yaml_encode),
		"log" => sync(scope, &args, self::log::log),
		"module_invalidated_resolutions" => {
			sync(scope, &args, self::module::invalidated_resolutions)
		},
		"module_load" => sync(scope, &args, self::module::load),
		"module_resolve" => sync(scope, &args, self::module::resolve),
		"module_validate_resolutions" => sync(scope, &args, self::module::validate_resolutions),
		"module_version" => sync(scope, &args, self::module::version),
		_ => unreachable!(r#"unknown syscall "{name}""#),
	};

	// Handle the result.
	match result {
		Ok(value) => {
			// Set the return value.
			return_value.set(value);
		},

		Err(error) => {
			// Throw an exception.
			let exception = super::error::to_exception(scope, &error);
			scope.throw_exception(exception);
		},
	}
}

fn sync<'s, A, T, F>(
	scope: &mut v8::HandleScope<'s>,
	args: &v8::FunctionCallbackArguments,
	f: F,
) -> tg::Result<v8::Local<'s, v8::Value>>
where
	A: tangram_v8::Deserialize<'s>,
	T: tangram_v8::Serialize,
	F: FnOnce(&Compiler, &mut v8::HandleScope<'s>, A) -> tg::Result<T>,
{
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<Compiler>().unwrap().clone();

	// Collect the args.
	let args = (1..args.length()).map(|i| args.get(i)).collect::<Vec<_>>();
	let args = v8::Array::new_with_elements(scope, args.as_slice());

	// Deserialize the args.
	let args = A::deserialize(scope, args.into())
		.map_err(|source| tg::error!(!source, "failed to deserialize the args"))?;

	// Call the function.
	let value = f(&state, scope, args)?;

	// Serialize the value to v8.
	let value = value
		.serialize(scope)
		.map_err(|source| tg::error!(!source, "failed to serialize the value"))?;

	Ok(value)
}
