#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use super::Compiler;
use itertools::Itertools as _;
use tangram_client as tg;

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
	let name: String = serde_v8::from_v8(scope, args.get(0)).unwrap();

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
		"module_load" => sync(scope, &args, self::module::load),
		"module_resolve" => sync(scope, &args, self::module::resolve),
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
	A: serde::de::DeserializeOwned,
	T: serde::Serialize,
	F: FnOnce(&mut v8::HandleScope<'s>, Compiler, A) -> tg::Result<T>,
{
	// Get the context.
	let context = scope.get_current_context();

	// Get the compiler.
	let compiler = context.get_slot::<Compiler>(scope).unwrap().clone();

	// Collect the args.
	let args = (1..args.length()).map(|i| args.get(i)).collect_vec();
	let args = v8::Array::new_with_elements(scope, args.as_slice());

	// Deserialize the args.
	let args = serde_v8::from_v8(scope, args.into())
		.map_err(|source| tg::error!(!source, "failed to deserialize the args"))?;

	// Call the function.
	let value = f(scope, compiler, args)?;

	// Serialize the value.
	let value = serde_v8::to_v8(scope, &value)
		.map_err(|source| tg::error!(!source, "failed to serialize the value"))?;

	Ok(value)
}
