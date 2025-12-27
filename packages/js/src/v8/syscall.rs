#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use {super::State, std::rc::Rc, tangram_client::prelude::*};

mod batch;
mod blob;
mod checksum;
mod encoding;
mod magic;
mod object;
mod process;
mod sleep;

pub mod log;

pub fn syscall<'s>(
	scope: &mut v8::PinScope<'s, '_>,
	args: v8::FunctionCallbackArguments<'s>,
	mut return_value: v8::ReturnValue,
) {
	// Get the syscall name.
	let name = <String as tangram_v8::Deserialize>::deserialize(scope, args.get(0)).unwrap();

	// Invoke the syscall.
	let result = match name.as_str() {
		"checksum" => async_(scope, &args, self::checksum::checksum),
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
		"magic" => self::magic::magic(scope, &args),
		"object_batch" => async_(scope, &args, self::batch::object_batch),
		"object_get" => async_(scope, &args, self::object::get),
		"object_id" => sync(scope, &args, self::object::id),
		"process_get" => async_(scope, &args, self::process::get),
		"process_spawn" => async_(scope, &args, self::process::spawn),
		"process_wait" => async_(scope, &args, self::process::wait),
		"read" => async_(scope, &args, self::blob::read),
		"sleep" => async_(scope, &args, self::sleep::sleep),
		"write" => async_(scope, &args, self::blob::write),
		_ => Err(tg::error!(%name, "unknown syscall")),
	};

	// Handle the result.
	match result {
		Ok(value) => {
			// Set the return value.
			return_value.set(value);
		},

		Err(error) => {
			// Throw an exception.
			let Some(exception) = super::error::to_exception(scope, &error) else {
				return;
			};
			scope.throw_exception(exception);
		},
	}
}

fn sync<'s, A, T, F>(
	scope: &mut v8::PinScope<'s, '_>,
	args: &v8::FunctionCallbackArguments,
	f: F,
) -> tg::Result<v8::Local<'s, v8::Value>>
where
	A: tangram_v8::Deserialize<'s>,
	T: tangram_v8::Serialize,
	F: FnOnce(Rc<State>, &mut v8::PinScope<'s, '_>, A) -> tg::Result<T>,
{
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<State>().unwrap().clone();

	// Collect the args.
	let args = (1..args.length()).map(|i| args.get(i)).collect::<Vec<_>>();
	let args = v8::Array::new_with_elements(scope, args.as_slice());

	// Deserialize the args.
	let args = A::deserialize(scope, args.into())
		.map_err(|source| tg::error!(!source, "failed to deserialize the args"))?;

	// Call the function.
	let value = f(state, scope, args)?;

	// Serialize the value to v8.
	let value = value
		.serialize(scope)
		.map_err(|source| tg::error!(!source, "failed to serialize the value"))?;

	Ok(value)
}

fn async_<'s, A, T, F, Fut>(
	scope: &mut v8::PinScope<'s, '_>,
	args: &v8::FunctionCallbackArguments,
	f: F,
) -> tg::Result<v8::Local<'s, v8::Value>>
where
	A: tangram_v8::Deserialize<'s> + 'static,
	T: tangram_v8::Serialize + 'static,
	F: FnOnce(Rc<State>, A) -> Fut + 'static,
	Fut: Future<Output = tg::Result<T>> + 'static,
{
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<State>().unwrap().clone();

	// Collect the args.
	let args = (1..args.length()).map(|i| args.get(i)).collect::<Vec<_>>();
	let args = v8::Array::new_with_elements(scope, args.as_slice());

	// Deserialize the args.
	let args = A::deserialize(scope, args.into())
		.map_err(|source| tg::error!(!source, "failed to deserialize the args"))?;

	let promise = state.create_promise(scope, f(state.clone(), args));

	Ok(promise.into())
}
