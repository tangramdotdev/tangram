#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use super::{FutureOutput, State};
use futures::FutureExt as _;
use itertools::Itertools as _;
use std::rc::Rc;
use tangram_client as tg;
use tangram_v8::convert::{FromV8, ToV8};

mod blob;
mod checksum;
mod encoding;
mod import;
mod magic;
mod object;
mod process;
mod sleep;

pub mod log;

pub fn syscall<'s>(
	scope: &mut v8::HandleScope<'s>,
	args: v8::FunctionCallbackArguments<'s>,
	mut return_value: v8::ReturnValue,
) {
	// Get the syscall name.
	let name = String::from_v8(scope, args.get(0)).unwrap();

	// Invoke the syscall.
	let result = match name.as_str() {
		"blob_create" => async_(scope, &args, self::blob::create),
		"blob_read" => async_(scope, &args, self::blob::read),
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
		"import" => async_(scope, &args, self::import::import),
		"log" => sync(scope, &args, self::log::log),
		"magic" => self::magic::magic(scope, &args),
		"object_get" => async_(scope, &args, self::object::get),
		"object_id" => sync(scope, &args, self::object::id),
		"process_get" => async_(scope, &args, self::process::get),
		"process_spawn" => async_(scope, &args, self::process::spawn),
		"process_wait" => async_(scope, &args, self::process::wait),
		"sleep" => async_(scope, &args, self::sleep::sleep),
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
	A: FromV8,
	T: ToV8,
	F: FnOnce(Rc<State>, &mut v8::HandleScope<'s>, A) -> tg::Result<T>,
{
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<Rc<State>>().unwrap().clone();

	// Collect the args.
	let args = (1..args.length()).map(|i| args.get(i)).collect_vec();
	let args = v8::Array::new_with_elements(scope, args.as_slice());

	// Deserialize the args.
	let args = A::from_v8(scope, args.into())
		.map_err(|source| tg::error!(!source, "failed to deserialize the args"))?;

	// Call the function.
	let value = f(state, scope, args)?;

	// Move the value to v8.
	let value = value
		.to_v8(scope)
		.map_err(|source| tg::error!(!source, "failed to serialize the value"))?;

	Ok(value)
}

fn async_<'s, A, T, F, Fut>(
	scope: &mut v8::HandleScope<'s>,
	args: &v8::FunctionCallbackArguments,
	f: F,
) -> tg::Result<v8::Local<'s, v8::Value>>
where
	A: FromV8 + 'static,
	T: ToV8 + 'static,
	F: FnOnce(Rc<State>, A) -> Fut + 'static,
	Fut: Future<Output = tg::Result<T>>,
{
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<Rc<State>>().unwrap().clone();

	// Collect the args.
	let args = (1..args.length()).map(|i| args.get(i)).collect_vec();
	let args = v8::Array::new_with_elements(scope, args.as_slice());

	// Deserialize the args.
	let args = A::from_v8(scope, args.into())
		.map_err(|source| tg::error!(!source, "failed to deserialize the args"))?;

	// Create the promise.
	let promise_resolver = v8::PromiseResolver::new(scope).unwrap();
	let promise = promise_resolver.get_promise(scope);

	// Move the promise resolver to the global scope.
	let promise_resolver = v8::Global::new(scope, promise_resolver);

	// Create the future.
	let future = {
		let state = state.clone();
		async move {
			let result = f(state, args)
				.await
				.map(|value| Box::new(value) as Box<dyn ToV8>);
			FutureOutput {
				promise_resolver,
				result,
			}
		}
		.boxed_local()
	};

	// Add the future to the context's futures.
	state.futures.borrow_mut().push(future);

	Ok(promise.into())
}
