#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use {super::State, std::rc::Rc, tangram_client::prelude::*};

mod encoding;
mod host;
mod http2;

pub fn syscall<'s>(
	scope: &mut v8::PinScope<'s, '_>,
	args: v8::FunctionCallbackArguments<'s>,
	mut return_value: v8::ReturnValue,
) {
	// Get the syscall name.
	let name = <String as tangram_v8::Deserialize>::deserialize(scope, args.get(0)).unwrap();

	// Invoke the syscall.
	let result = match name.as_str() {
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
		"host_checksum" => async_(scope, &args, self::host::checksum),
		"host_close" => async_(scope, &args, self::host::close),
		"host_current" => sync(scope, &args, self::host::current),
		"host_disable_raw_mode" => async_(scope, &args, self::host::disable_raw_mode),
		"host_enable_raw_mode" => async_(scope, &args, self::host::enable_raw_mode),
		"host_exec" => async_(scope, &args, self::host::exec),
		"host_exists" => async_(scope, &args, self::host::exists),
		"host_get_tty_size" => sync(scope, &args, self::host::get_tty_size),
		"host_get_xattr" => async_(scope, &args, self::host::getxattr),
		"host_is_foreground_controlling_tty" => {
			sync(scope, &args, self::host::is_foreground_controlling_tty)
		},
		"host_is_tty" => sync(scope, &args, self::host::is_tty),
		"host_magic" => self::host::magic(scope, &args),
		"host_mkdtemp" => async_(scope, &args, self::host::mkdtemp),
		"host_object_id" => sync(scope, &args, self::host::object_id),
		"host_parallelism" => sync(scope, &args, self::host::parallelism),
		"host_read" => async_(scope, &args, self::host::read),
		"host_remove" => async_(scope, &args, self::host::remove),
		"host_signal" => async_(scope, &args, self::host::signal),
		"host_signal_close" => async_(scope, &args, self::host::listen_signal_close),
		"host_signal_open" => async_(scope, &args, self::host::listen_signal_open),
		"host_signal_read" => async_(scope, &args, self::host::listen_signal_read),
		"host_sleep" => async_(scope, &args, self::host::sleep),
		"host_spawn" => async_(scope, &args, self::host::spawn),
		"host_stopper_close" => async_(scope, &args, self::host::stopper_close),
		"host_stopper_open" => async_(scope, &args, self::host::stopper_open),
		"host_stopper_stop" => async_(scope, &args, self::host::stopper_stop),
		"host_value_parse" => sync(scope, &args, self::host::value_parse),
		"host_value_stringify" => sync(scope, &args, self::host::value_stringify),
		"host_wait" => async_(scope, &args, self::host::wait),
		"host_write" => async_(scope, &args, self::host::write),
		"host_write_sync" => sync(scope, &args, self::host::write_sync),
		"http2_connect" => async_(scope, &args, self::http2::connect),
		"http2_session_close" => async_(scope, &args, self::http2::session_close),
		"http2_session_destroy" => async_(scope, &args, self::http2::session_destroy),
		"http2_session_request" => async_(scope, &args, self::http2::session_request),
		"http2_stream_close" => async_(scope, &args, self::http2::stream_close),
		"http2_stream_end" => async_(scope, &args, self::http2::stream_end),
		"http2_stream_read" => async_(scope, &args, self::http2::stream_read),
		"http2_stream_write" => async_(scope, &args, self::http2::stream_write),
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
			let error = tg::error!(!error, %name, "the syscall failed");
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
		.map_err(|error| tg::error!(!error, "failed to deserialize the args"))?;

	// Call the function.
	let value = f(state, scope, args)?;

	// Serialize the value to v8.
	let value = value
		.serialize(scope)
		.map_err(|error| tg::error!(!error, "failed to serialize the value"))?;

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
		.map_err(|error| tg::error!(!error, "failed to deserialize the args"))?;

	let promise = state.create_promise(scope, f(state.clone(), args));

	Ok(promise.into())
}
