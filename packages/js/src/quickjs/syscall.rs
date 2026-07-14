#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use {
	rquickjs::{self as qjs, function::Async},
	tangram_client::prelude::*,
};

mod encoding;
mod host;
mod http2;

pub(super) struct Arg<T>(pub T);

struct Result<T>(tg::Result<T>);

pub fn syscall<'js>(
	ctx: qjs::Ctx<'js>,
	args: qjs::function::Rest<qjs::Value<'js>>,
) -> qjs::Result<qjs::Value<'js>> {
	// Get the syscall name.
	let name: String = args
		.first()
		.ok_or_else(|| qjs::Error::new_from_js("args", "syscall name"))?
		.clone()
		.get()?;

	// Collect the remaining args.
	let syscall_args: Vec<qjs::Value> = args.iter().skip(1).cloned().collect();

	// Create the function for this syscall.
	let func: qjs::Function = match name.as_str() {
		"encoding_base64_decode" => qjs::Function::new(ctx.clone(), self::encoding::base64_decode),
		"encoding_base64_encode" => qjs::Function::new(ctx.clone(), self::encoding::base64_encode),
		"encoding_hex_decode" => qjs::Function::new(ctx.clone(), self::encoding::hex_decode),
		"encoding_hex_encode" => qjs::Function::new(ctx.clone(), self::encoding::hex_encode),
		"encoding_json_decode" => qjs::Function::new(ctx.clone(), self::encoding::json_decode),
		"encoding_json_encode" => qjs::Function::new(ctx.clone(), self::encoding::json_encode),
		"encoding_toml_decode" => qjs::Function::new(ctx.clone(), self::encoding::toml_decode),
		"encoding_toml_encode" => qjs::Function::new(ctx.clone(), self::encoding::toml_encode),
		"encoding_utf8_decode" => qjs::Function::new(ctx.clone(), self::encoding::utf8_decode),
		"encoding_utf8_encode" => qjs::Function::new(ctx.clone(), self::encoding::utf8_encode),
		"encoding_yaml_decode" => qjs::Function::new(ctx.clone(), self::encoding::yaml_decode),
		"encoding_yaml_encode" => qjs::Function::new(ctx.clone(), self::encoding::yaml_encode),
		"host_checksum" => qjs::Function::new(ctx.clone(), Async(self::host::checksum)),
		"host_close" => qjs::Function::new(ctx.clone(), Async(self::host::close)),
		"host_current" => qjs::Function::new(ctx.clone(), self::host::current),
		"host_disable_raw_mode" => {
			qjs::Function::new(ctx.clone(), Async(self::host::disable_raw_mode))
		},
		"host_enable_raw_mode" => {
			qjs::Function::new(ctx.clone(), Async(self::host::enable_raw_mode))
		},
		"host_exec" => qjs::Function::new(ctx.clone(), Async(self::host::exec)),
		"host_exists" => qjs::Function::new(ctx.clone(), Async(self::host::exists)),
		"host_get_tty_size" => qjs::Function::new(ctx.clone(), self::host::get_tty_size),
		"host_get_xattr" => qjs::Function::new(ctx.clone(), Async(self::host::getxattr)),
		"host_is_foreground_controlling_tty" => {
			qjs::Function::new(ctx.clone(), self::host::is_foreground_controlling_tty)
		},
		"host_is_tty" => qjs::Function::new(ctx.clone(), self::host::is_tty),
		"host_magic" => qjs::Function::new(ctx.clone(), self::host::magic),
		"host_mkdtemp" => qjs::Function::new(ctx.clone(), Async(self::host::mkdtemp)),
		"host_object_id" => qjs::Function::new(ctx.clone(), self::host::object_id),
		"host_parallelism" => qjs::Function::new(ctx.clone(), self::host::parallelism),
		"host_read" => qjs::Function::new(ctx.clone(), Async(self::host::read)),
		"host_remove" => qjs::Function::new(ctx.clone(), Async(self::host::remove)),
		"host_signal" => qjs::Function::new(ctx.clone(), Async(self::host::signal)),
		"host_signal_close" => {
			qjs::Function::new(ctx.clone(), Async(self::host::listen_signal_close))
		},
		"host_signal_open" => {
			qjs::Function::new(ctx.clone(), Async(self::host::listen_signal_open))
		},
		"host_signal_read" => {
			qjs::Function::new(ctx.clone(), Async(self::host::listen_signal_read))
		},
		"host_sleep" => qjs::Function::new(ctx.clone(), Async(self::host::sleep)),
		"host_spawn" => qjs::Function::new(ctx.clone(), Async(self::host::spawn)),
		"host_stopper_close" => qjs::Function::new(ctx.clone(), Async(self::host::stopper_close)),
		"host_stopper_open" => qjs::Function::new(ctx.clone(), Async(self::host::stopper_open)),
		"host_stopper_stop" => qjs::Function::new(ctx.clone(), Async(self::host::stopper_stop)),
		"host_value_parse" => qjs::Function::new(ctx.clone(), self::host::value_parse),
		"host_value_stringify" => qjs::Function::new(ctx.clone(), self::host::value_stringify),
		"host_wait" => qjs::Function::new(ctx.clone(), Async(self::host::wait)),
		"host_write" => qjs::Function::new(ctx.clone(), Async(self::host::write)),
		"host_write_sync" => qjs::Function::new(ctx.clone(), self::host::write_sync),
		"http2_connect" => qjs::Function::new(ctx.clone(), Async(self::http2::connect)),
		"http2_session_close" => qjs::Function::new(ctx.clone(), Async(self::http2::session_close)),
		"http2_session_destroy" => {
			qjs::Function::new(ctx.clone(), Async(self::http2::session_destroy))
		},
		"http2_session_read" => qjs::Function::new(ctx.clone(), Async(self::http2::session_read)),
		"http2_session_request" => {
			qjs::Function::new(ctx.clone(), Async(self::http2::session_request))
		},
		"http2_stream_close" => qjs::Function::new(ctx.clone(), Async(self::http2::stream_close)),
		"http2_stream_end" => qjs::Function::new(ctx.clone(), Async(self::http2::stream_end)),
		"http2_stream_read" => qjs::Function::new(ctx.clone(), Async(self::http2::stream_read)),
		"http2_stream_write" => qjs::Function::new(ctx.clone(), Async(self::http2::stream_write)),
		_ => {
			return Err(qjs::Error::Io(std::io::Error::other(tg::error!(
				"unknown syscall: {name}"
			))));
		},
	}?;

	// Call the function with the args.
	let mut call_args = qjs::function::Args::new_unsized(ctx);
	call_args.push_args(syscall_args)?;
	let output = call_args.apply(&func)?;

	Ok(output)
}

impl<T> From<tg::Result<T>> for Result<T> {
	fn from(result: tg::Result<T>) -> Self {
		Self(result)
	}
}

impl<'js, T> qjs::FromJs<'js> for Arg<T>
where
	T: tangram_quickjs::Deserialize<'js>,
{
	fn from_js(ctx: &qjs::Ctx<'js>, value: qjs::Value<'js>) -> qjs::Result<Self> {
		T::deserialize(ctx, value)
			.map(Self)
			.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))
	}
}

impl<'js, T> qjs::IntoJs<'js> for Result<T>
where
	T: tangram_quickjs::Serialize,
{
	fn into_js(self, ctx: &qjs::Ctx<'js>) -> qjs::Result<qjs::Value<'js>> {
		match self.0 {
			Ok(value) => {
				let value = value
					.serialize(ctx)
					.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;
				Ok(value)
			},
			Err(error) => {
				let exception = super::error::to_exception(ctx, &error)
					.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))?;
				Err(ctx.throw(exception))
			},
		}
	}
}
