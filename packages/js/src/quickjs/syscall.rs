#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use {
	rquickjs::{self as qjs, function::Async},
	tangram_client::prelude::*,
};

mod encoding;
mod handle;
mod host;

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
		"handle_arg" => qjs::Function::new(ctx.clone(), self::handle::arg),
		"handle_checkin" => qjs::Function::new(ctx.clone(), Async(self::handle::checkin)),
		"handle_checksum" => qjs::Function::new(ctx.clone(), Async(self::handle::checksum)),
		"handle_checkout" => qjs::Function::new(ctx.clone(), Async(self::handle::checkout)),
		"handle_object_batch" => qjs::Function::new(ctx.clone(), Async(self::handle::object_batch)),
		"handle_object_get" => qjs::Function::new(ctx.clone(), Async(self::handle::object_get)),
		"handle_object_id" => qjs::Function::new(ctx.clone(), self::handle::object_id),
		"handle_process_get" => qjs::Function::new(ctx.clone(), Async(self::handle::process_get)),
		"handle_sandbox_get" => qjs::Function::new(ctx.clone(), Async(self::handle::sandbox_get)),
		"handle_process_signal" => {
			qjs::Function::new(ctx.clone(), Async(self::handle::process_signal))
		},
		"handle_process_spawn" => {
			qjs::Function::new(ctx.clone(), Async(self::handle::process_spawn))
		},
		"handle_process_stdio_read_close" => {
			qjs::Function::new(ctx.clone(), Async(self::handle::process_stdio_read_close))
		},
		"handle_process_stdio_read_open" => {
			qjs::Function::new(ctx.clone(), Async(self::handle::process_stdio_read_open))
		},
		"handle_process_stdio_read_read" => {
			qjs::Function::new(ctx.clone(), Async(self::handle::process_stdio_read_read))
		},
		"handle_process_stdio_write_close" => {
			qjs::Function::new(ctx.clone(), Async(self::handle::process_stdio_write_close))
		},
		"handle_process_stdio_write_open" => {
			qjs::Function::new(ctx.clone(), Async(self::handle::process_stdio_write_open))
		},
		"handle_process_stdio_write_write" => {
			qjs::Function::new(ctx.clone(), Async(self::handle::process_stdio_write_write))
		},
		"handle_process_tty_size_put" => {
			qjs::Function::new(ctx.clone(), Async(self::handle::process_tty_size_put))
		},
		"handle_process_wait" => qjs::Function::new(ctx.clone(), Async(self::handle::process_wait)),
		"handle_read" => qjs::Function::new(ctx.clone(), Async(self::handle::read)),
		"handle_value_parse" => qjs::Function::new(ctx.clone(), self::handle::value_parse),
		"handle_value_stringify" => qjs::Function::new(ctx.clone(), self::handle::value_stringify),
		"handle_write" => qjs::Function::new(ctx.clone(), Async(self::handle::write)),
		"host_close" => qjs::Function::new(ctx.clone(), Async(self::host::close)),
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
		"host_wait" => qjs::Function::new(ctx.clone(), Async(self::host::wait)),
		"host_write" => qjs::Function::new(ctx.clone(), Async(self::host::write)),
		"host_write_sync" => qjs::Function::new(ctx.clone(), self::host::write_sync),
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

impl<'js, T> qjs::IntoJs<'js> for Result<T>
where
	T: qjs::IntoJs<'js>,
{
	fn into_js(self, ctx: &qjs::Ctx<'js>) -> qjs::Result<qjs::Value<'js>> {
		match self.0 {
			Ok(value) => {
				let value = value.into_js(ctx)?;
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
