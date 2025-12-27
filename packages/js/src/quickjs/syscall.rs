#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use {
	rquickjs::{self as qjs, function::Async},
	tangram_client::prelude::*,
};

mod batch;
mod blob;
mod checksum;
mod encoding;
mod log;
mod magic;
mod object;
mod process;
mod sleep;

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
		"checksum" => qjs::Function::new(ctx.clone(), Async(checksum::checksum)),
		"encoding_base64_decode" => qjs::Function::new(ctx.clone(), encoding::base64_decode),
		"encoding_base64_encode" => qjs::Function::new(ctx.clone(), encoding::base64_encode),
		"encoding_hex_decode" => qjs::Function::new(ctx.clone(), encoding::hex_decode),
		"encoding_hex_encode" => qjs::Function::new(ctx.clone(), encoding::hex_encode),
		"encoding_json_decode" => qjs::Function::new(ctx.clone(), encoding::json_decode),
		"encoding_json_encode" => qjs::Function::new(ctx.clone(), encoding::json_encode),
		"encoding_toml_decode" => qjs::Function::new(ctx.clone(), encoding::toml_decode),
		"encoding_toml_encode" => qjs::Function::new(ctx.clone(), encoding::toml_encode),
		"encoding_utf8_decode" => qjs::Function::new(ctx.clone(), encoding::utf8_decode),
		"encoding_utf8_encode" => qjs::Function::new(ctx.clone(), encoding::utf8_encode),
		"encoding_yaml_decode" => qjs::Function::new(ctx.clone(), encoding::yaml_decode),
		"encoding_yaml_encode" => qjs::Function::new(ctx.clone(), encoding::yaml_encode),
		"log" => qjs::Function::new(ctx.clone(), log::log),
		"magic" => qjs::Function::new(ctx.clone(), magic::magic),
		"object_batch" => qjs::Function::new(ctx.clone(), Async(batch::object_batch)),
		"object_get" => qjs::Function::new(ctx.clone(), Async(object::get)),
		"object_id" => qjs::Function::new(ctx.clone(), object::id),
		"process_get" => qjs::Function::new(ctx.clone(), Async(process::get)),
		"process_spawn" => qjs::Function::new(ctx.clone(), Async(process::spawn)),
		"process_wait" => qjs::Function::new(ctx.clone(), Async(process::wait)),
		"read" => qjs::Function::new(ctx.clone(), Async(blob::read)),
		"sleep" => qjs::Function::new(ctx.clone(), Async(sleep::sleep)),
		"write" => qjs::Function::new(ctx.clone(), Async(blob::write)),
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
