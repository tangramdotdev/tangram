#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use crate::{error, Import, Inner, Module};
use base64::Engine as _;
use bytes::Bytes;
use itertools::Itertools;
use std::{collections::BTreeMap, sync::Arc};
use tangram_error::{Result, WrapErr};

pub fn syscall<'s>(
	scope: &mut v8::HandleScope<'s>,
	args: v8::FunctionCallbackArguments<'s>,
	mut return_value: v8::ReturnValue,
) {
	// Get the syscall name.
	let name: String = serde_v8::from_v8(scope, args.get(0)).unwrap();

	// Invoke the syscall.
	let result = match name.as_str() {
		"documents" => syscall_sync(scope, &args, syscall_documents),
		"encoding_base64_decode" => syscall_sync(scope, &args, syscall_encoding_base64_decode),
		"encoding_base64_encode" => syscall_sync(scope, &args, syscall_encoding_base64_encode),
		"encoding_hex_decode" => syscall_sync(scope, &args, syscall_encoding_hex_decode),
		"encoding_hex_encode" => syscall_sync(scope, &args, syscall_encoding_hex_encode),
		"encoding_json_decode" => syscall_sync(scope, &args, syscall_encoding_json_decode),
		"encoding_json_encode" => syscall_sync(scope, &args, syscall_encoding_json_encode),
		"encoding_toml_decode" => syscall_sync(scope, &args, syscall_encoding_toml_decode),
		"encoding_toml_encode" => syscall_sync(scope, &args, syscall_encoding_toml_encode),
		"encoding_utf8_decode" => syscall_sync(scope, &args, syscall_encoding_utf8_decode),
		"encoding_utf8_encode" => syscall_sync(scope, &args, syscall_encoding_utf8_encode),
		"encoding_yaml_decode" => syscall_sync(scope, &args, syscall_encoding_yaml_decode),
		"encoding_yaml_encode" => syscall_sync(scope, &args, syscall_encoding_yaml_encode),
		"log" => syscall_sync(scope, &args, syscall_log),
		"module_load" => syscall_sync(scope, &args, syscall_module_load),
		"module_resolve" => syscall_sync(scope, &args, syscall_module_resolve),
		"module_version" => syscall_sync(scope, &args, syscall_module_version),
		_ => unreachable!(r#"Unknown syscall "{name}"."#),
	};

	// Handle the result.
	match result {
		Ok(value) => {
			// Set the return value.
			return_value.set(value);
		},

		Err(error) => {
			// Throw an exception.
			let exception = error::to_exception(scope, &error);
			scope.throw_exception(exception);
		},
	}
}

fn syscall_documents(
	_scope: &mut v8::HandleScope,
	state: &Inner,
	_args: (),
) -> Result<Vec<Module>> {
	state.main_runtime_handle.clone().block_on(async move {
		Ok(state
			.document_store
			.documents()
			.await
			.into_iter()
			.map(Module::Document)
			.collect())
	})
}

fn syscall_encoding_base64_decode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (String,),
) -> Result<Bytes> {
	let (value,) = args;
	let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
		.decode(value)
		.wrap_err("Failed to decode the bytes.")?;
	Ok(bytes.into())
}

fn syscall_encoding_base64_encode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (Bytes,),
) -> Result<String> {
	let (value,) = args;
	let encoded = base64::engine::general_purpose::STANDARD_NO_PAD.encode(value);
	Ok(encoded)
}

fn syscall_encoding_hex_decode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (String,),
) -> Result<Bytes> {
	let (string,) = args;
	let bytes = hex::decode(string).wrap_err("Failed to decode the string as hex.")?;
	Ok(bytes.into())
}

fn syscall_encoding_hex_encode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (Bytes,),
) -> Result<String> {
	let (bytes,) = args;
	let hex = hex::encode(bytes);
	Ok(hex)
}

fn syscall_encoding_json_decode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (String,),
) -> Result<serde_json::Value> {
	let (json,) = args;
	let value = serde_json::from_str(&json).wrap_err("Failed to decode the string as json.")?;
	Ok(value)
}

fn syscall_encoding_json_encode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (serde_json::Value,),
) -> Result<String> {
	let (value,) = args;
	let json = serde_json::to_string(&value).wrap_err("Failed to encode the value.")?;
	Ok(json)
}

fn syscall_encoding_toml_decode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (String,),
) -> Result<toml::Value> {
	let (toml,) = args;
	let value = toml::from_str(&toml).wrap_err("Failed to decode the string as toml.")?;
	Ok(value)
}

fn syscall_encoding_toml_encode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (toml::Value,),
) -> Result<String> {
	let (value,) = args;
	let toml = toml::to_string(&value).wrap_err("Failed to encode the value.")?;
	Ok(toml)
}

fn syscall_encoding_utf8_decode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (Bytes,),
) -> Result<String> {
	let (bytes,) = args;
	let string =
		String::from_utf8(bytes.into()).wrap_err("Failed to decode the bytes as UTF-8.")?;
	Ok(string)
}

fn syscall_encoding_utf8_encode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (String,),
) -> Result<Bytes> {
	let (string,) = args;
	let bytes = string.into_bytes().into();
	Ok(bytes)
}

fn syscall_encoding_yaml_decode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (String,),
) -> Result<serde_yaml::Value> {
	let (yaml,) = args;
	let value = serde_yaml::from_str(&yaml).wrap_err("Failed to decode the string as yaml.")?;
	Ok(value)
}

fn syscall_encoding_yaml_encode(
	_scope: &mut v8::HandleScope,
	_state: &Inner,
	args: (serde_yaml::Value,),
) -> Result<String> {
	let (value,) = args;
	let yaml = serde_yaml::to_string(&value).wrap_err("Failed to encode the value.")?;
	Ok(yaml)
}

fn syscall_log(_scope: &mut v8::HandleScope, _state: &Inner, args: (String,)) -> Result<()> {
	let (string,) = args;
	tracing::debug!("{string}");
	Ok(())
}

fn syscall_module_load(
	_scope: &mut v8::HandleScope,
	state: &Inner,
	args: (Module,),
) -> Result<String> {
	let (module,) = args;
	state.main_runtime_handle.clone().block_on(async move {
		let tg = state.tg.as_ref();
		let text = module
			.load(tg, Some(&state.document_store))
			.await
			.wrap_err_with(|| format!(r#"Failed to load module "{module}"."#))?;
		Ok(text)
	})
}

fn syscall_module_resolve(
	_scope: &mut v8::HandleScope,
	state: &Inner,
	args: (Module, String, Option<BTreeMap<String, String>>),
) -> Result<Module> {
	let (module, specifier, attributes) = args;
	let import = Import::with_specifier_and_attributes(&specifier, attributes.as_ref())
		.wrap_err("Failed to create the import.")?;
	state.main_runtime_handle.clone().block_on(async move {
		let tg = state.tg.as_ref();
		let module = module
			.resolve(tg, Some(&state.document_store), &import)
			.await
			.wrap_err_with(|| {
				format!(
					r#"Failed to resolve specifier "{specifier}" relative to module "{module}"."#
				)
			})?;
		Ok(module)
	})
}

fn syscall_module_version(
	_scope: &mut v8::HandleScope,
	state: &Inner,
	args: (Module,),
) -> Result<String> {
	let (module,) = args;
	state.main_runtime_handle.clone().block_on(async move {
		let version = module.version(Some(&state.document_store)).await?;
		Ok(version.to_string())
	})
}

fn syscall_sync<'s, A, T, F>(
	scope: &mut v8::HandleScope<'s>,
	args: &v8::FunctionCallbackArguments,
	f: F,
) -> Result<v8::Local<'s, v8::Value>>
where
	A: serde::de::DeserializeOwned,
	T: serde::Serialize,
	F: FnOnce(&mut v8::HandleScope<'s>, &Inner, A) -> Result<T>,
{
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<Arc<Inner>>(scope).unwrap().clone();

	// Collect the args.
	let args = (1..args.length()).map(|i| args.get(i)).collect_vec();
	let args = v8::Array::new_with_elements(scope, args.as_slice());

	// Deserialize the args.
	let args = serde_v8::from_v8(scope, args.into()).wrap_err("Failed to deserialize the args.")?;

	// Call the function.
	let value = f(scope, &state, args)?;

	// Serialize the value.
	let value = serde_v8::to_v8(scope, &value).wrap_err("Failed to serialize the value.")?;

	Ok(value)
}
