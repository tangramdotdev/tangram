#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use super::{
	convert::{from_v8, FromV8, ToV8},
	State,
};
use base64::Engine as _;
use bytes::Bytes;
use futures::{Future, TryStreamExt};
use itertools::Itertools;
use std::rc::Rc;
use tangram_client as tg;
use tangram_error::{error, Result, WrapErr};
use tokio_util::io::StreamReader;
use url::Url;

pub fn syscall<'s>(
	scope: &mut v8::HandleScope<'s>,
	args: v8::FunctionCallbackArguments<'s>,
	mut return_value: v8::ReturnValue,
) {
	// Get the syscall name.
	let name = String::from_v8(scope, args.get(0)).unwrap();

	// Invoke the syscall.
	let result = match name.as_str() {
		"archive" => syscall_async(scope, &args, syscall_archive),
		"build" => syscall_async(scope, &args, syscall_build),
		"bundle" => syscall_async(scope, &args, syscall_bundle),
		"checksum" => syscall_sync(scope, &args, syscall_checksum),
		"compress" => syscall_async(scope, &args, syscall_compress),
		"decompress" => syscall_async(scope, &args, syscall_decompress),
		"download" => syscall_async(scope, &args, syscall_download),
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
		"extract" => syscall_async(scope, &args, syscall_extract),
		"load" => syscall_async(scope, &args, syscall_load),
		"log" => syscall_sync(scope, &args, syscall_log),
		"read" => syscall_async(scope, &args, syscall_read),
		"sleep" => syscall_async(scope, &args, syscall_sleep),
		"store" => syscall_async(scope, &args, syscall_store),
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
			let exception = super::error::to_exception(scope, &error);
			scope.throw_exception(exception);
		},
	}
}

async fn syscall_archive(
	state: Rc<State>,
	args: (tg::Artifact, tg::blob::ArchiveFormat),
) -> Result<tg::Blob> {
	let (artifact, format) = args;
	let blob = tg::Blob::archive(state.tg.as_ref(), &artifact, format).await?;
	Ok(blob)
}

async fn syscall_build(state: Rc<State>, args: (tg::Target,)) -> Result<tg::Value> {
	let (target,) = args;
	let options = tg::build::GetOrCreateOptions {
		parent: Some(state.build.clone()),
		depth: state.depth + 1,
		retry: state.retry,
		..Default::default()
	};
	let output = target.build(state.tg.as_ref(), options).await?;
	Ok(output)
}

async fn syscall_bundle(state: Rc<State>, args: (tg::Artifact,)) -> Result<tg::Artifact> {
	let (artifact,) = args;
	let artifact = artifact.bundle(state.tg.as_ref()).await?;
	Ok(artifact)
}

fn syscall_checksum(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (tg::checksum::Algorithm, Bytes),
) -> Result<tg::Checksum> {
	let (algorithm, bytes) = args;
	let mut checksum_writer = tg::checksum::Writer::new(algorithm);
	checksum_writer.update(&bytes);
	let checksum = checksum_writer.finalize();
	Ok(checksum)
}

async fn syscall_compress(
	state: Rc<State>,
	args: (tg::Blob, tg::blob::CompressionFormat),
) -> Result<tg::Blob> {
	let (blob, format) = args;
	let blob = blob.compress(state.tg.as_ref(), format).await?;
	Ok(blob)
}

async fn syscall_decompress(
	state: Rc<State>,
	args: (tg::Blob, tg::blob::CompressionFormat),
) -> Result<tg::Blob> {
	let (blob, format) = args;
	let blob = blob.decompress(state.tg.as_ref(), format).await?;
	Ok(blob)
}

async fn syscall_download(state: Rc<State>, args: (Url, tg::Checksum)) -> Result<tg::Blob> {
	let (url, checksum) = args;
	let _permit = state
		.tg
		.file_descriptor_semaphore()
		.acquire()
		.await
		.unwrap();
	let response = reqwest::get(url)
		.await
		.wrap_err("Failed to perform the request.")?
		.error_for_status()
		.wrap_err("Expected a sucess status.")?;
	let mut checksum_writer = tg::checksum::Writer::new(checksum.algorithm());
	let stream = response
		.bytes_stream()
		.map_err(|error| std::io::Error::new(std::io::ErrorKind::Other, error))
		.inspect_ok(|chunk| checksum_writer.update(chunk));
	let blob = tg::Blob::with_reader(state.tg.as_ref(), StreamReader::new(stream)).await?;
	let actual = checksum_writer.finalize();
	if actual != checksum {
		return Err(error!(
			r#"The checksum did not match. Expected "{checksum}" but got "{actual}"."#
		));
	}
	Ok(blob)
}

fn syscall_encoding_base64_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
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
	_state: Rc<State>,
	args: (Bytes,),
) -> Result<String> {
	let (value,) = args;
	let encoded = base64::engine::general_purpose::STANDARD_NO_PAD.encode(value);
	Ok(encoded)
}

fn syscall_encoding_hex_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> Result<Bytes> {
	let (string,) = args;
	let bytes = hex::decode(string).wrap_err("Failed to decode the string as hex.")?;
	Ok(bytes.into())
}

fn syscall_encoding_hex_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (Bytes,),
) -> Result<String> {
	let (bytes,) = args;
	let hex = hex::encode(bytes);
	Ok(hex)
}

fn syscall_encoding_json_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> Result<serde_json::Value> {
	let (json,) = args;
	let value = serde_json::from_str(&json).wrap_err("Failed to decode the string as json.")?;
	Ok(value)
}

fn syscall_encoding_json_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (serde_json::Value,),
) -> Result<String> {
	let (value,) = args;
	let json = serde_json::to_string(&value).wrap_err("Failed to encode the value.")?;
	Ok(json)
}

fn syscall_encoding_toml_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> Result<toml::Value> {
	let (toml,) = args;
	let value = toml::from_str(&toml).wrap_err("Failed to decode the string as toml.")?;
	Ok(value)
}

fn syscall_encoding_toml_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (toml::Value,),
) -> Result<String> {
	let (value,) = args;
	let toml = toml::to_string(&value).wrap_err("Failed to encode the value.")?;
	Ok(toml)
}

fn syscall_encoding_utf8_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (Bytes,),
) -> Result<String> {
	let (bytes,) = args;
	let string =
		String::from_utf8(bytes.to_vec()).wrap_err("Failed to decode the bytes as UTF-8.")?;
	Ok(string)
}

fn syscall_encoding_utf8_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> Result<Bytes> {
	let (string,) = args;
	let bytes = string.into_bytes().into();
	Ok(bytes)
}

fn syscall_encoding_yaml_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> Result<serde_yaml::Value> {
	let (yaml,) = args;
	let value = serde_yaml::from_str(&yaml).wrap_err("Failed to decode the string as yaml.")?;
	Ok(value)
}

fn syscall_encoding_yaml_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (serde_yaml::Value,),
) -> Result<String> {
	let (value,) = args;
	let yaml = serde_yaml::to_string(&value).wrap_err("Failed to encode the value.")?;
	Ok(yaml)
}

async fn syscall_extract(
	state: Rc<State>,
	args: (tg::Blob, tg::blob::ArchiveFormat),
) -> Result<tg::Artifact> {
	let (blob, format) = args;
	let artifact = blob.extract(state.tg.as_ref(), format).await?;
	Ok(artifact)
}

async fn syscall_load(state: Rc<State>, args: (tg::object::Id,)) -> Result<tg::object::Object> {
	let (id,) = args;
	tg::object::Handle::with_id(id)
		.object(state.tg.as_ref())
		.await
}

fn syscall_log(_scope: &mut v8::HandleScope, state: Rc<State>, args: (String,)) -> Result<()> {
	let (string,) = args;
	if let Some(log_sender) = state.log_sender.borrow().as_ref() {
		log_sender.send(string).unwrap();
	}
	Ok(())
}

async fn syscall_read(state: Rc<State>, args: (tg::Blob,)) -> Result<Bytes> {
	let (blob,) = args;
	let bytes = blob.bytes(state.tg.as_ref()).await?;
	Ok(bytes.into())
}

async fn syscall_sleep(_state: Rc<State>, args: (f64,)) -> Result<()> {
	let (duration,) = args;
	let duration = std::time::Duration::from_secs_f64(duration);
	tokio::time::sleep(duration).await;
	Ok(())
}

async fn syscall_store(state: Rc<State>, args: (tg::object::Object,)) -> Result<tg::object::Id> {
	let (object,) = args;
	let handle = tg::object::Handle::with_object(object);
	let id = handle.id(state.tg.as_ref()).await?;
	Ok(id.clone())
}

fn syscall_sync<'s, A, T, F>(
	scope: &mut v8::HandleScope<'s>,
	args: &v8::FunctionCallbackArguments,
	f: F,
) -> Result<v8::Local<'s, v8::Value>>
where
	A: FromV8,
	T: ToV8,
	F: FnOnce(&mut v8::HandleScope<'s>, Rc<State>, A) -> Result<T>,
{
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<Rc<State>>(scope).unwrap().clone();

	// Collect the args.
	let args = (1..args.length()).map(|i| args.get(i)).collect_vec();
	let args = v8::Array::new_with_elements(scope, args.as_slice());

	// Deserialize the args.
	let args = from_v8(scope, args.into()).wrap_err("Failed to deserialize the args.")?;

	// Call the function.
	let value = f(scope, state, args)?;

	// Move the value to v8.
	let value = value
		.to_v8(scope)
		.wrap_err("Failed to serialize the value.")?;

	Ok(value)
}

fn syscall_async<'s, A, T, F, Fut>(
	scope: &mut v8::HandleScope<'s>,
	args: &v8::FunctionCallbackArguments,
	f: F,
) -> Result<v8::Local<'s, v8::Value>>
where
	A: FromV8 + 'static,
	T: ToV8 + 'static,
	F: FnOnce(Rc<State>, A) -> Fut + 'static,
	Fut: Future<Output = Result<T>>,
{
	// Get the context.
	let context = scope.get_current_context();

	// Get the state.
	let state = context.get_slot::<Rc<State>>(scope).unwrap().clone();

	// Create the promise.
	let promise_resolver = v8::PromiseResolver::new(scope).unwrap();
	let promise = promise_resolver.get_promise(scope);

	// Collect the args.
	let args = (1..args.length()).map(|i| args.get(i)).collect_vec();
	let args = v8::Array::new_with_elements(scope, args.as_slice());

	// Deserialize the args.
	let args = from_v8(scope, args.into()).wrap_err("Failed to deserialize the args.")?;

	// Move the promise resolver to the global scope.
	let promise_resolver = v8::Global::new(scope, promise_resolver);

	// Create the future.
	let future = {
		let state = state.clone();
		async move {
			let result = f(state, args)
				.await
				.map(|value| Box::new(value) as Box<dyn ToV8>);
			(result, promise_resolver)
		}
	};
	let future = Box::pin(future);

	// Add the future to the context's futures.
	state.futures.borrow_mut().push(future);

	Ok(promise.into())
}
