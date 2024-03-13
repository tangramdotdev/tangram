#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use super::{
	convert::{from_v8, FromV8, ToV8},
	State,
};
use bytes::Bytes;
use futures::{Future, FutureExt, TryStreamExt};
use itertools::Itertools;
use num::ToPrimitive;
use std::rc::Rc;
use tangram_client as tg;
use tangram_error::{error, Result};
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
	let blob = tg::Blob::archive(&state.server, &artifact, format)
		.await
		.map_err(
			|error| error!(source = error,  %artifact, %format, "Failed to archive the artifact."),
		)?;
	Ok(blob)
}

async fn syscall_build(state: Rc<State>, args: (tg::Target,)) -> Result<tg::Value> {
	let (target,) = args;
	let arg = tg::build::GetOrCreateArg {
		parent: Some(state.build.id().clone()),
		remote: false,
		retry: state.build.retry(&state.server).await?,
		target: target.id(&state.server).await?.clone(),
	};
	let output = target
		.build(&state.server, arg)
		.await
		.map_err(|error| error!(source = error, %target, "Failed to build the target."))?;
	Ok(output)
}

async fn syscall_bundle(state: Rc<State>, args: (tg::Artifact,)) -> Result<tg::Artifact> {
	let (artifact,) = args;
	let artifact = artifact
		.bundle(&state.server)
		.await
		.map_err(|error| error!(source = error, %artifact, "Failed to bundle the artifact."))?;
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
	let blob = blob
		.compress(&state.server, format)
		.await
		.map_err(|error| error!(source = error, %blob, %format, "Failed to compress the blob."))?;
	Ok(blob)
}

async fn syscall_decompress(
	state: Rc<State>,
	args: (tg::Blob, tg::blob::CompressionFormat),
) -> Result<tg::Blob> {
	let (blob, format) = args;
	let blob = blob.decompress(&state.server, format).await?;
	Ok(blob)
}

async fn syscall_download(state: Rc<State>, args: (Url, tg::Checksum)) -> Result<tg::Blob> {
	let (url, checksum) = args;
	let _permit = state
		.server
		.inner
		.file_descriptor_semaphore
		.acquire()
		.await
		.unwrap();
	let response = reqwest::get(url.clone())
		.await
		.map_err(|error| error!(source = error, %url, "Failed to perform the request."))?
		.error_for_status()
		.map_err(|error| error!(source = error, %url, "Expected a sucess status."))?;

	// Spawn a task to log progress.
	let content_length = response.content_length();
	let (log_sender, mut log_receiver) = tokio::sync::watch::channel(0);
	let task = tokio::spawn({
		let url = url.clone();
		let server = state.server.clone();
		let build = state.build.clone();
		async move {
			let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
			loop {
				// Get the current number of bytes read from the watch.
				let bytes_read = *log_receiver.borrow();
				let message =
					if let Some(content_length) = content_length {
						let percent =
							100.0 * bytes_read.to_f64().unwrap() / content_length.to_f64().unwrap();
						format!("Downloading \"{url}\": {bytes_read} of {content_length} {percent:.2}%\n")
					} else {
						format!("Downloading \"{url}\": {bytes_read}\n")
					};
				build.add_log(&server, message.into()).await?;

				// Wait for the interval to tick and more data to be available.
				let (status, _) = tokio::join!(log_receiver.changed(), interval.tick());
				if status.is_err() {
					break;
				}
			}
			Ok::<_, tangram_error::Error>(())
		}
	});
	scopeguard::defer! {
		task.abort();
	};

	// Create a stream of chunks.
	let mut checksum_writer = tg::checksum::Writer::new(checksum.algorithm());
	let mut bytes_read = 0;
	let stream = response
		.bytes_stream()
		.map_err(std::io::Error::other)
		.inspect_ok(|chunk| {
			bytes_read += chunk.len().to_u64().unwrap();
			log_sender.send_replace(bytes_read);
			checksum_writer.update(chunk);
		});

	// Create the blob and validate.
	let blob = tg::Blob::with_reader(&state.server, StreamReader::new(stream))
		.await
		.map_err(|error| error!(source = error, "Failed to create the blob."))?;

	// Update the log task with the full content length so it can display 100%.
	if let Some(content_length) = content_length {
		log_sender.send_replace(content_length);
	}

	let actual = checksum_writer.finalize();
	if actual != checksum {
		let expected = checksum;
		return Err(error!(
			source = error!(%actual, %expected, "The checksum did not match."),
			%url,
			"Failed to verify download."
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
	let bytes = data_encoding::BASE64
		.decode(value.as_bytes())
		.map_err(|error| error!(source = error, "Failed to decode the bytes."))?;
	Ok(bytes.into())
}

fn syscall_encoding_base64_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (Bytes,),
) -> Result<String> {
	let (value,) = args;
	let encoded = data_encoding::BASE64.encode(&value);
	Ok(encoded)
}

fn syscall_encoding_hex_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> Result<Bytes> {
	let (string,) = args;
	let bytes = data_encoding::HEXLOWER
		.decode(string.as_bytes())
		.map_err(|error| error!(source = error, "Failed to decode the string as hex."))?;
	Ok(bytes.into())
}

fn syscall_encoding_hex_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (Bytes,),
) -> Result<String> {
	let (bytes,) = args;
	let hex = data_encoding::HEXLOWER.encode(&bytes);
	Ok(hex)
}

fn syscall_encoding_json_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> Result<serde_json::Value> {
	let (json,) = args;
	let value = serde_json::from_str(&json)
		.map_err(|error| error!(source = error, "Failed to decode the string as json."))?;
	Ok(value)
}

fn syscall_encoding_json_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (serde_json::Value,),
) -> Result<String> {
	let (value,) = args;
	let json = serde_json::to_string(&value)
		.map_err(|error| error!(source = error, %value, "Failed to encode the value."))?;
	Ok(json)
}

fn syscall_encoding_toml_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> Result<toml::Value> {
	let (toml,) = args;
	let value = toml::from_str(&toml)
		.map_err(|error| error!(source = error, "Failed to decode the string as toml."))?;
	Ok(value)
}

fn syscall_encoding_toml_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (toml::Value,),
) -> Result<String> {
	let (value,) = args;
	let toml = toml::to_string(&value)
		.map_err(|error| error!(source = error, "Failed to encode the value."))?;
	Ok(toml)
}

fn syscall_encoding_utf8_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (Bytes,),
) -> Result<String> {
	let (bytes,) = args;
	let string = String::from_utf8(bytes.to_vec())
		.map_err(|error| error!(source = error, "Failed to decode the bytes as UTF-8."))?;
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
	let value = serde_yaml::from_str(&yaml)
		.map_err(|error| error!(source = error, "Failed to decode the string as yaml."))?;
	Ok(value)
}

fn syscall_encoding_yaml_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (serde_yaml::Value,),
) -> Result<String> {
	let (value,) = args;
	let yaml = serde_yaml::to_string(&value)
		.map_err(|error| error!(source = error, "Failed to encode the value."))?;
	Ok(yaml)
}

async fn syscall_extract(
	state: Rc<State>,
	args: (tg::Blob, tg::blob::ArchiveFormat),
) -> Result<tg::Artifact> {
	let (blob, format) = args;
	let artifact = blob
		.extract(&state.server, format)
		.await
		.map_err(|error| error!(source = error, %blob, %format, "Failed to extract the blob."))?;
	Ok(artifact)
}

async fn syscall_load(state: Rc<State>, args: (tg::object::Id,)) -> Result<tg::object::Object> {
	let (id,) = args;
	tg::object::Handle::with_id(id).object(&state.server).await
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
	let bytes = blob
		.bytes(&state.server)
		.await
		.map_err(|error| error!(source = error, %blob, "Failed to read the blob."))?;
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
	let id = handle.id(&state.server).await?;
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
	let args = from_v8(scope, args.into())
		.map_err(|error| error!(source = error, "Failed to deserialize the args."))?;

	// Call the function.
	let value = f(scope, state, args)?;

	// Move the value to v8.
	let value = value
		.to_v8(scope)
		.map_err(|error| error!(source = error, "Failed to serialize the value."))?;

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
	let args = from_v8(scope, args.into())
		.map_err(|error| error!(source = error, "Failed to deserialize the args."))?;

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
		.boxed_local()
	};

	// Add the future to the context's futures.
	state.futures.borrow_mut().push(future);

	Ok(promise.into())
}
