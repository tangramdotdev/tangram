#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use super::{
	convert::{from_v8, FromV8, ToV8},
	error::current_stack_trace,
	State,
};
use bytes::Bytes;
use futures::{Future, FutureExt as _, TryStreamExt as _};
use itertools::Itertools as _;
use num::ToPrimitive;
use std::{
	rc::Rc,
	sync::{atomic::AtomicU64, Arc},
};
use tangram_client as tg;
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
		"archive" => async_(scope, &args, archive),
		"build" => async_(scope, &args, build),
		"bundle" => async_(scope, &args, bundle),
		"checksum" => sync(scope, &args, checksum),
		"compress" => async_(scope, &args, compress),
		"decompress" => async_(scope, &args, decompress),
		"download" => async_(scope, &args, download),
		"encoding_base64_decode" => sync(scope, &args, encoding_base64_decode),
		"encoding_base64_encode" => sync(scope, &args, encoding_base64_encode),
		"encoding_hex_decode" => sync(scope, &args, encoding_hex_decode),
		"encoding_hex_encode" => sync(scope, &args, encoding_hex_encode),
		"encoding_json_decode" => sync(scope, &args, encoding_json_decode),
		"encoding_json_encode" => sync(scope, &args, encoding_json_encode),
		"encoding_toml_decode" => sync(scope, &args, encoding_toml_decode),
		"encoding_toml_encode" => sync(scope, &args, encoding_toml_encode),
		"encoding_utf8_decode" => sync(scope, &args, encoding_utf8_decode),
		"encoding_utf8_encode" => sync(scope, &args, encoding_utf8_encode),
		"encoding_yaml_decode" => sync(scope, &args, encoding_yaml_decode),
		"encoding_yaml_encode" => sync(scope, &args, encoding_yaml_encode),
		"extract" => async_(scope, &args, extract),
		"load" => async_(scope, &args, load),
		"log" => sync(scope, &args, log),
		"read" => async_(scope, &args, read),
		"sleep" => async_(scope, &args, sleep),
		"store" => async_(scope, &args, store),
		_ => unreachable!(r#"unknown syscall "{name}""#),
	};

	// Handle the result.
	match result {
		Ok(value) => {
			// Set the return value.
			return_value.set(value);
		},

		Err(error) => {
			// Create the error.
			let stack = current_stack_trace(scope).unwrap_or_default();
			let error = tg::error!(source = error, stack = stack, %name, "the syscall failed");

			// Throw an exception.
			let exception = super::error::to_exception(scope, &error);
			scope.throw_exception(exception);
		},
	}
}

async fn archive(
	state: Rc<State>,
	args: (tg::Artifact, tg::artifact::archive::Format),
) -> tg::Result<tg::Blob> {
	let (artifact, format) = args;
	let blob = artifact.archive(&state.server, format).await.map_err(
		|source| tg::error!(!source,  %artifact, %format, "failed to archive the artifact"),
	)?;
	Ok(blob)
}

async fn build(state: Rc<State>, args: (tg::Target,)) -> tg::Result<tg::Value> {
	let (target,) = args;
	let arg = tg::build::create::Arg {
		parent: Some(state.build.id().clone()),
		remote: false,
		retry: state.build.retry(&state.server).await?,
		target: target.id(&state.server, None).await?,
	};
	let output = target
		.build(&state.server, arg)
		.await
		.map_err(|source| tg::error!(!source, %target, "failed to build the target"))?;
	Ok(output)
}

async fn bundle(state: Rc<State>, args: (tg::Artifact,)) -> tg::Result<tg::Artifact> {
	let (artifact,) = args;
	let artifact = artifact
		.bundle(&state.server)
		.await
		.map_err(|source| tg::error!(!source, %artifact, "failed to bundle the artifact"))?;
	Ok(artifact)
}

fn checksum(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (tg::checksum::Algorithm, Bytes),
) -> tg::Result<tg::Checksum> {
	let (algorithm, bytes) = args;
	let mut checksum_writer = tg::checksum::Writer::new(algorithm);
	checksum_writer.update(&bytes);
	let checksum = checksum_writer.finalize();
	Ok(checksum)
}

async fn compress(
	state: Rc<State>,
	args: (tg::Blob, tg::blob::compress::Format),
) -> tg::Result<tg::Blob> {
	let (blob, format) = args;
	let blob = blob
		.compress(&state.server, format)
		.await
		.map_err(|source| tg::error!(!source, %format, "failed to compress the blob"))?;
	Ok(blob)
}

async fn decompress(
	state: Rc<State>,
	args: (tg::Blob, tg::blob::compress::Format),
) -> tg::Result<tg::Blob> {
	let (blob, format) = args;
	let blob = blob.decompress(&state.server, format).await?;
	Ok(blob)
}

async fn download(state: Rc<State>, args: (Url, tg::Checksum)) -> tg::Result<tg::Blob> {
	let (url, checksum) = args;
	let _permit = state
		.server
		.file_descriptor_semaphore
		.acquire()
		.await
		.unwrap();
	let response = reqwest::get(url.clone())
		.await
		.map_err(|source| tg::error!(!source, %url, "failed to perform the request"))?
		.error_for_status()
		.map_err(|source| tg::error!(!source, %url, "expected a sucess status"))?;

	// Spawn a task to log progress.
	let n = Arc::new(AtomicU64::new(0));
	let content_length = response.content_length();
	let log_task = tokio::spawn({
		let server = state.server.clone();
		let build = state.build.clone();
		let url = url.clone();
		let n = n.clone();
		async move {
			loop {
				let n = n.load(std::sync::atomic::Ordering::Relaxed);
				let message = if let Some(content_length) = content_length {
					let percent = 100.0 * n.to_f64().unwrap() / content_length.to_f64().unwrap();
					format!("downloading from \"{url}\": {n} of {content_length} {percent:.2}%\n")
				} else {
					format!("downloading from \"{url}\": {n}\n")
				};
				build.add_log(&server, message.into()).await.ok();
				tokio::time::sleep(std::time::Duration::from_secs(1)).await;
			}
		}
	});
	let log_task_abort_handle = log_task.abort_handle();
	scopeguard::defer! {
		log_task_abort_handle.abort();
	};

	// Create a stream of chunks.
	let checksum_writer = tg::checksum::Writer::new(checksum.algorithm());
	let checksum_writer = Arc::new(std::sync::Mutex::new(Some(checksum_writer)));
	let stream = response
		.bytes_stream()
		.map_err(std::io::Error::other)
		.inspect_ok({
			let n = n.clone();
			let checksum_writer = checksum_writer.clone();
			move |chunk| {
				n.fetch_add(
					chunk.len().to_u64().unwrap(),
					std::sync::atomic::Ordering::Relaxed,
				);
				checksum_writer
					.lock()
					.unwrap()
					.as_mut()
					.unwrap()
					.update(chunk);
			}
		});

	// Create the blob and validate.
	let blob = tg::Blob::with_reader(&state.server, StreamReader::new(stream), None)
		.await
		.map_err(|source| tg::error!(!source, "failed to create the blob"))?;

	// Abort the log task.
	log_task.abort();

	// Log that the download finished.
	let message = format!("finished download from \"{url}\"\n");
	state
		.build
		.add_log(&state.server, message.into())
		.await
		.map_err(|source| tg::error!(!source, "failed to add the log"))?;

	// Verify the checksum.
	let checksum_writer = checksum_writer.lock().unwrap().take().unwrap();
	let actual = checksum_writer.finalize();
	if actual != checksum {
		let expected = checksum;
		return Err(tg::error!(%url, %actual, %expected, "the checksum did not match"));
	}

	Ok(blob)
}

fn encoding_base64_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> tg::Result<Bytes> {
	let (value,) = args;
	let bytes = data_encoding::BASE64
		.decode(value.as_bytes())
		.map_err(|source| tg::error!(!source, "failed to decode the bytes"))?;
	Ok(bytes.into())
}

fn encoding_base64_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (Bytes,),
) -> tg::Result<String> {
	let (value,) = args;
	let encoded = data_encoding::BASE64.encode(&value);
	Ok(encoded)
}

fn encoding_hex_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> tg::Result<Bytes> {
	let (string,) = args;
	let bytes = data_encoding::HEXLOWER
		.decode(string.as_bytes())
		.map_err(|source| tg::error!(!source, "failed to decode the string as hex"))?;
	Ok(bytes.into())
}

fn encoding_hex_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (Bytes,),
) -> tg::Result<String> {
	let (bytes,) = args;
	let hex = data_encoding::HEXLOWER.encode(&bytes);
	Ok(hex)
}

fn encoding_json_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> tg::Result<serde_json::Value> {
	let (json,) = args;
	let value = serde_json::from_str(&json)
		.map_err(|source| tg::error!(!source, "failed to decode the string as json"))?;
	Ok(value)
}

fn encoding_json_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (serde_json::Value,),
) -> tg::Result<String> {
	let (value,) = args;
	let json = serde_json::to_string(&value)
		.map_err(|source| tg::error!(!source, %value, "failed to encode the value"))?;
	Ok(json)
}

fn encoding_toml_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> tg::Result<toml::Value> {
	let (toml,) = args;
	let value = toml::from_str(&toml)
		.map_err(|source| tg::error!(!source, "failed to decode the string as toml"))?;
	Ok(value)
}

fn encoding_toml_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (toml::Value,),
) -> tg::Result<String> {
	let (value,) = args;
	let toml = toml::to_string(&value)
		.map_err(|source| tg::error!(!source, "failed to encode the value"))?;
	Ok(toml)
}

fn encoding_utf8_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (Bytes,),
) -> tg::Result<String> {
	let (bytes,) = args;
	let string = String::from_utf8(bytes.to_vec())
		.map_err(|source| tg::error!(!source, "failed to decode the bytes as UTF-8"))?;
	Ok(string)
}

fn encoding_utf8_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> tg::Result<Bytes> {
	let (string,) = args;
	let bytes = string.into_bytes().into();
	Ok(bytes)
}

fn encoding_yaml_decode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (String,),
) -> tg::Result<serde_yaml::Value> {
	let (yaml,) = args;
	let value = serde_yaml::from_str(&yaml)
		.map_err(|source| tg::error!(!source, "failed to decode the string as yaml"))?;
	Ok(value)
}

fn encoding_yaml_encode(
	_scope: &mut v8::HandleScope,
	_state: Rc<State>,
	args: (serde_yaml::Value,),
) -> tg::Result<String> {
	let (value,) = args;
	let yaml = serde_yaml::to_string(&value)
		.map_err(|source| tg::error!(!source, "failed to encode the value"))?;
	Ok(yaml)
}

async fn extract(
	state: Rc<State>,
	args: (tg::Blob, tg::artifact::archive::Format),
) -> tg::Result<tg::Artifact> {
	let (blob, format) = args;
	let artifact = tg::Artifact::extract(&state.server, &blob, format)
		.await
		.map_err(|source| tg::error!(!source, %format, "failed to extract the blob"))?;
	Ok(artifact)
}

async fn load(state: Rc<State>, args: (tg::object::Id,)) -> tg::Result<tg::object::Object> {
	let (id,) = args;
	tg::object::Handle::with_id(id).object(&state.server).await
}

fn log(_scope: &mut v8::HandleScope, state: Rc<State>, args: (String,)) -> tg::Result<()> {
	let (string,) = args;
	if let Some(log_sender) = state.log_sender.borrow().as_ref() {
		log_sender.send(string).unwrap();
	}
	Ok(())
}

async fn read(state: Rc<State>, args: (tg::Blob,)) -> tg::Result<Bytes> {
	let (blob,) = args;
	let bytes = blob
		.bytes(&state.server)
		.await
		.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
	Ok(bytes.into())
}

async fn sleep(_state: Rc<State>, args: (f64,)) -> tg::Result<()> {
	let (duration,) = args;
	let duration = std::time::Duration::from_secs_f64(duration);
	tokio::time::sleep(duration).await;
	Ok(())
}

async fn store(state: Rc<State>, args: (tg::object::Object,)) -> tg::Result<tg::object::Id> {
	let (object,) = args;
	let handle = tg::object::Handle::with_object(object);
	let id = handle.id(&state.server, None).await?;
	Ok(id.clone())
}

fn sync<'s, A, T, F>(
	scope: &mut v8::HandleScope<'s>,
	args: &v8::FunctionCallbackArguments,
	f: F,
) -> tg::Result<v8::Local<'s, v8::Value>>
where
	A: FromV8,
	T: ToV8,
	F: FnOnce(&mut v8::HandleScope<'s>, Rc<State>, A) -> tg::Result<T>,
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
		.map_err(|source| tg::error!(!source, "failed to deserialize the args"))?;

	// Call the function.
	let value = f(scope, state, args)?;

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
	let state = context.get_slot::<Rc<State>>(scope).unwrap().clone();

	// Create the promise.
	let promise_resolver = v8::PromiseResolver::new(scope).unwrap();
	let promise = promise_resolver.get_promise(scope);

	// Collect the args.
	let args = (1..args.length()).map(|i| args.get(i)).collect_vec();
	let args = v8::Array::new_with_elements(scope, args.as_slice());

	// Deserialize the args.
	let args = from_v8(scope, args.into())
		.map_err(|source| tg::error!(!source, "failed to deserialize the args"))?;

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
