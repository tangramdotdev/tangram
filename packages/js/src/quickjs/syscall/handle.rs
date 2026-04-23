use {
	super::Result,
	crate::quickjs::{
		StateHandle,
		serde::Serde,
		types::{Either, Uint8Array},
	},
	futures::{StreamExt as _, TryStreamExt as _, future},
	rquickjs as qjs,
	std::{io::Cursor, pin::pin},
	tangram_client::prelude::*,
	tokio::io::AsyncReadExt as _,
	tokio_util::io::StreamReader,
};

#[derive(serde::Deserialize)]
pub(super) struct ObjectBatchArg {
	objects: Vec<ObjectBatchObject>,
}

#[derive(serde::Deserialize)]
struct ObjectBatchObject {
	data: tg::object::Data,
	id: tg::object::Id,
}

#[derive(serde::Serialize)]
pub struct HandleArg {
	process: Option<tg::process::Id>,
	token: Option<String>,
	url: Option<String>,
}

pub fn arg(ctx: qjs::Ctx<'_>, _value: Option<String>) -> Result<Serde<HandleArg>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let arg = state.handle.arg();
	Result(Ok(Serde(HandleArg {
		process: arg.process,
		token: arg.token,
		url: arg.url.map(|url| url.to_string()),
	})))
}

pub async fn checkin(
	ctx: qjs::Ctx<'_>,
	arg: Serde<tg::checkin::Arg>,
) -> Result<Serde<tg::artifact::Id>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(arg) = arg;
	let result = async {
		let artifact = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let artifact = tg::checkin::checkin_with_handle(&handle, arg).await?;
					Ok::<_, tg::Error>(artifact)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
		Ok(Serde(artifact.id().clone()))
	}
	.await;
	Result(result)
}

pub async fn checksum(
	bytes: Either<String, Uint8Array>,
	algorithm: Serde<tg::checksum::Algorithm>,
) -> Result<Serde<tg::Checksum>> {
	let bytes = match &bytes {
		Either::Left(left) => left.as_bytes(),
		Either::Right(right) => right.0.as_ref(),
	};
	let Serde(algorithm) = algorithm;
	let mut writer = tg::checksum::Writer::new(algorithm);
	writer.update(bytes);
	let checksum = writer.finalize();
	Result(Ok(Serde(checksum)))
}

pub async fn checkout(ctx: qjs::Ctx<'_>, arg: Serde<tg::checkout::Arg>) -> Result<String> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(arg) = arg;
	let result = async {
		let path = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let path = tg::checkout::checkout_with_handle(&handle, arg).await?;
					Ok::<_, tg::Error>(path)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;
		let path = path
			.into_os_string()
			.into_string()
			.map_err(|path| tg::error!(?path, "failed to convert the checkout path"))?;
		Ok(path)
	}
	.await;
	Result(result)
}

pub async fn object_batch(ctx: qjs::Ctx<'_>, arg: Serde<ObjectBatchArg>) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(arg) = arg;
	let result = async {
		if arg.objects.is_empty() {
			return Ok(());
		}
		state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let mut batch_objects = Vec::with_capacity(arg.objects.len());
					for object in arg.objects {
						let bytes = object.data.serialize()?;
						batch_objects.push(tg::object::batch::Object {
							id: object.id,
							bytes,
						});
					}
					let arg = tg::object::batch::Arg {
						objects: batch_objects,
						..Default::default()
					};
					handle.post_object_batch(arg).await?;
					Ok::<_, tg::Error>(())
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to post object batch"))?;
		Ok(())
	}
	.await;
	Result(result)
}

pub async fn object_get(
	ctx: qjs::Ctx<'_>,
	id: Serde<tg::object::Id>,
) -> Result<Serde<tg::object::Data>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let result = async {
		let data = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				let id = id.clone();
				async move {
					let arg = tg::object::get::Arg::default();
					let tg::object::get::Output { bytes, .. } = handle.get_object(&id, arg).await?;
					let data = tg::object::Data::deserialize(id.kind(), bytes)?;
					Ok::<_, tg::Error>(data)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to get the object"))?;
		Ok(data)
	}
	.await;
	Result(result.map(Serde))
}

pub fn object_id(data: Serde<tg::object::Data>) -> Result<Serde<tg::object::Id>> {
	let Serde(data) = data;
	let result = (|| {
		let bytes = data.serialize()?;
		let id = tg::object::Id::new(data.kind(), &bytes);
		Ok(id)
	})();
	Result(result.map(Serde))
}

pub async fn process_get(
	ctx: qjs::Ctx<'_>,
	id: Serde<tg::process::Id>,
	arg: Option<Serde<tg::process::get::Arg>>,
) -> Result<Serde<tg::process::get::Output>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let arg = arg.map(|Serde(arg)| arg).unwrap_or_default();
	let result = async {
		let output = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				let id = id.clone();
				async move {
					let output = handle
						.try_get_process(&id, arg)
						.await?
						.ok_or_else(|| tg::error!("failed to find the process"))?;
					Ok::<_, tg::Error>(output)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to get the process"))?;
		Ok(output)
	}
	.await;
	Result(result.map(Serde))
}

pub async fn sandbox_get(
	ctx: qjs::Ctx<'_>,
	id: Serde<tg::sandbox::Id>,
) -> Result<Serde<tg::sandbox::get::Output>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let result = async {
		let data = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				let id = id.clone();
				async move {
					let output = handle.get_sandbox(&id).await?;
					Ok::<_, tg::Error>(output)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to get the sandbox"))?;
		Ok(data)
	}
	.await;
	Result(result.map(Serde))
}

pub async fn process_stdio_read_close(ctx: qjs::Ctx<'_>, token: usize) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	state.stdio.process_stdio_read_close(token).await;
	Result(Ok(()))
}

pub async fn process_stdio_read_read(
	ctx: qjs::Ctx<'_>,
	token: usize,
) -> Result<Serde<Option<tg::process::stdio::read::Event>>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let result = async {
		let event = state.stdio.process_stdio_read_read(token).await?;
		Ok(event)
	}
	.await;
	Result(result.map(Serde))
}

pub async fn process_stdio_read_open(
	ctx: qjs::Ctx<'_>,
	id: Serde<tg::process::Id>,
	arg: Serde<tg::process::stdio::read::Arg>,
) -> Result<Option<usize>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let Serde(arg) = arg;
	let result = state.stdio.process_stdio_read_open(id, arg).await;
	Result(result)
}

pub async fn process_stdio_write_close(ctx: qjs::Ctx<'_>, token: usize) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let result = state.stdio.process_stdio_write_close(token).await;
	Result(result)
}

pub async fn process_stdio_write_open(
	ctx: qjs::Ctx<'_>,
	id: Serde<tg::process::Id>,
	arg: Serde<tg::process::stdio::write::Arg>,
) -> Result<usize> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let Serde(arg) = arg;
	let result = state.stdio.process_stdio_write_open(id, arg).await;
	Result(result)
}

pub async fn process_stdio_write_write(
	ctx: qjs::Ctx<'_>,
	token: usize,
	chunk: Serde<tg::process::stdio::Chunk>,
) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(chunk) = chunk;
	let result = state.stdio.process_stdio_write_write(token, chunk).await;
	Result(result)
}

pub async fn process_signal(
	ctx: qjs::Ctx<'_>,
	id: Serde<tg::process::Id>,
	arg: Serde<tg::process::signal::post::Arg>,
) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let Serde(arg) = arg;
	let result = async {
		state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					handle.signal_process(&id, arg).await?;
					Ok::<_, tg::Error>(())
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))??;
		Ok(())
	}
	.await;
	Result(result)
}

pub async fn process_spawn(
	ctx: qjs::Ctx<'_>,
	arg: Serde<tg::process::spawn::Arg>,
) -> Result<Serde<tg::process::spawn::Output>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(arg) = arg;
	let result = async {
		let stream = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let stream = handle
						.try_spawn_process(arg)
						.await?
						.and_then(|event| {
							let result = event.try_map_output(
								|output: Option<tg::process::spawn::Output>| {
									output.ok_or_else(|| tg::error!("expected a process"))
								},
							);
							future::ready(result)
						})
						.boxed();
					Ok::<_, tg::Error>(stream)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))??;
		let writer = std::io::stderr();
		let handle = state.handle.clone();
		let output = tg::progress::write_progress_stream(&handle, stream, writer, false).await?;
		Ok(output)
	}
	.await;
	Result(result.map(Serde))
}

pub async fn process_tty_size_put(
	ctx: qjs::Ctx<'_>,
	id: Serde<tg::process::Id>,
	arg: Serde<tg::process::tty::size::put::Arg>,
) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let Serde(arg) = arg;
	let result = async {
		state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					handle.set_process_tty_size(&id, arg).await?;
					Ok::<_, tg::Error>(())
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))??;
		Ok(())
	}
	.await;
	Result(result)
}

pub async fn process_wait(
	ctx: qjs::Ctx<'_>,
	id: Serde<tg::process::Id>,
	arg: Serde<tg::process::wait::Arg>,
) -> Result<Serde<tg::process::wait::Output>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let Serde(arg) = arg;
	let result = async {
		let output = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let output = handle.wait_process(&id, arg).await?;
					Ok::<_, tg::Error>(output)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))??;
		Ok(output)
	}
	.await;
	Result(result.map(Serde))
}

pub async fn read(ctx: qjs::Ctx<'_>, arg: Serde<tg::read::Arg>) -> Result<Uint8Array> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(arg) = arg;
	let result = async {
		let bytes = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let stream = handle.read(arg).await?;
					let reader = StreamReader::new(
						stream
							.map_ok(|chunk| chunk.bytes)
							.map_err(std::io::Error::other),
					);
					let mut buffer = Vec::new();
					pin!(reader)
						.read_to_end(&mut buffer)
						.await
						.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
					Ok::<_, tg::Error>(buffer)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
		Ok(Uint8Array::from(bytes))
	}
	.await;
	Result(result)
}

pub fn value_parse(_ctx: qjs::Ctx<'_>, value: String) -> Result<Serde<tg::value::Data>> {
	let result = value
		.parse::<tg::Value>()
		.map(|value| Serde(value.to_data()))
		.map_err(|source| tg::error!(!source, "failed to parse the value"));
	Result(result)
}

pub fn value_stringify(_ctx: qjs::Ctx<'_>, value: Serde<tg::value::Data>) -> Result<String> {
	let Serde(value) = value;
	let result = tg::Value::try_from_data(value)
		.map(|value| value.to_string())
		.map_err(|source| tg::error!(!source, "failed to convert the value"));
	Result(result)
}

pub async fn write(
	ctx: qjs::Ctx<'_>,
	bytes: Either<String, Uint8Array>,
) -> Result<Serde<tg::blob::Id>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let bytes: Vec<u8> = match bytes {
		Either::Left(left) => left.into_bytes(),
		Either::Right(right) => right.0.to_vec(),
	};
	let result = async {
		let reader = Cursor::new(bytes);
		let blob_id = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				async move {
					let arg = tg::write::Arg::default();
					let tg::write::Output { blob } = handle.write(arg, reader).await?;
					Ok::<_, tg::Error>(blob)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to create the blob"))?;
		Ok(Serde(blob_id))
	}
	.await;
	Result(result)
}
