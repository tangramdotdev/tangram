use {
	super::State,
	bytes::Bytes,
	futures::{SinkExt as _, StreamExt as _, TryStreamExt as _, future},
	std::{io::Cursor, pin::pin, rc::Rc},
	tangram_client::prelude::*,
	tangram_v8::Serde,
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

pub async fn checkin(
	state: Rc<State>,
	args: (Serde<tg::checkin::Arg>,),
) -> tg::Result<Serde<tg::artifact::Id>> {
	let (Serde(arg),) = args;
	let handle = state.handle.clone();
	let artifact = state
		.main_runtime_handle
		.spawn(async move {
			let artifact = tg::checkin::checkin(&handle, arg).await?;
			Ok::<_, tg::Error>(artifact)
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to check in the output"))?;
	Ok(Serde(artifact.id().clone()))
}

pub async fn checksum(
	_state: Rc<State>,
	args: (tg::Either<String, Bytes>, Serde<tg::checksum::Algorithm>),
) -> tg::Result<Serde<tg::Checksum>> {
	let (bytes, Serde(algorithm)) = args;
	let bytes = match &bytes {
		tg::Either::Left(string) => string.as_bytes(),
		tg::Either::Right(bytes) => bytes.as_ref(),
	};
	let mut writer = tg::checksum::Writer::new(algorithm);
	writer.update(bytes);
	let checksum = writer.finalize();
	Ok(Serde(checksum))
}

pub async fn checkout(state: Rc<State>, args: (Serde<tg::checkout::Arg>,)) -> tg::Result<String> {
	let (Serde(arg),) = args;
	let handle = state.handle.clone();
	let path = state
		.main_runtime_handle
		.spawn(async move {
			let path = tg::checkout::checkout(&handle, arg).await?;
			Ok::<_, tg::Error>(path)
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to check out the artifact"))?;
	let path = path
		.into_os_string()
		.into_string()
		.map_err(|path| tg::error!(?path, "failed to convert the checkout path"))?;
	Ok(path)
}

pub async fn object_batch(state: Rc<State>, args: (Serde<ObjectBatchArg>,)) -> tg::Result<()> {
	let (Serde(arg),) = args;
	if arg.objects.is_empty() {
		return Ok(());
	}
	let handle = state.handle.clone();
	state
		.main_runtime_handle
		.spawn(async move {
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
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to post object batch"))?;
	Ok(())
}

pub async fn object_get(
	state: Rc<State>,
	args: (Serde<tg::object::Id>,),
) -> tg::Result<Serde<tg::object::Data>> {
	let (Serde(id),) = args;
	let handle = state.handle.clone();
	let data = state
		.main_runtime_handle
		.spawn({
			let id = id.clone();
			async move {
				let arg = tg::object::get::Arg::default();
				let tg::object::get::Output { bytes, .. } = handle.get_object(&id, arg).await?;
				let data = tg::object::Data::deserialize(id.kind(), bytes)?;
				Ok::<_, tg::Error>(data)
			}
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
	Ok(Serde(data))
}

pub fn object_id(
	_state: Rc<State>,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (Serde<tg::object::Data>,),
) -> tg::Result<Serde<tg::object::Id>> {
	let (Serde(data),) = args;
	let bytes = data.serialize()?;
	let id = tg::object::Id::new(data.kind(), &bytes);
	Ok(Serde(id))
}

pub async fn process_get(
	state: Rc<State>,
	args: (Serde<tg::process::Id>, Option<String>),
) -> tg::Result<Serde<tg::process::Data>> {
	let (Serde(id), _) = args;
	let handle = state.handle.clone();
	let data = state
		.main_runtime_handle
		.spawn(async move {
			let tg::process::get::Output { data, .. } = handle.get_process(&id).await?;
			Ok::<_, tg::Error>(data)
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to get the process"))?;
	Ok(Serde(data))
}

pub fn process_id(
	_state: Rc<State>,
	_scope: &mut v8::PinScope<'_, '_>,
	_args: (Option<String>,),
) -> tg::Result<Serde<tg::process::Id>> {
	Ok(Serde(tg::process::Id::new()))
}

pub async fn process_stdio_read_close(state: Rc<State>, args: (usize,)) -> tg::Result<()> {
	let (token,) = args;
	state.process_stdio_readers.lock().await.remove(&token);
	Ok(())
}

pub async fn process_stdio_read_read(
	state: Rc<State>,
	args: (usize,),
) -> tg::Result<Serde<Option<tg::process::stdio::read::Event>>> {
	let (token,) = args;
	let reader = state.process_stdio_readers.lock().await.remove(&token);
	let Some(mut reader) = reader else {
		return Ok(Serde(None));
	};
	let event = reader.next().await.transpose()?;
	if event
		.as_ref()
		.is_some_and(|event| !matches!(event, tg::process::stdio::read::Event::End))
	{
		state
			.process_stdio_readers
			.lock()
			.await
			.insert(token, reader);
	}
	Ok(Serde(event))
}

pub async fn process_stdio_read_open(
	state: Rc<State>,
	args: (Serde<tg::process::Id>, Serde<tg::process::stdio::read::Arg>),
) -> tg::Result<Option<usize>> {
	let (Serde(id), Serde(arg)) = args;
	let handle = state.handle.clone();
	let stream = state
		.main_runtime_handle
		.spawn(async move {
			let stream = handle
				.try_read_process_stdio_all(&id, arg)
				.await?
				.map(futures::StreamExt::boxed);
			Ok::<_, tg::Error>(stream)
		})
		.await
		.unwrap()?;
	let Some(stream) = stream else {
		return Ok(None);
	};
	let token = state
		.next_process_stdio_token
		.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
		+ 1;
	state
		.process_stdio_readers
		.lock()
		.await
		.insert(token, stream);
	Ok(Some(token))
}

pub async fn process_stdio_write_close(state: Rc<State>, args: (usize,)) -> tg::Result<()> {
	let (token,) = args;
	let writer = state.process_stdio_writers.lock().await.remove(&token);
	let Some(mut writer) = writer else {
		return Ok(());
	};
	let _ = writer
		.sender
		.send(Ok(tg::process::stdio::read::Event::End))
		.await;
	drop(writer.sender);
	writer
		.task
		.await
		.map_err(|source| tg::error!(!source, "the task panicked"))?
}

pub async fn process_stdio_write_open(
	state: Rc<State>,
	args: (
		Serde<tg::process::Id>,
		Serde<tg::process::stdio::write::Arg>,
	),
) -> tg::Result<usize> {
	let (Serde(id), Serde(arg)) = args;
	let (sender, receiver) = futures::channel::mpsc::channel(16);
	let handle = state.handle.clone();
	let task = state.main_runtime_handle.spawn(async move {
		let input = receiver.boxed();
		handle.write_process_stdio_all(&id, arg, input).await
	});
	let token = state
		.next_process_stdio_token
		.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
		+ 1;
	state
		.process_stdio_writers
		.lock()
		.await
		.insert(token, crate::v8::ProcessStdioWriter { sender, task });
	Ok(token)
}

pub async fn process_stdio_write_write(
	state: Rc<State>,
	args: (usize, Serde<tg::process::stdio::Chunk>),
) -> tg::Result<()> {
	let (token, Serde(chunk)) = args;
	let mut sender = {
		let writers = state.process_stdio_writers.lock().await;
		let writer = writers
			.get(&token)
			.ok_or_else(|| tg::error!(%token, "failed to find the process stdio writer"))?;
		writer.sender.clone()
	};
	let event = tg::process::stdio::read::Event::Chunk(chunk);
	sender
		.send(Ok(event))
		.await
		.map_err(|source| tg::error!(!source, %token, "failed to send the process stdio event"))
}

pub async fn process_signal(
	state: Rc<State>,
	args: (
		Serde<tg::process::Id>,
		Serde<tg::process::signal::post::Arg>,
	),
) -> tg::Result<()> {
	let (Serde(id), Serde(arg)) = args;
	let handle = state.handle.clone();
	state
		.main_runtime_handle
		.spawn(async move {
			handle.signal_process(&id, arg).await?;
			Ok::<_, tg::Error>(())
		})
		.await
		.unwrap()?;
	Ok(())
}

pub async fn process_spawn(
	state: Rc<State>,
	args: (Serde<tg::process::spawn::Arg>,),
) -> tg::Result<Serde<tg::process::spawn::Output>> {
	let (Serde(arg),) = args;
	let stream = state
		.main_runtime_handle
		.spawn({
			let handle = state.handle.clone();
			async move {
				let stream = handle
					.try_spawn_process(arg)
					.await?
					.and_then(|event| {
						let result =
							event.try_map_output(|output: Option<tg::process::spawn::Output>| {
								output.ok_or_else(|| tg::error!("expected a process"))
							});
						future::ready(result)
					})
					.boxed();
				Ok::<_, tg::Error>(stream)
			}
		})
		.await
		.unwrap()?;
	let writer = std::io::stderr();
	let handle = state.handle.clone();
	let output = tg::progress::write_progress_stream(&handle, stream, writer, false).await?;
	Ok(Serde(output))
}

pub async fn process_tty_size_put(
	state: Rc<State>,
	args: (
		Serde<tg::process::Id>,
		Serde<tg::process::tty::size::put::Arg>,
	),
) -> tg::Result<()> {
	let (Serde(id), Serde(arg)) = args;
	let handle = state.handle.clone();
	state
		.main_runtime_handle
		.spawn(async move {
			handle.set_process_tty_size(&id, arg).await?;
			Ok::<_, tg::Error>(())
		})
		.await
		.unwrap()?;
	Ok(())
}

pub async fn process_wait(
	state: Rc<State>,
	args: (Serde<tg::process::Id>, Serde<tg::process::wait::Arg>),
) -> tg::Result<Serde<tg::process::wait::Output>> {
	let (Serde(id), Serde(arg)) = args;
	let handle = state.handle.clone();
	let output = state
		.main_runtime_handle
		.spawn(async move {
			let output = handle.wait_process(&id, arg).await?;
			Ok::<_, tg::Error>(output)
		})
		.await
		.unwrap()?;
	Ok(Serde(output))
}

pub async fn read(state: Rc<State>, args: (Serde<tg::read::Arg>,)) -> tg::Result<Bytes> {
	let (Serde(arg),) = args;
	let handle = state.handle.clone();
	let bytes = state
		.main_runtime_handle
		.spawn(async move {
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
			Ok::<_, tg::Error>(buffer.into())
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to read the blob"))?;
	Ok(bytes)
}

pub fn value_parse(
	_state: Rc<State>,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (String,),
) -> tg::Result<Serde<tg::value::Data>> {
	let (value,) = args;
	let value = value
		.parse::<tg::Value>()
		.map_err(|source| tg::error!(!source, "failed to parse the value"))?;
	Ok(Serde(value.to_data()))
}

pub fn value_stringify(
	_state: Rc<State>,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (Serde<tg::value::Data>,),
) -> tg::Result<String> {
	let (value,) = args;
	let Serde(value) = value;
	let value = tg::Value::try_from_data(value)
		.map_err(|source| tg::error!(!source, "failed to convert the value"))?;
	Ok(value.to_string())
}

pub async fn write(
	state: Rc<State>,
	args: (tg::Either<String, Bytes>,),
) -> tg::Result<Serde<tg::blob::Id>> {
	let (bytes,) = args;
	let reader = Cursor::new(bytes);
	let handle = state.handle.clone();
	let blob = state
		.main_runtime_handle
		.spawn(async move {
			let arg = tg::write::Arg::default();
			let tg::write::Output { blob } = handle.write(arg, reader).await?;
			Ok::<_, tg::Error>(blob)
		})
		.await
		.unwrap()
		.map_err(|source| tg::error!(!source, "failed to create the blob"))?;
	Ok(Serde(blob))
}
