use {
	super::{Arg, Result},
	crate::quickjs::{
		StateHandle,
		types::{Either, Uint8Array},
	},
	rquickjs as qjs,
	tangram_client::prelude::*,
	tangram_quickjs::Serde,
};

pub async fn close(ctx: qjs::Ctx<'_>, fd: i32) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.host.close(fd).await)
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

pub fn current(ctx: qjs::Ctx<'_>) -> Result<Option<String>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(Ok(state.arg.host.clone()))
}

pub async fn disable_raw_mode(ctx: qjs::Ctx<'_>, fd: i32) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.host.disable_raw_mode(fd).await)
}

pub async fn enable_raw_mode(ctx: qjs::Ctx<'_>, fd: i32) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.host.enable_raw_mode(fd).await)
}

pub async fn exec(ctx: qjs::Ctx<'_>, arg: Serde<crate::host::SpawnArg>) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(arg) = arg;
	Result(state.host.exec(arg).await)
}

pub async fn exists(ctx: qjs::Ctx<'_>, path: String) -> Result<bool> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.host.exists(path).await)
}

pub fn get_tty_size(_ctx: qjs::Ctx<'_>) -> Result<Serde<Option<tg::process::tty::Size>>> {
	Result(Ok(Serde(crate::host::Host::get_tty_size())))
}

pub async fn getxattr(ctx: qjs::Ctx<'_>, path: String, name: String) -> Result<Option<Uint8Array>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let result = state
		.host
		.getxattr(path, name)
		.await
		.map(|bytes| bytes.map(Uint8Array::from));
	Result(result)
}

pub fn is_tty(_ctx: qjs::Ctx<'_>, fd: i32) -> Result<bool> {
	Result(Ok(crate::host::Host::is_tty(fd)))
}

pub fn is_foreground_controlling_tty(_ctx: qjs::Ctx<'_>, fd: i32) -> Result<bool> {
	Result(Ok(crate::host::Host::is_foreground_controlling_tty(fd)))
}

pub async fn listen_signal_close(ctx: qjs::Ctx<'_>, token: usize) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	state.host.listen_signal_close(token).await;
	Result(Ok(()))
}

pub async fn listen_signal_read(ctx: qjs::Ctx<'_>, token: usize) -> Result<bool> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(
		state
			.host
			.listen_signal_read(token)
			.await
			.map(|value| value.is_some()),
	)
}

pub async fn listen_signal_open(
	ctx: qjs::Ctx<'_>,
	kind: Serde<crate::host::SignalKind>,
) -> Result<usize> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(kind) = kind;
	Result(state.host.listen_signal_open(kind).await)
}

pub fn magic<'js>(
	ctx: qjs::Ctx<'js>,
	function: qjs::Function<'js>,
) -> Result<Serde<tg::command::data::Executable>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let name: Option<String> = function.get("name").ok();
	let file_name: Option<String> = function.get("fileName").ok();
	let module = file_name
		.as_deref()
		.and_then(|file_name| {
			file_name.parse::<tg::module::Data>().ok().or_else(|| {
				let modules = state.modules.borrow();
				modules.iter().find_map(|module_info| {
					let name = module_info.module.to_string();
					if file_name == name || file_name.contains(&name) {
						Some(module_info.module.clone())
					} else {
						None
					}
				})
			})
		})
		.ok_or_else(|| tg::error!("failed to find the module for the function"));
	let module = match module {
		Ok(module) => module,
		Err(error) => return Result(Err(error)),
	};
	let executable = tg::command::data::Executable::Module(tg::command::data::ModuleExecutable {
		module,
		export: name,
	});
	Result(Ok(Serde(executable)))
}

pub async fn mkdtemp(ctx: qjs::Ctx<'_>) -> Result<String> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.host.mkdtemp().await)
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

pub fn parallelism(_ctx: qjs::Ctx<'_>) -> Result<usize> {
	Result(Ok(crate::host::Host::parallelism()))
}

pub fn value_parse(_ctx: qjs::Ctx<'_>, value: String) -> Result<Serde<tg::value::Data>> {
	let result = value
		.parse::<tg::Value>()
		.map(|value| Serde(value.to_data()))
		.map_err(|error| tg::error!(!error, "failed to parse the value"));
	Result(result)
}

pub fn value_stringify(_ctx: qjs::Ctx<'_>, value: Serde<tg::value::Data>) -> Result<String> {
	let Serde(value) = value;
	let result = tg::Value::try_from_data(value)
		.map(|value| value.to_string())
		.map_err(|error| tg::error!(!error, "failed to convert the value"));
	Result(result)
}

pub async fn read(
	ctx: qjs::Ctx<'_>,
	fd: i32,
	length: Arg<Option<usize>>,
	stopper: Arg<Option<usize>>,
) -> Result<Option<Uint8Array>> {
	let Arg(length) = length;
	let Arg(stopper) = stopper;
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let result = state
		.host
		.read(fd, length, stopper)
		.await
		.map(|bytes| bytes.map(Uint8Array::from));
	Result(result)
}

pub async fn remove(ctx: qjs::Ctx<'_>, path: String) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.host.remove(path).await)
}

pub async fn signal(ctx: qjs::Ctx<'_>, pid: u32, signal: Serde<tg::process::Signal>) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(signal) = signal;
	Result(state.host.signal(pid, signal).await)
}

pub async fn sleep(ctx: qjs::Ctx<'_>, duration: f64, stopper: Arg<Option<usize>>) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Arg(stopper) = stopper;
	Result(state.host.sleep(duration, stopper).await)
}

pub async fn spawn(
	ctx: qjs::Ctx<'_>,
	arg: Serde<crate::host::SpawnArg>,
) -> Result<Serde<crate::host::SpawnOutput>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	if state.arg.host.as_deref() == Some("js") {
		return Result(Err(tg::error!(
			"cannot spawn a host process when the host is js"
		)));
	}
	let Serde(arg) = arg;
	let result = state.host.spawn(arg).await.map(Serde);
	Result(result)
}

pub async fn stopper_close(ctx: qjs::Ctx<'_>, stopper: usize) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.host.stopper_close(stopper).await)
}

pub async fn stopper_open(ctx: qjs::Ctx<'_>) -> Result<usize> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.host.stopper_open().await)
}

pub async fn stopper_stop(ctx: qjs::Ctx<'_>, stopper: usize) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.host.stopper_stop(stopper).await)
}

pub async fn wait(
	ctx: qjs::Ctx<'_>,
	pid: u32,
	stopper: Arg<Option<usize>>,
) -> Result<Serde<crate::host::WaitOutput>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Arg(stopper) = stopper;
	let result = state.host.wait(pid, stopper).await.map(Serde);
	Result(result)
}

pub async fn write(ctx: qjs::Ctx<'_>, fd: i32, bytes: Uint8Array) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	Result(state.host.write(fd, bytes.into()).await)
}

pub fn write_sync(_ctx: qjs::Ctx<'_>, fd: i32, bytes: Uint8Array) -> Result<()> {
	Result(crate::host::Host::write_sync(fd, bytes.0.as_ref()))
}
