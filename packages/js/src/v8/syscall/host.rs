use {
	super::State,
	bytes::Bytes,
	std::{rc::Rc, time::Duration},
	tangram_client::prelude::*,
	tangram_v8::{Serde, Serialize as _},
};

pub async fn close(state: Rc<State>, args: (i32,)) -> tg::Result<()> {
	let (fd,) = args;
	state.host.close(fd).await
}

pub async fn exists(state: Rc<State>, args: (String,)) -> tg::Result<bool> {
	let (path,) = args;
	state.host.exists(path).await
}

pub fn get_tty_size(
	_state: Rc<State>,
	_scope: &mut v8::PinScope<'_, '_>,
	_args: (Option<String>,),
) -> tg::Result<Serde<Option<tg::process::tty::Size>>> {
	Ok(Serde(crate::host::Host::get_tty_size()))
}

pub async fn getxattr(state: Rc<State>, args: (String, String)) -> tg::Result<Option<Bytes>> {
	let (path, name) = args;
	state.host.getxattr(path, name).await
}

pub fn is_tty(
	_state: Rc<State>,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (i32,),
) -> tg::Result<bool> {
	let (fd,) = args;
	Ok(crate::host::Host::is_tty(fd))
}

pub async fn listen_signal_close(state: Rc<State>, args: (usize,)) -> tg::Result<()> {
	let (token,) = args;
	state.host.listen_signal_close(token).await;
	Ok(())
}

pub async fn listen_signal_read(state: Rc<State>, args: (usize,)) -> tg::Result<bool> {
	let (token,) = args;
	state
		.host
		.listen_signal_read(token)
		.await
		.map(|value| value.is_some())
}

pub async fn listen_signal_open(
	state: Rc<State>,
	args: (Serde<crate::host::SignalKind>,),
) -> tg::Result<usize> {
	let (Serde(kind),) = args;
	state.host.listen_signal_open(kind).await
}

pub fn magic<'s>(
	scope: &mut v8::PinScope<'s, '_>,
	args: &v8::FunctionCallbackArguments,
) -> tg::Result<v8::Local<'s, v8::Value>> {
	let context = scope.get_current_context();
	let state = context.get_slot::<State>().unwrap().clone();
	let arg = args.get(1);
	let function = v8::Local::<v8::Function>::try_from(arg)
		.ok()
		.ok_or_else(|| tg::error!("expected a function"))?;
	let modules = state.modules.borrow();
	let mut module = None;
	for module_ in modules.iter() {
		if let Some(v8_module) = &module_.v8 {
			let v8_module = v8::Local::new(scope, v8_module);
			if v8_module.script_id() == Some(function.get_script_origin().script_id()) {
				module = Some(module_.data.clone());
			}
		}
	}
	let module = module.ok_or_else(|| tg::error!("failed to find the module for the function"))?;
	let export = Some(function.get_name(scope).to_rust_string_lossy(scope));
	let executable = tg::command::data::Executable::Module(tg::command::data::ModuleExecutable {
		module,
		export,
	});
	let value = Serde(executable).serialize(scope)?;
	Ok(value)
}

pub async fn mkdtemp(state: Rc<State>, _args: (Option<String>,)) -> tg::Result<String> {
	state.host.mkdtemp().await
}

pub async fn read(state: Rc<State>, args: (i32, Option<usize>)) -> tg::Result<Option<Bytes>> {
	let (fd, length) = args;
	state.host.read(fd, length).await
}

pub async fn remove(state: Rc<State>, args: (String,)) -> tg::Result<()> {
	let (path,) = args;
	state.host.remove(path).await
}

pub async fn signal(state: Rc<State>, args: (u32, Serde<tg::process::Signal>)) -> tg::Result<()> {
	let (pid, Serde(signal)) = args;
	state.host.signal(pid, signal).await
}

pub async fn sleep(_state: Rc<State>, args: (f64,)) -> tg::Result<()> {
	let (duration,) = args;
	let duration = Duration::from_secs_f64(duration);
	tokio::time::sleep(duration).await;
	Ok(())
}

pub async fn spawn(
	state: Rc<State>,
	args: (Serde<crate::host::SpawnArg>,),
) -> tg::Result<Serde<crate::host::SpawnOutput>> {
	let (Serde(arg),) = args;
	let output = state.host.spawn(arg).await?;
	Ok(Serde(output))
}

pub async fn stdin_close(state: Rc<State>, args: (usize,)) -> tg::Result<()> {
	let (token,) = args;
	state.host.stdin_close(token).await;
	Ok(())
}

pub async fn stdin_open(state: Rc<State>, _args: (Option<String>,)) -> tg::Result<usize> {
	state.host.stdin_open().await
}

pub async fn stdin_read(
	state: Rc<State>,
	args: (usize, Option<usize>),
) -> tg::Result<Option<Bytes>> {
	let (token, length) = args;
	state.host.stdin_read(token, length).await
}

pub async fn wait(state: Rc<State>, args: (u32,)) -> tg::Result<Serde<crate::host::WaitOutput>> {
	let (pid,) = args;
	let output = state.host.wait(pid).await?;
	Ok(Serde(output))
}

pub async fn write(state: Rc<State>, args: (i32, Bytes)) -> tg::Result<()> {
	let (fd, bytes) = args;
	state.host.write(fd, bytes).await
}

pub fn write_sync(
	_state: Rc<State>,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (i32, Bytes),
) -> tg::Result<()> {
	let (fd, bytes) = args;
	crate::host::Host::write_sync(fd, bytes.as_ref())
}
