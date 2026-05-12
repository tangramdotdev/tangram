use crate::prelude::*;

pub async fn build(arg: tg::process::Arg) -> tg::Result<tg::Value> {
	let handle = tg::handle()?;
	build_with_handle(handle, arg).await
}

pub async fn build_with_handle<H>(handle: &H, arg: tg::process::Arg) -> tg::Result<tg::Value>
where
	H: tg::Handle,
{
	tg::Process::<tg::Value>::build_with_handle(handle, arg).await
}

impl<O> tg::Process<O> {
	pub async fn build(arg: tg::process::Arg) -> tg::Result<O>
	where
		O: TryFrom<tg::Value> + 'static,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let handle = tg::handle()?;
		Self::build_with_handle(handle, arg).await
	}

	pub async fn build_with_handle<H>(handle: &H, arg: tg::process::Arg) -> tg::Result<O>
	where
		H: tg::Handle,
		O: TryFrom<tg::Value> + 'static,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let sandbox = arg
			.sandbox
			.clone()
			.unwrap_or(tg::process::SandboxArg::Bool(true));
		let cacheable = match &sandbox {
			tg::process::SandboxArg::Bool(true) => {
				arg.mounts.is_empty() && arg.ports.is_empty() && arg.network.is_none()
			},
			tg::process::SandboxArg::Arg(sandbox) => {
				sandbox.mounts.is_empty()
					&& sandbox.network.is_none()
					&& arg.mounts.is_empty()
					&& arg.ports.is_empty()
					&& arg.network.is_none()
			},
			tg::process::SandboxArg::Bool(false) | tg::process::SandboxArg::Id(_) => false,
		} && arg.stdin.is_null()
			&& arg.stdout.is_log()
			&& arg.stderr.is_log()
			&& matches!(&arg.tty, None | Some(tg::Either::Left(false)));
		let cacheable = cacheable || arg.checksum.is_some();
		if !cacheable {
			return Err(tg::error!("a build must be cacheable"));
		}
		let arg = tg::process::Arg {
			sandbox: Some(sandbox),
			..arg
		};
		tg::Process::<O>::run_with_handle(handle, arg).await
	}
}
