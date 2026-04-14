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
		let sandbox =
			super::normalize_sandbox_arg(arg.sandbox.clone(), arg.cpu, arg.isolation, arg.memory)?
				.unwrap_or_else(|| {
					tg::Either::Left(tg::sandbox::create::Arg {
						network: false,
						ttl: 0,
						..Default::default()
					})
				});
		let cacheable = matches!(
			&sandbox,
			tg::Either::Left(arg) if arg.mounts.is_empty() && !arg.network
		) && arg.stdin.is_null()
			&& arg.stdout.is_log()
			&& arg.stderr.is_log();
		let cacheable = cacheable || arg.checksum.is_some();
		if !cacheable {
			return Err(tg::error!("a build must be cacheable"));
		}
		if matches!(sandbox, tg::Either::Right(_)) {
			return Err(tg::error!("a build cannot use an existing sandbox"));
		}
		let arg = tg::process::Arg {
			sandbox: Some(sandbox),
			..arg
		};
		tg::Process::<O>::run_with_handle(handle, arg).await
	}
}
