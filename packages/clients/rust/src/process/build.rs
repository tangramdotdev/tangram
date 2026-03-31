use crate::prelude::*;

pub async fn build<H>(handle: &H, arg: tg::process::Arg) -> tg::Result<tg::Value>
where
	H: tg::Handle,
{
	tg::Process::build(handle, arg).await
}

impl tg::Process {
	pub async fn build<H>(handle: &H, arg: tg::process::Arg) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let cacheable = arg.mounts.as_ref().is_none_or(Vec::is_empty)
			&& !arg.network.unwrap_or_default()
			&& arg.stdin.is_null()
			&& arg.stdout.is_log()
			&& arg.stderr.is_log();
		let cacheable = cacheable || arg.checksum.is_some();
		if !cacheable {
			return Err(tg::error!("a build must be cacheable"));
		}
		let arg = tg::process::Arg {
			sandbox: Some(arg.sandbox.unwrap_or(true)),
			..arg
		};
		tg::run(handle, arg).await
	}
}
