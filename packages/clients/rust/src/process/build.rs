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
		let sandbox = arg.sandbox.clone().unwrap_or_else(|| {
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
		tg::run(handle, arg).await
	}
}
