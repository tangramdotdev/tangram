use crate::prelude::*;

pub async fn build<H>(handle: &H, arg: tg::run::Arg) -> tg::Result<tg::Value>
where
	H: tg::Handle,
{
	let arg = tg::run::Arg {
		sandbox: Some(arg.sandbox.unwrap_or(true)),
		..arg
	};
	tg::run::run(handle, arg).await
}
