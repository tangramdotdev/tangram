use crate::prelude::*;

pub async fn run(arg: tg::process::Arg) -> tg::Result<tg::Value> {
	let instance = tg::instance()?;
	run_with_instance(instance, arg).await
}

pub async fn run_with_instance<I>(instance: &I, arg: tg::process::Arg) -> tg::Result<tg::Value>
where
	I: tg::Instance,
{
	tg::Process::<tg::Value>::run_with_instance(instance, arg).await
}

impl<O> tg::Process<O> {
	pub async fn run(arg: tg::process::Arg) -> tg::Result<O>
	where
		O: TryFrom<tg::Value> + 'static,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let instance = tg::instance()?;
		Self::run_with_instance(instance, arg).await
	}

	pub async fn run_with_instance<I>(instance: &I, arg: tg::process::Arg) -> tg::Result<O>
	where
		I: tg::Instance,
		O: TryFrom<tg::Value> + 'static,
		O::Error: std::error::Error + Send + Sync + 'static,
	{
		let process = tg::Process::<O>::connect_spawn_with_progress_with_instance(
			instance,
			arg,
			tg::process::connect::Mode::Run,
			|stream| {
				let writer = std::io::stderr();
				tg::progress::write_progress_stream(instance, stream, writer, false)
			},
		)
		.await
		.map_err(|error| tg::error!(!error, "failed to spawn the process"))?;

		let output = process
			.output_with_instance(instance, tg::process::wait::Options::default())
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process output"))?;

		Ok(output)
	}
}
