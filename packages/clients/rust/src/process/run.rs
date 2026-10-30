use {crate::prelude::*, futures::FutureExt as _};

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
		// Get the logged streams to echo.
		let echo = if arg.echo {
			[
				(tg::process::stdio::Stream::Stdout, arg.stdout.is_log()),
				(tg::process::stdio::Stream::Stderr, arg.stderr.is_log()),
			]
			.into_iter()
			.filter_map(|(stream, log)| log.then_some(stream))
			.collect()
		} else {
			Vec::new()
		};

		// Spawn the process.
		let process = tg::Process::<O>::attach_spawn_with_progress_with_instance(
			instance,
			arg,
			tg::process::attach::Mode::Run,
			|stream| {
				let writer = std::io::stderr();
				tg::progress::write_progress_stream(instance, stream, writer, false)
			},
		)
		.boxed_local()
		.await
		.map_err(|error| tg::error!(!error, "failed to spawn the process"))?;

		// Await the output while echoing the logged streams, and finish echoing before returning, even if the process fails.
		let output = process.output_with_instance(instance, tg::process::wait::Options::default());
		let echoed = process.echo_with_instance(instance, echo);
		let (output, echoed) = futures::future::join(output, echoed).boxed_local().await;
		let output =
			output.map_err(|error| tg::error!(!error, "failed to get the process output"))?;
		echoed.map_err(|error| tg::error!(!error, "failed to echo the process stdio"))?;

		Ok(output)
	}
}
