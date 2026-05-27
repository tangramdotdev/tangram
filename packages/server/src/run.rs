use {crate::Server, std::time::Duration, tangram_client::prelude::*};

mod process;
mod progress;
mod sandbox;

pub(crate) use self::sandbox::SpawnSandboxTaskArg;

#[derive(Clone, Debug)]
pub struct Output {
	pub checksum: Option<tg::Checksum>,
	pub error: Option<tg::Error>,
	pub exit: u8,
	pub value: Option<tg::Value>,
}

impl Server {
	pub(crate) async fn runner_task(&self) {
		loop {
			let permit = self
				.sandbox_semaphore
				.clone()
				.acquire_owned()
				.await
				.unwrap();
			let permit = crate::sandbox::Permit(tg::Either::Left(permit));

			let location = self
				.config
				.runner
				.as_ref()
				.and_then(|config| config.remote.as_ref())
				.map_or_else(
					|| tg::Location::Local(tg::location::Local::default()),
					|name| {
						tg::Location::Remote(tg::location::Remote {
							name: name.to_owned(),
							region: None,
						})
					},
				);
			let arg = tg::sandbox::queue::Arg {
				location: Some(location.clone().into()),
				timeout: None,
			};
			let result = self.dequeue_sandbox(arg).await;
			let output = match result {
				Ok(output) => output,
				Err(error) => {
					tracing::error!(error = %error.trace(), "failed to dequeue a sandbox");
					tokio::time::sleep(Duration::from_secs(1)).await;
					continue;
				},
			};

			self.spawn_sandbox_task(SpawnSandboxTaskArg {
				created_by: output.created_by,
				id: output.sandbox,
				location,
				permit,
				process: output.process,
				process_token: output.process_token,
				token: output.token,
			});
		}
	}
}
