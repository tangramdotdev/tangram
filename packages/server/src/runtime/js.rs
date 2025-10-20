use {crate::Server, futures::FutureExt as _, tangram_client as tg};

#[derive(Clone)]
pub struct Runtime {
	pub local_pool_handle: tokio_util::task::LocalPoolHandle,
	pub logger: tangram_js::Logger,
	pub main_runtime_handle: tokio::runtime::Handle,
	pub server: Server,
}

impl Runtime {
	pub async fn run(&self, process: &tg::Process) -> super::Output {
		// Create a channel to receive the isolate handle.
		let (isolate_handle_sender, isolate_handle_receiver) = tokio::sync::watch::channel(None);

		// Spawn the task.
		let task = self.local_pool_handle.spawn_pinned({
			let server = self.server.clone();
			let process = process.clone();
			let logger = self.logger.clone();
			let main_runtime_handle = self.main_runtime_handle.clone();
			move || async move {
				tangram_js::run(
					&server,
					&process,
					logger,
					main_runtime_handle,
					isolate_handle_sender,
				)
				.boxed_local()
				.await
			}
		});

		let abort_handle = task.abort_handle();
		scopeguard::defer! {
			abort_handle.abort();
			if let Some(isolate_handle) = isolate_handle_receiver.borrow().as_ref() {
				tracing::trace!("terminating execution");
				isolate_handle.terminate_execution();
			}
		};

		// Get the output.
		match task.await.unwrap() {
			Ok(output) => super::Output {
				checksum: output.checksum,
				error: output.error,
				exit: output.exit,
				output: output.output,
			},
			Err(error) => super::Output {
				checksum: None,
				error: Some(error),
				exit: 1,
				output: None,
			},
		}
	}
}
