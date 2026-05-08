use {
	crate::{SandboxPermit, Server, Session, temp::Temp},
	futures::{FutureExt as _, TryStreamExt as _, future},
	std::{pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tokio::task::JoinSet,
};

mod listener;

impl Server {
	pub(crate) fn spawn_sandbox_task(
		&self,
		id: &tg::sandbox::Id,
		location: tg::Location,
		permit: SandboxPermit,
		process: Option<tg::process::Id>,
	) {
		let session = self.session(&self.context);
		self.sandbox_tasks
			.spawn(id.clone(), |_| {
				let session = session.clone();
				let id = id.clone();
				async move {
					// Run the sandbox task.
					let result = session
						.sandbox_task(&id, location.clone(), permit, process)
						.await;

					// If the sandbox task fails, then finish the sandbox with an error.
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), sandbox = %id, "the sandbox failed");
						let mut error = error.to_data_or_id();
						if !session.server.config.advanced.internal_error_locations
							&& let tg::Either::Left(error) = &mut error
						{
							error.remove_internal_locations();
						}
						let arg = tg::sandbox::finish::Arg {
							error: Some(error),
							location: Some(location.into()),
						};
						let result = session.finish_sandbox(&id, arg).await;
						if let Err(error) = result {
							tracing::error!(
								error = %error.trace(),
								sandbox = %id,
								"failed to finish the sandbox after the sandbox failed"
							);
						}
					}
				}
			})
			.detach();
	}
}

impl Session {
	async fn sandbox_task(
		&self,
		id: &tg::sandbox::Id,
		location: tg::Location,
		permit: SandboxPermit,
		process: Option<tg::process::Id>,
	) -> tg::Result<()> {
		// Get the sandbox.
		let state = self
			.try_get_sandbox(
				id,
				tg::sandbox::get::Arg {
					location: Some(location.clone().into()),
				},
			)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox"))?;
		let Some(state) = state else {
			return Ok(());
		};
		if state.status.is_finished() {
			return Ok(());
		}

		let isolation = match &state.isolation {
			Some(tg::sandbox::Isolation::Container) => {
				tangram_sandbox::Isolation::Container(tangram_sandbox::ContainerIsolation::default())
			},
			Some(tg::sandbox::Isolation::Seatbelt) => {
				tangram_sandbox::Isolation::Seatbelt(tangram_sandbox::SeatbeltIsolation::default())
			},
			Some(tg::sandbox::Isolation::Vm) => {
				let kernel_path = self
					.server
					.config()
					.sandbox
					.isolation
					.vm
					.as_ref()
					.ok_or_else(|| tg::error!("no vm image configured"))?
					.kernel_path
					.clone();
				tangram_sandbox::Isolation::Vm(tangram_sandbox::VmIsolation { kernel_path })
			},
			None => self.server.resolve_sandbox_isolation()?,
		};

		// Associate the permit with the sandbox.
		let permit = Arc::new(tokio::sync::Mutex::new(Some(permit)));
		self.server.sandbox_permits.insert(id.clone(), permit);
		scopeguard::defer! {
			self.server.sandbox_permits.remove(id);
		}

		// Create the temp.
		let temp = Temp::new(&self.server);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|error| tg::error!(!error, "failed to create the temp directory"))?;

		// Create the listener.
		let (listener, guest_uri) = Server::run_create_listener(temp.path(), &isolation)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to create the tangram listener"))?;

		// Create the sandbox. Include the artifacts directory as a readonly mount.
		let artifacts_path = self.server.artifacts_path();
		let mut mounts = state.mounts.clone();
		mounts.push(tg::sandbox::Mount {
			source: artifacts_path.clone(),
			target: artifacts_path.clone(),
			readonly: true,
		});
		let network = match (state.network.as_ref(), &isolation) {
			(tg::Either::Left(false), _) => None,
			(
				tg::Either::Left(true),
				tangram_sandbox::Isolation::Container(_) | tangram_sandbox::Isolation::Seatbelt(_),
			) => Some(tangram_sandbox::Network::Host),
			(tg::Either::Left(true), tangram_sandbox::Isolation::Vm(_)) => {
				if matches!(
					self.server.config().sandbox.network,
					crate::config::Network::Pasta(_)
				) {
					Some(tangram_sandbox::Network::Pasta)
				} else {
					Some(tangram_sandbox::Network::Tap)
				}
			},
			(tg::Either::Right(tg::sandbox::Network::Host), _) => {
				Some(tangram_sandbox::Network::Host)
			},
			(tg::Either::Right(tg::sandbox::Network::Bridge), _) => match &self.server.config().sandbox.network {
				crate::config::Network::Pasta(_) => Some(tangram_sandbox::Network::Pasta),
				crate::config::Network::Bridge(bridge) => {
					let ip = bridge.ip.unwrap_or_else(crate::config::default_bridge_ip);
					let name = bridge
						.name
						.clone()
						.unwrap_or_else(|| "tangram0".to_owned());
					Some(tangram_sandbox::Network::Bridge(tangram_sandbox::Bridge {
						ip,
						name,
					}))
				},
			},
		};
		let (host_ip, guest_ip) = match (&isolation, network.as_ref()) {
			(
				tangram_sandbox::Isolation::Container(_),
				Some(tangram_sandbox::Network::Bridge(_)),
			) => (None, Some(self.server.allocate_guest_ip()?)),
			(tangram_sandbox::Isolation::Vm(_), Some(_)) => {
				let (host, guest) = self.server.allocate_guest_ip_pair()?;
				(Some(host), Some(guest))
			},
			_ => (None, None),
		};
		let arg = tangram_sandbox::Arg {
			artifacts_path,
			cpu: state.cpu,
			dns: self.server.config.sandbox.dns.clone(),
			host_ip: host_ip.as_ref().map(|ip| ip.addr),
			guest_ip: guest_ip.as_ref().map(|ip| ip.addr),
			hostname: state.hostname.clone(),
			id: id.clone(),
			isolation,
			memory: state.memory,
			mounts,
			network,
			nice: self.server.config.sandbox.nice,
			path: temp.path().to_owned(),
			rootfs_path: self.server.sandbox_rootfs.clone(),
			tangram_path: self.server.tangram_path.clone(),
			user: state.user.clone(),
		};
		let sandbox = tangram_sandbox::Sandbox::new(arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to create the sandbox"))?;

		let _temp = temp;
		self.server.sandboxes.insert(id.clone(), sandbox.clone());
		let session = self.clone();
		scopeguard::defer! {
			session.server.sandboxes.remove(id);
			drop(host_ip);
			drop(guest_ip);
		}

		// Spawn the serve task.
		let serve_task = Task::spawn({
			let session = self.clone();
			let id = id.clone();
			let config = crate::config::HttpListener {
				url: guest_uri.clone(),
				tls: None,
			};
			|stop| async move {
				session
					.server
					.serve(listener, config, None, Some(id.clone()), stop)
					.await;
			}
		});

		// Spawn the heartbeat task.
		let heartbeat_task = Task::spawn({
			let session = session.clone();
			let id = id.clone();
			let location = location.clone();
			move |stopper| async move {
				session
					.sandbox_heartbeat_task(&id, &location, stopper)
					.await
			}
		});

		let status = session
			.get_sandbox_status(
				id,
				tg::sandbox::status::Arg {
					location: Some(location.clone().into()),
					timeout: None,
				},
			)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox status stream"))?;
		let mut status = pin!(status);

		// Create the process tasks.
		let mut process_tasks = JoinSet::new();
		let process_stopper = Stopper::new();

		// Create the timer.
		let mut timer = None;
		let ttl = state.ttl;

		// If the sandbox was created with a process, then start it. Otherwise, start the timer.
		if let Some(process) = process {
			self.spawn_process_task(
				&mut process_tasks,
				&process_stopper,
				process.clone(),
				&location,
				&sandbox,
				&guest_uri,
			);
		} else if let Some(ttl) = ttl {
			timer.replace(tokio::time::sleep(ttl).boxed());
		}

		// Start dequeueing a process.
		let mut dequeue = self.dequeue_sandbox_process(id, &location).boxed();

		let mut finish = false;
		loop {
			let timer_future = timer.as_mut().map_or_else(
				|| future::pending().left_future(),
				|timer| timer.as_mut().right_future(),
			);
			tokio::select! {
				// If a process is dequeued, then start it and dequeue another.
				output = &mut dequeue => {
					let output = output.map_err(|error| tg::error!(!error, "failed to dequeue a process"))?;
					timer.take();
					self.spawn_process_task(
						&mut process_tasks,
						&process_stopper,
						output.process.clone(),
						&location,
						&sandbox,
						&guest_uri,
					);
					dequeue = self.dequeue_sandbox_process(id, &location).boxed();
				},

				// If the sandbox finishes, then break.
				result = status.try_next() => {
					let option = result.map_err(|error| tg::error!(!error, "failed to read the sandbox status"))?;
					let Some(status) = option else {
						break;
					};
					if status.is_finished() {
						break;
					}
				},

				// If a process finishes and there are no processes, then start the timer.
				output = process_tasks.join_next(), if !process_tasks.is_empty() => {
					output
						.unwrap()
						.map_err(|error| tg::error!(!error, "a process task panicked"))?
						.map_err(|error| tg::error!(!error, "a process task failed"))?;
					if process_tasks.is_empty() && let Some(ttl) = ttl {
						timer.replace(tokio::time::sleep(ttl).boxed());
					}
				},

				// If the timer fires, then break and finish the sandbox.
				() = timer_future => {
					finish = true;
					break;
				},
			}
		}

		// Drop the dequeue future.
		drop(dequeue);

		// Finish.
		if finish {
			let arg = tg::sandbox::finish::Arg {
				error: None,
				location: Some(location.clone().into()),
			};
			self.finish_sandbox(id, arg).await?;
		}

		// Stop and await the process tasks.
		process_stopper.stop();
		while let Some(result) = process_tasks.join_next().await {
			result
				.map_err(|error| tg::error!(!error, "a process task panicked"))?
				.map_err(|error| tg::error!(!error, "a process task failed"))?;
		}

		// Stop and await the server task.
		serve_task.stop();
		serve_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the serve task panicked"))?;

		// Stop and await the heartbeat task.
		heartbeat_task.stop();
		heartbeat_task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the heartbeat task panicked"))?
			.map_err(|error| tg::error!(!error, "the heartbeat task failed"))?;

		Ok(())
	}

	async fn dequeue_sandbox_process(
		&self,
		id: &tg::sandbox::Id,
		location: &tg::location::Location,
	) -> tg::Result<tg::sandbox::process::queue::Output> {
		loop {
			let arg = tg::sandbox::process::queue::Arg {
				location: Some(location.clone().into()),
				timeout: None,
			};
			match self.try_dequeue_sandbox_process(id, arg).await {
				Ok(Some(output)) => return Ok(output),
				Ok(None) => (),
				Err(error) => {
					tracing::trace!(error = %error.trace(), sandbox = %id, ?location, "failed to dequeue a process");
				},
			}
		}
	}

	async fn sandbox_heartbeat_task(
		&self,
		id: &tg::sandbox::Id,
		location: &tg::location::Location,
		stopper: Stopper,
	) -> tg::Result<()> {
		let config = self.server.config.runner.clone().unwrap_or_default();
		loop {
			let arg = tg::sandbox::heartbeat::Arg {
				location: Some(location.clone().into()),
			};
			let result = self.heartbeat_sandbox(id, arg).await;
			if let Ok(output) = result
				&& output.status.is_finished()
			{
				break;
			}
			let sleep = tokio::time::sleep(config.heartbeat_interval);
			match future::select(pin!(sleep), pin!(stopper.wait())).await {
				future::Either::Left(_) => (),
				future::Either::Right(_) => break,
			}
		}
		Ok(())
	}
}
