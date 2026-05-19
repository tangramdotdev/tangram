use {
	super::process::ProcessTask,
	crate::{Context, Server, Session, context::Authentication, temp::Temp},
	futures::{FutureExt as _, TryStreamExt as _, future},
	std::{pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tokio::task::JoinSet,
};

impl Server {
	pub(crate) fn spawn_sandbox_task(
		&self,
		id: &tg::sandbox::Id,
		_token: Option<String>,
		location: tg::Location,
		permit: crate::sandbox::Permit,
		process: Option<tg::process::Id>,
		process_token: Option<String>,
	) {
		self.sandbox_tasks
			.spawn(id.clone(), |_| {
				let server = self.clone();
				let id = id.clone();
				async move {
					// Create the session.
					let context = Context {
						authentication: Some(Authentication::Sandbox(crate::context::Sandbox {
							id: id.clone(),
							location: location.clone(),
						})),
						..server.context.clone()
					};
					let session = server.session(&context);

					// Run the sandbox task.
					let result = session
						.sandbox_task(&id, location.clone(), permit, process, process_token)
						.boxed()
						.await;

					// If the sandbox task fails, then destroy the sandbox with an error.
					if let Err(error) = result {
						tracing::error!(error = %error.trace(), sandbox = %id, "the sandbox failed");
						let mut error = error.to_data_or_id();
						if !session.server.config.advanced.internal_error_locations
							&& let tg::Either::Left(error) = &mut error
						{
							error.remove_internal_locations();
						}
						let arg = tg::sandbox::destroy::Arg {
							error: Some(error),
							location: Some(location.into()),
						};
						let result = session.destroy_sandbox(&id, arg).await;
						if let Err(error) = result {
							tracing::error!(
								error = %error.trace(),
								sandbox = %id,
								"failed to destroy the sandbox after the sandbox failed"
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
		permit: crate::sandbox::Permit,
		process: Option<tg::process::Id>,
		process_token: Option<String>,
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
		if state.status.is_destroyed() {
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
				#[cfg(target_os = "linux")]
				{
					let vm = self
						.server
						.config()
						.sandbox
						.isolation
						.vm
						.as_ref()
						.ok_or_else(|| tg::error!("no vm image configured"))?;
					let kernel_path = vm.kernel_path.clone();
					let image_path = self.server.sandbox_vm_image.clone().ok_or_else(|| {
						tg::error!(
							"vm isolation requested but no image path was configured; check the server config"
						)
					})?;
					let snapshot = Some(
						vm.snapshot
							.clone()
							.unwrap_or_else(|| self.server.vm_snapshot_path()),
					);
					tangram_sandbox::Isolation::Vm(tangram_sandbox::VmIsolation {
						kernel_path,
						max_cpu: vm.max_cpu,
						max_memory: vm.max_memory,
						image_path,
						snapshot,
						snapshot_cpu: vm.snapshot_cpu,
						snapshot_memory: vm.snapshot_memory,
					})
				}
				#[cfg(target_os = "macos")]
				{
					return Err(tg::error!("vm isolation is not supported on macos"));
				}
			},
			None => self.server.resolve_sandbox_isolation()?,
		};

		#[cfg(target_os = "linux")]
		if let tangram_sandbox::Isolation::Vm(vm) = &isolation
			&& let Some(snapshot_path) = vm.snapshot.as_deref()
		{
			let mut image_created = self.server.sandbox_vm_image_lock.lock().await;
			if !*image_created {
				let _file_lock = acquire_vm_lock(&self.server.path).await?;
				let arg = tangram_sandbox::vm::image::Arg {
					image_path: vm.image_path.clone(),
					path: self.server.sandbox_container_root.clone(),
					tangram_path: self.server.tangram_path.clone(),
				};
				let created =
					tokio::task::spawn_blocking(move || tangram_sandbox::vm::image::ensure(&arg))
						.await
						.map_err(|error| tg::error!(!error, "the vm image task panicked"))??;
				if created {
					std::fs::remove_dir_all(snapshot_path).ok();
					std::fs::remove_file(snapshot_path).ok();
				}
				*image_created = true;
			}
			self.server
				.ensure_vm_snapshot(snapshot_path, &vm.kernel_path, vm)
				.await?;
		}

		let rootfs_path = match &isolation {
			tangram_sandbox::Isolation::Container(_) | tangram_sandbox::Isolation::Vm(_) => {
				self.server.sandbox_container_root.clone()
			},
			tangram_sandbox::Isolation::Seatbelt(_) => self.server.sandbox_seatbelt_root.clone(),
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

		// Get the Tangram URL and socket path.
		let guest_url = self
			.server
			.sandbox_guest_url(&isolation)
			.map_err(|error| tg::error!(!error, %id, "failed to get the sandbox tangram URL"))?;
		let tangram_socket_path = self
			.server
			.sandbox_tangram_socket_path(&isolation)
			.map_err(
				|error| tg::error!(!error, %id, "failed to get the sandbox tangram socket path"),
			)?;

		// Create the sandbox. Include the artifacts directory as a readonly mount.
		let artifacts_path = self.server.artifacts_path();
		let mut mounts = state.mounts.clone();
		mounts.push(tg::sandbox::Mount {
			readonly: true,
			source: artifacts_path.clone(),
			target: artifacts_path.clone(),
		});
		let network = match state.network.clone() {
			None => None,
			Some(tg::sandbox::Network::Default) => Some(tangram_sandbox::Network::Default),
			Some(tg::sandbox::Network::Bridge(bridge)) => {
				Some(tangram_sandbox::Network::Bridge(tangram_sandbox::Bridge {
					ports: bridge.ports,
				}))
			},
			Some(tg::sandbox::Network::Host) => Some(tangram_sandbox::Network::Host),
		};
		let arg = tangram_sandbox::Arg {
			artifacts_path,
			cpu: state.cpu,
			dns: self.server.config.sandbox.network.dns.clone(),
			#[cfg(target_os = "linux")]
			firewall: match self.server.config.sandbox.network.firewall {
				crate::config::SandboxNetworkFirewall::Iptables => {
					tangram_sandbox::Firewall::Iptables
				},
				crate::config::SandboxNetworkFirewall::Nft => tangram_sandbox::Firewall::Nft,
			},
			hostname: state.hostname.clone(),
			id: id.clone(),
			identity: self.server.path.clone(),
			#[cfg(target_os = "linux")]
			ip_pool: self.server.ip_pool.clone(),
			isolation,
			memory: state.memory,
			mounts,
			network,
			nice: self.server.config.sandbox.nice,
			path: temp.path().to_owned(),
			rootfs_path,
			tangram_path: self.server.tangram_path.clone(),
			tangram_socket_path,
			user: state.user.clone(),
		};
		let sandbox = tangram_sandbox::Sandbox::new(arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to create the sandbox"))?;

		self.server.sandboxes.insert(id.clone(), sandbox.clone());
		scopeguard::defer! {
			self.server.sandboxes.remove(id);
		}

		// Spawn the heartbeat task.
		let heartbeat_task = Task::spawn({
			let session = self.clone();
			let id = id.clone();
			let location = location.clone();
			move |stopper| async move {
				session
					.sandbox_heartbeat_task(&id, &location, stopper)
					.await
			}
		});

		let arg = tg::sandbox::status::Arg {
			location: Some(location.clone().into()),
			timeout: None,
		};
		let status = self
			.get_sandbox_status(id, arg)
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
				ProcessTask {
					process: process.clone(),
					token: process_token,
				},
				&location,
				&sandbox,
				&guest_url,
			);
		} else if let Some(ttl) = ttl {
			timer.replace(tokio::time::sleep(ttl).boxed());
		}

		// Start dequeueing a process.
		let mut dequeue = self.dequeue_sandbox_process(id, &location).boxed();

		let mut destroy = false;
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
						ProcessTask {
							process: output.process.clone(),
							token: Some(output.token),
						},
						&location,
						&sandbox,
						&guest_url,
					);
					dequeue = self.dequeue_sandbox_process(id, &location).boxed();
				},

				// If the sandbox is destroyed, then break.
				result = status.try_next() => {
					let option = result.map_err(|error| tg::error!(!error, "failed to read the sandbox status"))?;
					let Some(status) = option else {
						break;
					};
					if status.is_destroyed() {
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

				// If the timer fires, then break and destroy the sandbox.
				() = timer_future => {
					destroy = true;
					break;
				},
			}
		}

		// Drop the dequeue future.
		drop(dequeue);

		// Destroy the sandbox.
		if destroy {
			let arg = tg::sandbox::destroy::Arg {
				error: None,
				location: Some(location.clone().into()),
			};
			self.destroy_sandbox(id, arg).await?;
		}

		// Stop and await the process tasks.
		process_stopper.stop();
		while let Some(result) = process_tasks.join_next().await {
			result
				.map_err(|error| tg::error!(!error, "a process task panicked"))?
				.map_err(|error| tg::error!(!error, "a process task failed"))?;
		}

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
		location: &tg::Location,
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
		location: &tg::Location,
		stopper: Stopper,
	) -> tg::Result<()> {
		let config = self.server.config.runner.clone().unwrap_or_default();
		loop {
			let arg = tg::sandbox::heartbeat::Arg {
				location: Some(location.clone().into()),
			};
			let result = self.heartbeat_sandbox(id, arg).await;
			if let Ok(output) = result
				&& output.status.is_destroyed()
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

#[cfg(target_os = "linux")]
impl Server {
	async fn ensure_vm_snapshot(
		&self,
		snapshot_path: &std::path::Path,
		kernel_path: &std::path::Path,
		vm: &tangram_sandbox::VmIsolation,
	) -> tg::Result<()> {
		if snapshot_path.exists() {
			return Ok(());
		}
		let _guard = self.sandbox_vm_snapshot_lock.lock().await;
		if snapshot_path.exists() {
			return Ok(());
		}
		let _file_lock = acquire_vm_lock(&self.path).await?;
		if snapshot_path.exists() {
			return Ok(());
		}
		if let Some(parent) = snapshot_path.parent() {
			tokio::fs::create_dir_all(parent).await.map_err(|error| {
				tg::error!(!error, path = %parent.display(), "failed to create the vm snapshot parent directory")
			})?;
		}
		let temp = Temp::new(self);
		tokio::fs::create_dir_all(temp.path())
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to create the vm snapshot temp directory")
			})?;
		let image_path = self.sandbox_vm_image.as_ref().ok_or_else(|| {
			tg::error!(
				"cannot create the vm snapshot without an image; ensure vm isolation is configured"
			)
		})?;
		let snapshot_id = tg::sandbox::Id::new();
		tracing::info!(
			snapshot = %snapshot_path.display(),
			sandbox = %snapshot_id,
			"creating vm snapshot",
		);
		let firewall = match self.config.sandbox.network.firewall {
			crate::config::SandboxNetworkFirewall::Iptables => tangram_sandbox::Firewall::Iptables,
			crate::config::SandboxNetworkFirewall::Nft => tangram_sandbox::Firewall::Nft,
		};
		let status = tokio::process::Command::new(&self.tangram_path)
			.arg("sandbox")
			.arg("vm")
			.arg("run")
			.arg("--create-snapshot")
			.arg(snapshot_path)
			.arg("--id")
			.arg(snapshot_id.to_string())
			.arg("--artifacts-path")
			.arg(self.artifacts_path())
			.arg("--firewall")
			.arg(firewall.to_string())
			.arg("--kernel-path")
			.arg(kernel_path)
			.arg("--max-cpu")
			.arg(vm.max_cpu.to_string())
			.arg("--max-memory")
			.arg(vm.max_memory.to_string())
			.arg("--rootfs-path")
			.arg(&self.sandbox_container_root)
			.arg("--image-path")
			.arg(image_path)
			.arg("--snapshot-cpu")
			.arg(vm.snapshot_cpu.to_string())
			.arg("--snapshot-memory")
			.arg(vm.snapshot_memory.to_string())
			.arg("--tangram-path")
			.arg(&self.tangram_path)
			.arg("--path")
			.arg(temp.path())
			.arg("--url")
			.arg("http+vsock://2:6748")
			.status()
			.await
			.map_err(|error| tg::error!(!error, "failed to spawn the snapshot process"))?;
		if !status.success() {
			return Err(tg::error!(
				%status,
				snapshot = %snapshot_path.display(),
				"the snapshot process exited with a non-zero status",
			));
		}
		tracing::info!(
			snapshot = %snapshot_path.display(),
			"vm snapshot created",
		);
		Ok(())
	}
}

#[cfg(target_os = "linux")]
async fn acquire_vm_lock(data_dir: &std::path::Path) -> tg::Result<std::fs::File> {
	let lock_path = data_dir.join(".tangram/vm.lock");
	tokio::task::spawn_blocking(move || -> tg::Result<std::fs::File> {
		if let Some(parent) = lock_path.parent() {
			std::fs::create_dir_all(parent).map_err(
				|error| tg::error!(!error, path = %parent.display(), "failed to create the vm lock parent"),
			)?;
		}
		let lock = std::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.open(&lock_path)
			.map_err(
				|error| tg::error!(!error, path = %lock_path.display(), "failed to open the vm lock"),
			)?;
		let ret = unsafe { libc::flock(std::os::fd::AsRawFd::as_raw_fd(&lock), libc::LOCK_EX) };
		if ret != 0 {
			let error = std::io::Error::last_os_error();
			return Err(tg::error!(!error, "failed to acquire the vm lock"));
		}
		Ok(lock)
	})
	.await
	.map_err(|error| tg::error!(!error, "the vm lock task panicked"))?
}
