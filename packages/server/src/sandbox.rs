use {
	crate::Server,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	std::net::Ipv4Addr,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_messenger::prelude::*,
};

pub mod create;
pub mod delete;
pub mod finalize;
pub mod finish;
pub mod get;
pub mod heartbeat;
pub mod list;
pub mod process;
pub mod queue;
pub mod status;

impl Server {
	fn sandbox_isolation_from_config(
		isolation: &crate::config::SandboxIsolation,
	) -> tangram_sandbox::Isolation {
		match isolation {
			crate::config::SandboxIsolation::Container(container) => {
				let net = match &container.net {
					crate::config::ContainerNet::None => tangram_sandbox::Net::None,
					crate::config::ContainerNet::Host => tangram_sandbox::Net::Host,
					crate::config::ContainerNet::Bridge(bridge) => {
						let ip = bridge.ip.unwrap_or(Ipv4Addr::new(172, 18, 0, 1));
						tangram_sandbox::Net::Bridge(tangram_sandbox::Bridge {
							ip,
							name: bridge.name.clone(),
						})
					},
				};
				tangram_sandbox::Isolation::Container(tangram_sandbox::ContainerIsolation { net })
			},
			crate::config::SandboxIsolation::Seatbelt(_) => {
				tangram_sandbox::Isolation::Seatbelt(tangram_sandbox::SeatbeltIsolation::default())
			},
			crate::config::SandboxIsolation::Vm(vm) => {
				tangram_sandbox::Isolation::Vm(tangram_sandbox::VmIsolation {
					kernel_path: vm.kernel_path.clone(),
				})
			},
		}
	}

	pub(crate) fn resolve_sandbox_isolation(&self) -> tg::Result<tangram_sandbox::Isolation> {
		let isolation = Self::sandbox_isolation_from_config(&self.config.sandbox.isolation);
		#[cfg(target_os = "linux")]
		{
			match isolation {
				tangram_sandbox::Isolation::Container(_) | tangram_sandbox::Isolation::Vm(_) => {
					Ok(isolation)
				},
				tangram_sandbox::Isolation::Seatbelt(_) => {
					Err(tg::error!("seatbelt isolation is not supported on linux"))
				},
			}
		}
		#[cfg(target_os = "macos")]
		{
			match isolation {
				tangram_sandbox::Isolation::Container(_) => {
					Err(tg::error!("container isolation is not supported on macos"))
				},
				tangram_sandbox::Isolation::Seatbelt(_) => Ok(isolation),
				tangram_sandbox::Isolation::Vm(_) => {
					Err(tg::error!("vm isolation is not supported on macos"))
				},
			}
		}
	}

	pub(crate) fn validate_sandbox_resources(
		isolation: &tangram_sandbox::Isolation,
		cpu: Option<u64>,
		memory: Option<u64>,
	) -> tg::Result<()> {
		if cpu == Some(0) {
			return Err(tg::error!("sandbox cpu must be greater than zero"));
		}
		if memory == Some(0) {
			return Err(tg::error!("sandbox memory must be greater than zero"));
		}
		if matches!(isolation, tangram_sandbox::Isolation::Seatbelt(_))
			&& (cpu.is_some() || memory.is_some())
		{
			return Err(tg::error!(
				"sandbox cpu and memory are not supported with seatbelt isolation"
			));
		}
		Ok(())
	}

	pub(crate) async fn get_sandbox_exists_local(&self, id: &tg::sandbox::Id) -> tg::Result<bool> {
		let connection = self
			.process_store
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count(*) != 0
				from sandboxes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let exists = connection
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);
		Ok(exists)
	}

	pub(crate) async fn try_start_sandbox_local(&self, id: &tg::sandbox::Id) -> tg::Result<bool> {
		let connection = self
			.process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update sandboxes
				set
					heartbeat_at = {p}1,
					started_at = case when started_at is null then {p}1 else started_at end,
					status = 'started'
				where id = {p}2 and status = 'created';
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![now, id.to_string()];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);
		if n == 0 {
			return Ok(false);
		}
		self.publish_sandbox_status(id);
		Ok(true)
	}

	pub(crate) fn allocate_guest_ip(&self) -> tg::Result<crate::network::Ip> {
		if self.networks.is_empty() {
			return Err(tg::error!("no networks are configured"));
		}
		self.networks
			.iter()
			.find_map(|network| {
				network
					.try_reserve()
					.inspect_err(|error| tracing::warn!(?error, "failed to allocate ip"))
					.ok()
			})
			.ok_or_else(|| tg::error!("failed to allocate guest IP address"))
	}

	pub(crate) fn allocate_guest_ip_pair(
		&self,
	) -> tg::Result<(crate::network::Ip, crate::network::Ip)> {
		if self.networks.is_empty() {
			return Err(tg::error!("no networks are configured"));
		}
		self.networks
			.iter()
			.find_map(|network| {
				network
					.try_reserve_pair()
					.inspect_err(|error| tracing::warn!(?error, "failed to allocate ip pair"))
					.ok()
			})
			.ok_or_else(|| tg::error!("failed to allocate guest IP address pair"))
	}

	pub(crate) fn publish_sandbox_status(&self, id: &tg::sandbox::Id) {
		let subject = format!("sandboxes.{id}.status");
		tokio::spawn({
			let server = self.clone();
			async move {
				server.messenger.publish(subject, ()).await.ok();
			}
		});
	}

	pub(crate) async fn finish_unfinished_processes_in_sandbox(
		&self,
		id: &tg::sandbox::Id,
		location: &tg::location::Location,
		error: tg::error::Data,
	) -> tg::Result<()> {
		match location {
			tg::Location::Local(_) => {
				self.finish_unfinished_processes_in_sandbox_local(id, error)
					.await
			},
			tg::Location::Remote(remote) => {
				self.finish_unfinished_processes_in_sandbox_remote(id, remote, error)
					.await
			},
		}
	}

	pub(crate) async fn finish_unfinished_processes_in_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
		error: tg::error::Data,
	) -> tg::Result<()> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::process::Id,
		}
		let connection = self
			.process_store
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select id
				from processes
				where sandbox = {p}1 and status != 'finished';
			"
		);
		let params = db::params![id.to_string()];
		let rows = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);

		rows.into_iter()
			.map(|row| {
				let server = self.clone();
				let error = error.clone();
				async move {
					let arg = tg::process::finish::Arg {
						checksum: None,
						error: Some(tg::Either::Left(error)),
						exit: 1,
						location: Some(tg::Location::Local(tg::location::Local::default()).into()),
						output: None,
					};
					server.finish_process(&row.id, arg).await.ok();
				}
				.boxed()
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(())
	}

	async fn finish_unfinished_processes_in_sandbox_remote(
		&self,
		id: &tg::sandbox::Id,
		remote: &tg::location::Remote,
		error: tg::error::Data,
	) -> tg::Result<()> {
		let client = self
			.get_remote_client(remote.name.clone())
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					%id,
					remote = %remote.name,
					"failed to get the remote client"
				)
			})?;
		let arg = tg::process::list::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.region.clone().map(|region| vec![region]),
				}),
			])),
		};
		let output = client.list_processes(arg).await.map_err(
			|source| tg::error!(!source, %id, remote = %remote.name, "failed to list the remote processes"),
		)?;

		output
			.data
			.into_iter()
			.filter(|output| &output.data.sandbox == id && !output.data.status.is_finished())
			.map(|output| {
				let server = self.clone();
				let error = error.clone();
				let remote = remote.clone();
				async move {
					let arg = tg::process::finish::Arg {
						checksum: None,
						error: Some(tg::Either::Left(error)),
						exit: 1,
						location: Some(tg::Location::Remote(remote).into()),
						output: None,
					};
					server.finish_process(&output.id, arg).await.ok();
				}
				.boxed()
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(())
	}
}
