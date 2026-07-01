use {
	crate::{Server, Session},
	futures::{FutureExt as _, StreamExt as _, future},
	indoc::formatdoc,
	std::{
		net::{IpAddr, Ipv4Addr, SocketAddr},
		ops::ControlFlow,
		pin::pin,
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
	tangram_messenger::Messenger as _,
};

#[derive(Clone)]
struct CreateSandboxArg {
	arg: tg::sandbox::create::Arg,
	cpu: Option<i64>,
	creator: Option<tg::Principal>,
	id: tg::sandbox::Id,
	memory: Option<i64>,
	now: i64,
	owner: Option<tg::Principal>,
	status: tg::sandbox::Status,
	token: String,
	ttl: Option<std::time::Duration>,
}

impl Session {
	pub(crate) async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		if matches!(self.context.principal, tg::Principal::Process(_)) {
			return Err(tg::error!("unauthorized"));
		}

		let location = self.server.location(arg.location.as_ref())?;

		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.create_sandbox_local(arg).await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => self.create_sandbox_region(arg, region).await?,
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => self.create_sandbox_remote(arg, remote, region).await?,
		};

		Ok(output)
	}

	async fn create_sandbox_local(
		&self,
		mut arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		self.authorize_owner(arg.owner.as_ref()).await?;
		arg = Self::normalize_sandbox_create_arg(arg)?;
		if arg.host.is_none() {
			arg.host = Some(tg::host::current().to_owned());
		}
		let id = tg::sandbox::Id::new();
		let creator = self.context.principal.clone();
		let owner = arg
			.owner
			.clone()
			.or_else(|| Some(creator.clone()))
			.filter(|owner| !matches!(owner, tg::Principal::Root));
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let isolation = self.server.resolve_sandbox_isolation()?;
		Server::validate_sandbox_resources(
			&isolation,
			arg.cpu,
			arg.memory,
			arg.hostname.as_deref(),
		)?;
		let cpu = arg
			.cpu
			.map(i64::try_from)
			.transpose()
			.map_err(|error| tg::error!(!error, "invalid sandbox cpu"))?;
		let memory = arg
			.memory
			.map(i64::try_from)
			.transpose()
			.map_err(|error| tg::error!(!error, "invalid sandbox memory"))?;
		let ttl = arg.ttl;
		db::value::DurationSeconds::validate(ttl).map_err(|_| tg::error!("invalid sandbox ttl"))?;

		// Create the token that the caller uses to authenticate as the sandbox.
		let token = Self::create_sandbox_token_string();

		let create_arg = CreateSandboxArg {
			arg: arg.clone(),
			cpu,
			creator: Some(creator),
			id: id.clone(),
			memory,
			now,
			owner: owner.clone(),
			status: tg::sandbox::Status::Created,
			token: token.clone(),
			ttl,
		};
		self.server
			.process_store
			.run(|transaction| {
				let arg = create_arg.clone();
				async move { Self::create_sandbox_with_transaction(transaction, arg).await }.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to create the sandbox"))?;

		self.server
			.index
			.batch(tangram_index::batch::Arg {
				put_sandboxes: vec![tangram_index::sandbox::put::Arg {
					id: id.clone(),
					owner: owner.clone(),
				}],
				..Default::default()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to put the sandbox to the index"))?;

		self.server.spawn_publish_sandbox_status_task(&id);

		// Push the sandbox to the scheduler and wait for it to be started.
		self.schedule_process(&id, arg, None, Some(token.clone()), None)
			.await?;

		let output = tg::sandbox::create::Output {
			id,
			token: Some(token),
		};

		Ok(output)
	}

	pub(crate) async fn schedule_process(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::create::Arg,
		process: Option<&tg::process::Id>,
		token: Option<String>,
		process_token: Option<String>,
	) -> tg::Result<()> {
		let mut delay = std::time::Duration::from_millis(10);
		let max_delay = std::time::Duration::from_secs(1);
		loop {
			let message_id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());

			// Subscribe to the response subject before publishing so that the response is not missed.
			let response = self
				.server
				.messenger
				.subscribe::<crate::scheduler::Message>(format!(
					"scheduler.create-sandbox.{message_id}"
				))
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to subscribe to the scheduler response")
				})?;
			let mut response = pin!(response);

			// Create the request.
			let request = crate::scheduler::Message::Request(
				crate::scheduler::Request::CreateSandbox(crate::scheduler::CreateSandboxRequest {
					arg: arg.clone(),
					id: message_id.clone(),
					process: process.cloned(),
					process_token: process_token.clone(),
					sandbox: id.clone(),
					token: token.clone(),
				}),
			);

			// Publish the request and wait for the response.
			let options = tangram_futures::retry::Options {
				max_retries: u64::MAX,
				..Default::default()
			};
			let mut retries = pin!(tangram_futures::retry::stream(options));
			let response = loop {
				match future::select(response.next(), retries.next()).await {
					future::Either::Left((message, _)) => {
						let message = message
							.ok_or_else(|| tg::error!("the scheduler response stream ended"))?
							.map_err(|source| {
								tg::error!(!source, "failed to receive the scheduler response")
							})?;
						let crate::scheduler::Message::Response(
							crate::scheduler::Response::CreateSandbox(response),
						) = message.payload
						else {
							return Err(tg::error!("expected a create sandbox response"));
						};
						break response;
					},
					future::Either::Right((tick, _)) => {
						if tick.is_none() {
							return Err(tg::error!("timed out waiting for the scheduler response"));
						}
						self.server
							.messenger
							.publish("scheduler".to_owned(), request.clone())
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to publish the scheduler request")
							})?;
					},
				}
			};

			// Acknowledge the response so that the scheduler can release its cached response.
			let ack = crate::scheduler::Message::Ack(crate::scheduler::Ack { id: message_id });
			self.server
				.messenger
				.publish("scheduler".to_owned(), ack)
				.await
				.inspect_err(|error| {
					tracing::error!(%error, "failed to acknowledge the scheduler response");
				})
				.ok();

			// If the response is an error, then return it.
			if let Some(error) = response.error {
				let error = tg::Error::try_from(error).map_err(|source| {
					tg::error!(!source, "failed to deserialize the scheduler error")
				})?;
				return Err(error);
			}

			// If the sandbox was created, then return.
			if response.created {
				return Ok(());
			}

			// Otherwise, no runner was available, so back off and try again with a new request.
			tokio::time::sleep(delay).await;
			delay = (delay * 2).min(max_delay);
		}
	}

	pub(crate) async fn start_sandbox_process(
		&self,
		sandbox: &tg::sandbox::Id,
		process: Option<&tg::process::Id>,
	) -> tg::Result<bool> {
		let session = self.clone();
		let sandbox = sandbox.clone();
		let process = process.cloned();
		let started = self
			.server
			.process_store
			.run(|transaction| {
				let process = process.clone();
				let sandbox = sandbox.clone();
				let session = session.clone();
				async move {
					session
						.start_sandbox_process_with_transaction(
							transaction,
							&sandbox,
							process.as_ref(),
						)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, %sandbox, "failed to start the sandbox process"))?;

		if started {
			self.server.spawn_publish_sandbox_status_task(&sandbox);
			if let Some(process) = &process {
				self.server.spawn_publish_process_status_task(process);
			}
		}

		Ok(started)
	}

	async fn start_sandbox_process_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		sandbox: &tg::sandbox::Id,
		process: Option<&tg::process::Id>,
	) -> tg::Result<ControlFlow<bool, crate::database::Error>> {
		let p = transaction.p();
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let statement = formatdoc!(
			"
				update sandboxes
				set
					heartbeat_at = coalesce(heartbeat_at, {p}1),
					started_at = coalesce(started_at, {p}1),
					status = {p}2
				where id = {p}3 and status = {p}4;
			"
		);
		let params = db::params![
			now,
			tg::sandbox::Status::Started.to_string(),
			sandbox.to_string(),
			tg::sandbox::Status::Created.to_string(),
		];
		let result = transaction.execute(statement.into(), params).await;
		let n = crate::database::retry!(result, "failed to execute the statement");
		if n == 0 && process.is_none() {
			return Ok(ControlFlow::Break(false));
		}

		if let Some(process) = process {
			let statement = formatdoc!(
				"
					update processes
					set
						started_at = coalesce(started_at, {p}1),
						status = {p}2
					where id = {p}3 and sandbox = {p}4 and status = {p}5;
				"
			);
			let params = db::params![
				now,
				tg::process::Status::Started.to_string(),
				process.to_string(),
				sandbox.to_string(),
				tg::process::Status::Created.to_string(),
			];
			let result = transaction.execute(statement.into(), params).await;
			let n = crate::database::retry!(result, "failed to execute the statement");
			if n == 0 {
				return Ok(ControlFlow::Break(false));
			}
		}

		Ok(ControlFlow::Break(true))
	}

	pub(crate) async fn dispatch_process(
		&self,
		sandbox: &tg::sandbox::Id,
		process: &tg::process::Id,
		process_token: Option<String>,
	) -> tg::Result<()> {
		let timeout = self.server.config.sandbox.spawn_process_timeout;
		let message_id = tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes());

		// Subscribe to the sandbox response before publishing so that the response is not missed.
		let response = self
			.server
			.messenger
			.subscribe::<crate::sandbox::control::SandboxControlClientMessage>(format!(
				"sandboxes.{sandbox}.client.{message_id}"
			))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe to the sandbox response"))?;
		let mut response = pin!(response);

		// Create the request.
		let request = tg::sandbox::control::ServerMessage::Request(
			tg::sandbox::control::ServerRequest::SpawnProcess(
				tg::sandbox::control::SpawnProcessServerRequest {
					id: message_id.clone(),
					process: process.clone(),
					process_token: process_token.clone(),
				},
			),
		);
		let request = crate::sandbox::control::SandboxControlServerMessage(request);

		// Publish the request and wait for the response.
		let options = tangram_futures::retry::Options {
			max_retries: u64::MAX,
			..Default::default()
		};
		let mut retries = pin!(tangram_futures::retry::stream(options));
		let deadline = tokio::time::Instant::now() + timeout;
		let response = loop {
			match future::select(response.next(), retries.next()).await {
				future::Either::Left((message, _)) => {
					let message = message
						.ok_or_else(|| tg::error!("the sandbox client message stream ended"))?
						.map_err(|source| {
							tg::error!(!source, "failed to receive a sandbox client message")
						})?;
					let tg::sandbox::control::ClientMessage::Response(
						tg::sandbox::control::ClientResponse::SpawnProcess(response),
					) = message.payload.0
					else {
						return Err(tg::error!("expected a spawn process response"));
					};
					break response;
				},
				future::Either::Right((tick, _)) => {
					if tick.is_none() || tokio::time::Instant::now() >= deadline {
						return Err(tg::error!("timed out waiting for the sandbox response"));
					}
					self.server
						.messenger
						.publish(format!("sandboxes.{sandbox}.server"), request.clone())
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to publish the spawn process request")
						})?;
				},
			}
		};

		// Acknowledge the response so that the sandbox can release its cached response.
		let ack = tg::sandbox::control::ServerMessage::Ack(tg::sandbox::control::ServerAck {
			id: message_id,
		});
		self.server
			.messenger
			.publish(
				format!("sandboxes.{sandbox}.server"),
				crate::sandbox::control::SandboxControlServerMessage(ack),
			)
			.await
			.inspect_err(|error| {
				tracing::error!(%error, "failed to acknowledge the sandbox response");
			})
			.ok();

		// If the process was spawned, then return.
		if response.spawned {
			return Ok(());
		}

		Err(tg::error!(
			"the process could not be started in the sandbox"
		))
	}

	async fn create_sandbox_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		arg: CreateSandboxArg,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				insert into sandboxes (
					id,
					cpu,
					created_at,
					creator,
					heartbeat_at,
					hostname,
					isolation,
					memory,
					mounts,
					network,
					owner,
					started_at,
					status,
					ttl
				)
				values (
					{p}1,
					{p}2,
					{p}3,
					{p}4,
					{p}5,
					{p}6,
					{p}7,
					{p}8,
					{p}9,
					{p}10,
					{p}11,
					{p}12,
					{p}13,
					{p}14
				);
			"
		);
		let heartbeat_at = (arg.status == tg::sandbox::Status::Started).then_some(arg.now);
		let started_at = (arg.status == tg::sandbox::Status::Started).then_some(arg.now);
		let params = db::params![
			arg.id.to_string(),
			arg.cpu,
			arg.now,
			arg.creator.as_ref().map(ToString::to_string),
			heartbeat_at,
			arg.arg.hostname.clone(),
			arg.arg.isolation.map(db::value::Json),
			arg.memory,
			(!arg.arg.mounts.is_empty()).then_some(db::value::Json(arg.arg.mounts)),
			arg.arg.network.map(db::value::Json),
			arg.owner.as_ref().map(ToString::to_string),
			started_at,
			arg.status.to_string(),
			db::value::DurationSeconds(arg.ttl),
		];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		// Insert the sandbox's token so that the caller can authenticate as the sandbox.
		let statement = formatdoc!(
			"
				insert into sandbox_tokens (sandbox, token)
				values ({p}1, {p}2);
			"
		);
		let params = db::params![arg.id.to_string(), arg.token];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	async fn create_sandbox_region(
		&self,
		arg: tg::sandbox::create::Arg,
		region: String,
	) -> tg::Result<tg::sandbox::create::Output> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::sandbox::create::Arg {
			location: Some(location.into()),
			..arg
		};
		let output = client.create_sandbox(arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to create the sandbox"),
		)?;
		Ok(output)
	}

	async fn create_sandbox_remote(
		&self,
		arg: tg::sandbox::create::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<tg::sandbox::create::Output> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::create::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			..arg
		};
		let output = client.create_sandbox(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to create the sandbox"),
		)?;
		Ok(output)
	}

	pub(crate) async fn create_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		let output = self.create_sandbox(arg).await?;

		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();
		Ok(response)
	}

	pub(crate) fn normalize_sandbox_create_arg(
		mut arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Arg> {
		if let Some(tg::sandbox::Network::Bridge(bridge)) = &mut arg.network {
			bridge.ports = resolve_sandbox_ports(std::mem::take(&mut bridge.ports))?;
		}
		Ok(arg)
	}
}

fn resolve_sandbox_ports(ports: Vec<tg::sandbox::Port>) -> tg::Result<Vec<tg::sandbox::Port>> {
	let mut output = Vec::new();
	for port in ports {
		for mut port in port.expand() {
			if port.host.is_none() {
				let host = allocate_sandbox_port(&output, port.host_ip, port.protocol)?;
				port = port.with_host(tg::sandbox::PortRange::single(host));
			}
			validate_sandbox_port(&output, port)?;
			output.push(port);
		}
	}
	Ok(output)
}

fn allocate_sandbox_port(
	existing: &[tg::sandbox::Port],
	host_ip: Option<Ipv4Addr>,
	protocol: tg::sandbox::PortProtocol,
) -> tg::Result<u16> {
	for _ in 0..100 {
		let port = bind_ephemeral_port(host_ip, protocol)?;
		if !existing
			.iter()
			.any(|existing| host_port_conflicts(*existing, host_ip, port, protocol))
		{
			return Ok(port);
		}
	}
	Err(tg::error!("failed to allocate a host port"))
}

fn validate_sandbox_port(
	existing: &[tg::sandbox::Port],
	port: tg::sandbox::Port,
) -> tg::Result<()> {
	let host = port
		.host
		.ok_or_else(|| tg::error!("expected a resolved host port"))?;
	if !host.is_single() || !port.guest.is_single() {
		return Err(tg::error!("expected resolved port mappings"));
	}
	let host_port = host.start;
	if existing
		.iter()
		.any(|existing| host_port_conflicts(*existing, port.host_ip, host_port, port.protocol))
	{
		return Err(tg::error!(%host_port, "duplicate host port mapping"));
	}
	bind_specific_port(port.host_ip, host_port, port.protocol)?;
	Ok(())
}

fn host_port_conflicts(
	existing: tg::sandbox::Port,
	host_ip: Option<Ipv4Addr>,
	host_port: u16,
	protocol: tg::sandbox::PortProtocol,
) -> bool {
	existing.protocol == protocol
		&& existing
			.host
			.is_some_and(|host| host.start <= host_port && host.end >= host_port)
		&& host_ips_conflict(existing.host_ip, host_ip)
}

fn host_ips_conflict(a: Option<Ipv4Addr>, b: Option<Ipv4Addr>) -> bool {
	a.is_none() || b.is_none() || a == b
}

fn bind_ephemeral_port(
	host_ip: Option<Ipv4Addr>,
	protocol: tg::sandbox::PortProtocol,
) -> tg::Result<u16> {
	let addr = socket_addr(host_ip, 0);
	match protocol {
		tg::sandbox::PortProtocol::Tcp => {
			let listener = std::net::TcpListener::bind(addr)
				.map_err(|error| tg::error!(!error, "failed to bind a host port"))?;
			listener
				.local_addr()
				.map(|addr| addr.port())
				.map_err(|error| tg::error!(!error, "failed to get the host port"))
		},
		tg::sandbox::PortProtocol::Udp => {
			let socket = std::net::UdpSocket::bind(addr)
				.map_err(|error| tg::error!(!error, "failed to bind a host port"))?;
			socket
				.local_addr()
				.map(|addr| addr.port())
				.map_err(|error| tg::error!(!error, "failed to get the host port"))
		},
	}
}

fn bind_specific_port(
	host_ip: Option<Ipv4Addr>,
	port: u16,
	protocol: tg::sandbox::PortProtocol,
) -> tg::Result<()> {
	let addr = socket_addr(host_ip, port);
	match protocol {
		tg::sandbox::PortProtocol::Tcp => std::net::TcpListener::bind(addr)
			.map(drop)
			.map_err(|error| tg::error!(!error, %port, "failed to bind the host port")),
		tg::sandbox::PortProtocol::Udp => std::net::UdpSocket::bind(addr)
			.map(drop)
			.map_err(|error| tg::error!(!error, %port, "failed to bind the host port")),
	}
}

fn socket_addr(host_ip: Option<Ipv4Addr>, port: u16) -> SocketAddr {
	let host_ip = host_ip.unwrap_or(Ipv4Addr::UNSPECIFIED);
	SocketAddr::new(IpAddr::V4(host_ip), port)
}
