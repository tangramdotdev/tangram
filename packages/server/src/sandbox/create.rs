use {
	crate::{Server, Session},
	indoc::formatdoc,
	std::net::{IpAddr, Ipv4Addr, SocketAddr},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

impl Session {
	pub(crate) async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
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
		arg = Self::normalize_sandbox_create_arg(arg)?;
		let id = tg::sandbox::Id::new();
		let created_by = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
			.map(|user| user.id.clone());
		let connection = self
			.server
			.process_store
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into sandboxes (
					id,
					cpu,
					created_at,
					created_by,
					hostname,
					isolation,
					memory,
					mounts,
					network,
					status,
					ttl,
					\"user\"
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
					{p}12
				);
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let isolation = self.server.resolve_sandbox_isolation()?;
		Server::validate_sandbox_resources(
			&isolation,
			arg.cpu,
			arg.memory,
			arg.hostname.as_deref(),
			arg.user.as_deref(),
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
		let params = db::params![
			id.to_string(),
			cpu,
			now,
			created_by.map(|user| user.to_string()),
			arg.hostname.clone(),
			arg.isolation.map(db::value::Json),
			memory,
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network.clone().map(db::value::Json),
			tg::sandbox::Status::Created.to_string(),
			db::value::DurationSeconds(ttl),
			arg.user.clone(),
		];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		drop(connection);

		self.server.spawn_publish_sandbox_status_task(&id);
		self.spawn_publish_sandboxes_created_message_task();

		let output = tg::sandbox::create::Output { id };

		Ok(output)
	}

	async fn create_sandbox_region(
		&self,
		arg: tg::sandbox::create::Arg,
		region: String,
	) -> tg::Result<tg::sandbox::create::Output> {
		let client = self.get_region_session(region.clone()).await.map_err(
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
		let client = self.get_remote_session(remote.clone()).await.map_err(
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
