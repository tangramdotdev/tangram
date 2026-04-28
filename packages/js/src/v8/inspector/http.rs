use {
	super::debugger,
	axum::response::IntoResponse as _,
	futures::{SinkExt as _, StreamExt as _},
	std::{
		sync::{
			Arc,
			atomic::{AtomicU64, Ordering},
		},
		thread,
	},
	tangram_client::prelude::*,
	tokio::sync::{mpsc as tokio_mpsc, oneshot},
};

pub(super) const DEFAULT_ADDRESS: std::net::SocketAddr =
	std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 9229);

pub(super) struct Server {
	shutdown: Option<oneshot::Sender<()>>,
	thread: Option<thread::JoinHandle<()>>,
	url: String,
}

#[derive(Clone)]
struct ServerState {
	command: debugger::CommandSender,
	connection_counter: Arc<AtomicU64>,
	host: std::net::SocketAddr,
	target: Arc<Target>,
}

struct Target {
	id: String,
	title: String,
	url: String,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TargetMetadata {
	description: String,
	devtools_frontend_url: String,
	id: String,
	title: String,
	#[serde(rename = "type")]
	target_type: String,
	url: String,
	web_socket_debugger_url: String,
}

#[derive(Clone, Debug, serde::Serialize)]
struct VersionMetadata {
	#[serde(rename = "Browser")]
	browser: String,
	#[serde(rename = "Protocol-Version")]
	protocol_version: String,
	#[serde(rename = "V8-Version")]
	v8_version: String,
}

impl Server {
	pub(super) fn start(
		address: std::net::SocketAddr,
		command: debugger::CommandSender,
	) -> tg::Result<Self> {
		let listener = std::net::TcpListener::bind(address)
			.map_err(|source| tg::error!(!source, "failed to bind the inspector server"))?;
		listener
			.set_nonblocking(true)
			.map_err(|source| tg::error!(!source, "failed to configure the inspector server"))?;
		let host = listener
			.local_addr()
			.map_err(|source| tg::error!(!source, "failed to get the inspector server address"))?;
		const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
			symbols: "0123456789abcdefghjkmnpqrstvwxyz",
		};
		let id = uuid::Uuid::now_v7();
		let id = ENCODING.encode(&id.into_bytes());
		let target = Arc::new(Target {
			id,
			title: format!("tangram js [pid: {}]", std::process::id()),
			url: "tangram:js".to_owned(),
		});
		let state = ServerState {
			command,
			connection_counter: Arc::new(AtomicU64::new(1)),
			host,
			target,
		};
		let url = state.websocket_debugger_url(&host.to_string());
		let (shutdown_tx, shutdown_rx) = oneshot::channel();
		let thread = thread::spawn(move || {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("failed to build the inspector runtime");
			runtime.block_on(async move {
				let listener = tokio::net::TcpListener::from_std(listener)
					.expect("failed to create the inspector listener");
				let app = axum::Router::new()
					.route("/json", axum::routing::get(json_list))
					.route("/json/list", axum::routing::get(json_list))
					.route("/json/version", axum::routing::get(json_version))
					.route("/ws/{id}", axum::routing::get(ws))
					.with_state(state);
				let shutdown = async {
					let _ = shutdown_rx.await;
				};
				let _ = axum::serve(listener, app)
					.with_graceful_shutdown(shutdown)
					.await;
			});
		});
		Ok(Self {
			shutdown: Some(shutdown_tx),
			thread: Some(thread),
			url,
		})
	}

	pub(super) fn url(&self) -> &str {
		&self.url
	}
}

impl ServerState {
	fn target_metadata(&self, host: &str) -> TargetMetadata {
		TargetMetadata {
			description: "tangram js".to_owned(),
			devtools_frontend_url: self.frontend_url(host),
			id: self.target.id.clone(),
			title: self.target.title.clone(),
			target_type: "node".to_owned(),
			url: self.target.url.clone(),
			web_socket_debugger_url: self.websocket_debugger_url(host),
		}
	}

	fn frontend_url(&self, host: &str) -> String {
		format!(
			"devtools://devtools/bundled/js_app.html?ws={host}/ws/{}&experiments=true&v8only=true",
			self.target.id
		)
	}

	fn websocket_debugger_url(&self, host: &str) -> String {
		format!("ws://{host}/ws/{}", self.target.id)
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		if let Some(shutdown) = self.shutdown.take() {
			let _ = shutdown.send(());
		}
		if let Some(thread) = self.thread.take() {
			let _ = thread.join();
		}
	}
}

async fn json_list(
	axum::extract::State(state): axum::extract::State<ServerState>,
	headers: axum::http::HeaderMap,
) -> axum::Json<Vec<TargetMetadata>> {
	let host = request_host(&headers).unwrap_or_else(|| state.host.to_string());
	axum::Json(vec![state.target_metadata(&host)])
}

async fn json_version() -> axum::Json<VersionMetadata> {
	axum::Json(VersionMetadata {
		browser: "tangram".to_owned(),
		protocol_version: "1.3".to_owned(),
		v8_version: v8::V8::get_version().to_owned(),
	})
}

async fn ws(
	axum::extract::Path(id): axum::extract::Path<String>,
	axum::extract::State(state): axum::extract::State<ServerState>,
	upgrade: axum::extract::ws::WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
	if id != state.target.id {
		return axum::http::StatusCode::NOT_FOUND.into_response();
	}
	upgrade
		.on_upgrade(move |socket| websocket(socket, state))
		.into_response()
}

async fn websocket(socket: axum::extract::ws::WebSocket, state: ServerState) {
	let id = state.connection_counter.fetch_add(1, Ordering::SeqCst);
	let (outgoing, mut outgoing_rx) = tokio_mpsc::unbounded_channel::<String>();
	let _ = state
		.command
		.send(debugger::Command::Connected { id, outgoing });
	let (mut sender, mut receiver) = socket.split();
	loop {
		tokio::select! {
			Some(message) = outgoing_rx.recv() => {
				if sender.send(axum::extract::ws::Message::Text(message.into())).await.is_err() {
					break;
				}
			},
			message = receiver.next() => {
				match message {
					Some(Ok(axum::extract::ws::Message::Text(message))) => {
						let _ = state.command.send(debugger::Command::Message {
							id,
							message: message.to_string(),
						});
					},
					Some(Ok(axum::extract::ws::Message::Close(_)) | Err(_)) | None => break,
					Some(Ok(_)) => {},
				}
			},
		}
	}
	let _ = state.command.send(debugger::Command::Disconnected { id });
}

fn request_host(headers: &axum::http::HeaderMap) -> Option<String> {
	headers
		.get(axum::http::header::HOST)
		.and_then(|host| host.to_str().ok())
		.map(ToOwned::to_owned)
}
