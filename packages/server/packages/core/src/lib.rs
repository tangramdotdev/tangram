use {
	self::{
		archive::Archive,
		context::{Context, Origin},
		database::Database,
		index::Index,
		messenger::Messenger,
		temp::Temp,
	},
	dashmap::{DashMap, DashSet},
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	indoc::{formatdoc, indoc},
	std::{
		collections::BTreeMap,
		ops::{ControlFlow, Deref},
		path::{Path, PathBuf},
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_uri::Uri,
	tokio::io::AsyncWriteExt as _,
};

mod archive;
mod capture;
mod compiler;
mod diagnostics;
mod instance;
mod migrations;
mod notification;
mod region;
mod specifier;
mod start;
mod usage;

pub use {
	self::session::Session,
	tangram_server_cache as cache,
	tangram_server_config::{self as config, Config},
	tangram_server_directory as directory, tangram_server_graph as graph,
	tangram_server_messenger as messenger,
};

pub mod authentication;
pub mod authorization;
pub mod billing;
pub mod check;
pub mod checkin;
pub mod checkout;
pub mod checkpoint;
pub mod checksum;
pub mod children;
pub mod clean;
pub mod clock;
pub mod component;
pub mod context;
pub mod control;
pub mod cursor;
pub mod database;
pub mod document;
pub mod format;
pub mod get;
pub mod grant;
pub mod group;
pub mod health;
pub mod http;
pub mod index;
pub mod indexer;
pub mod list;
pub mod location;
pub mod log;
pub mod lsp;
pub mod match_;
pub mod module;
pub mod node;
pub mod object;
pub mod organization;
pub mod process;
pub mod pull;
pub mod push;
pub mod read;
pub mod remote;
pub mod runner;
pub mod sandbox;
pub mod scheduler;
pub mod session;
pub mod sync;
pub mod tag;
pub mod temp;
pub mod token;
pub mod user;
pub mod verify;
pub mod vfs;
pub mod watch;
pub mod write;

#[derive(Clone)]
pub struct Shared(Arc<Owned>);

pub struct Components {
	pub authentication: Arc<dyn self::authentication::Component>,
	pub authorization: Arc<dyn self::authorization::Component>,
	pub billing: Arc<dyn self::billing::Component>,
	pub check: Arc<dyn self::check::Component>,
	pub checkin: Arc<dyn self::checkin::Component>,
	pub checkout: Arc<dyn self::checkout::Component>,
	pub checkpoint: Arc<dyn self::checkpoint::Component>,
	pub checksum: Arc<dyn self::checksum::Component>,
	pub children: Arc<dyn self::children::Component>,
	pub clean: Arc<dyn self::clean::Component>,
	pub clock: Arc<dyn self::clock::Component>,
	pub document: Arc<dyn self::document::Component>,
	pub format: Arc<dyn self::format::Component>,
	pub get: Arc<dyn self::get::Component>,
	pub grant: Arc<dyn self::grant::Component>,
	pub group: Arc<dyn self::group::Component>,
	pub health: Arc<dyn self::health::Component>,
	pub indexer: Arc<dyn self::indexer::Component>,
	pub list: Arc<dyn self::list::Component>,
	pub log: Arc<dyn self::log::Component>,
	pub lsp: Arc<dyn self::lsp::Component>,
	pub match_: Arc<dyn self::match_::Component>,
	pub module: Arc<dyn self::module::Component>,
	pub node: Arc<dyn self::node::Component>,
	pub object: Arc<dyn self::object::Component>,
	pub organization: Arc<dyn self::organization::Component>,
	pub process: Arc<dyn self::process::Component>,
	pub pull: Arc<dyn self::pull::Component>,
	pub push: Arc<dyn self::push::Component>,
	pub read: Arc<dyn self::read::Component>,
	pub remote: Arc<dyn self::remote::Component>,
	pub runner: Arc<dyn self::runner::Component>,
	pub sandbox: Arc<dyn self::sandbox::Component>,
	pub scheduler: Arc<dyn self::scheduler::Component>,
	pub sync: Arc<dyn self::sync::Component>,
	pub tag: Arc<dyn self::tag::Component>,
	pub user: Arc<dyn self::user::Component>,
	pub verify: Arc<dyn self::verify::Component>,
	pub vfs: Arc<dyn self::vfs::Component>,
	pub watch: Arc<dyn self::watch::Component>,
	pub write: Arc<dyn self::write::Component>,
}

pub struct Owned {
	server: Server,
	task: tangram_futures::task::Shared<()>,
}

#[derive(Clone)]
pub struct Server(Arc<State>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Shutdown {
	Interrupt,
	Terminate,
}

pub struct State {
	pub archive: Option<Archive>,

	pub archive_tasks: tangram_futures::task::Set<tg::Result<()>>,

	pub authentication_tokens: Tokens,

	pub authorization_tokens: Tokens,

	pub billing: Option<self::billing::Billing>,

	pub cache: self::cache::Cache,

	pub checkout_lock: self::checkout::Lock,

	pub checkpoints: Option<Arc<self::checkpoint::State>>,

	pub clock: self::clock::Clock,

	pub components: Components,

	pub config: Config,

	pub context: Context,

	pub database: Database,

	pub diagnostics: Mutex<Vec<tg::Diagnostic>>,

	pub index: Index,

	pub index_changed: tokio::sync::Notify,

	pub index_tasks: tangram_futures::task::Set<tg::Result<()>>,

	#[cfg(target_os = "linux")]
	pub ip_pool: tangram_sandbox::network::ip::Pool,

	library: Mutex<Option<Arc<Temp>>>,

	lock: Mutex<Option<tokio::fs::File>>,

	pub messenger: Messenger,

	pub notifications: self::notification::Notifications,

	pub path: PathBuf,

	regions: DashMap<String, tg::Client, fnv::FnvBuildHasher>,

	pub runner: self::runner::Runner,

	pub shutdown: tokio::sync::watch::Sender<Option<Shutdown>>,

	pub tangram_path: PathBuf,

	temps: DashSet<PathBuf, fnv::FnvBuildHasher>,

	pub version: String,
}

pub struct Tokens {
	pub private_key: Option<tg::authorization::PrivateKey>,
	pub public_keys: BTreeMap<String, tg::authorization::PublicKey>,
}

impl Owned {
	pub fn shutdown(&self, shutdown: Shutdown) {
		self.server.shutdown.send_replace(Some(shutdown));
		self.server.components.runner.stop_runner_task();
		self.task.stop();
	}

	pub async fn wait(&self) -> tg::Result<()> {
		self.task
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "the server task panicked"))
	}
}

impl Server {
	async fn destroy_unfinished_sandboxes(&self) -> tg::Result<()> {
		let session = self.session(&self.context);
		let outputs = session
			.server
			.components
			.sandbox
			.list_sandboxes_local(&session, None, None)
			.await
			.map_err(|error| tg::error!(!error, "failed to list sandboxes"))?;
		outputs
			.into_iter()
			.map(|output| async move {
				let result = self
					.components
					.scheduler
					.destroy_runner_sandbox(self, &output.id, "server restarted")
					.boxed()
					.await;
				if let Err(error) = result {
					tracing::error!(sandbox = %output.id, error = %error.trace(), "failed to destroy the sandbox");
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;
		Ok(())
	}

	#[must_use]
	pub fn arg(&self) -> tg::Arg {
		let default_url = || {
			let path = self.path.join("socket");
			let path = path.to_str().unwrap();
			Uri::builder()
				.scheme("http+unix")
				.authority(path)
				.path("")
				.build()
				.unwrap()
		};
		let http = &self.config().http;
		let url = if http.listeners.is_empty() {
			Some(default_url())
		} else {
			http.listeners
				.iter()
				.find(|listener| !matches!(listener.url.scheme(), Some("http+stdio")))
				.map(|listener| listener.url.clone())
		};
		tg::Arg {
			http: tg::Http {
				http2: self.config().http.http2,
				..Default::default()
			},
			stream_retry: Self::client_stream_retry(),
			sync: tg::sync::Config {
				max_frame_size: self.config().sync.max_frame_size,
			},
			url,
			version: Some(self.version.clone()),
			..Default::default()
		}
	}

	/// Get the stream retry of the clients that a server creates, which reconnect for as long as the server runs.
	#[must_use]
	pub fn client_stream_retry() -> tangram_futures::retry::Options {
		tangram_futures::retry::Options {
			max_retries: u64::MAX,
			..Default::default()
		}
	}

	#[must_use]
	pub fn session(&self, context: &Context) -> Session {
		Session::new(self.clone(), context.clone())
	}

	async fn set_config_remotes_with_transaction(
		transaction: &database::Transaction<'_>,
		remotes: &std::collections::BTreeMap<String, crate::config::Remote>,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		#[derive(db::row::Deserialize)]
		struct RemoteTokenRow {
			name: String,
			token: Option<String>,
		}
		let statement = indoc!(
			r"
				select name, token
				from remotes
				where principal is null;
			",
		);
		let result = transaction
			.query_all_into::<RemoteTokenRow>(statement.into(), db::params![])
			.await;
		let tokens = crate::database::retry!(result, "failed to execute the statement")
			.into_iter()
			.map(|row| (row.name, row.token))
			.collect::<std::collections::BTreeMap<_, _>>();
		let statement = indoc!(
			r"
				delete from remotes
				where principal is null;
			",
		);
		let result = transaction.execute(statement.into(), db::params![]).await;
		crate::database::retry!(result, "failed to delete the remotes");
		for (name, remote) in remotes {
			let p = transaction.p();
			let statement = formatdoc!(
				r"
					insert into remotes (name, principal, token, trusted, url)
					values ({p}1, null, {p}2, {p}3, {p}4);
				",
			);
			let token = remote
				.token
				.clone()
				.or_else(|| tokens.get(name).cloned().flatten());
			let params = db::params![name.clone(), token, remote.trusted, remote.url.to_string()];
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to insert the remote");
		}
		Ok(ControlFlow::Break(()))
	}

	#[must_use]
	pub fn config(&self) -> &Config {
		&self.config
	}

	#[must_use]
	pub fn checkouts_enabled(&self) -> bool {
		self.config.checkouts
	}

	#[must_use]
	pub fn named_checkout_maintenance_enabled(&self) -> bool {
		self.checkouts_enabled() && !self.components.vfs.mounted()
	}

	#[must_use]
	pub fn store_path(&self) -> PathBuf {
		self.path.join("store")
	}

	#[must_use]
	pub fn checkout_path(&self) -> PathBuf {
		if self.components.vfs.mounted() {
			self.path.join("checkouts")
		} else {
			self.store_path()
		}
	}

	#[must_use]
	fn library_path(&self) -> PathBuf {
		let library = self
			.library
			.lock()
			.unwrap()
			.get_or_insert_with(|| Arc::new(Temp::new(self)))
			.clone();
		library.path().to_owned()
	}

	#[must_use]
	pub fn temp_path(&self) -> PathBuf {
		self.path.join("tmp")
	}
}

impl From<Owned> for Shared {
	fn from(value: Owned) -> Self {
		Self(Arc::new(value))
	}
}

impl Deref for Shared {
	type Target = Owned;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Deref for Owned {
	type Target = Server;

	fn deref(&self) -> &Self::Target {
		&self.server
	}
}

impl Deref for Server {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Drop for Owned {
	fn drop(&mut self) {
		self.components.sync.abort();
		self.components.process.abort();
		self.components.sandbox.abort();
		self.archive_tasks.abort_all();
		self.components.checkin.abort();
		self.components.checkout.abort();
		self.library.lock().unwrap().take();
		self.components.runner.abort();
		self.components.authentication.abort();
		self.components.object.abort();
		self.components.list.abort();
		self.index_tasks.abort_all();
		self.components.vfs.abort();
		self.components.watch.abort();
		self.components.indexer.abort();
		self.components.scheduler.abort();
	}
}

async fn load_token_keys(config: Option<&config::TokenKeys>, path: &Path) -> tg::Result<Tokens> {
	let private_key = match config.and_then(|config| config.private_key.as_ref()) {
		Some(config) => {
			let bytes = match &config.path {
				Some(path) => match config.algorithm {
					tg::authorization::Algorithm::Ed25519 => tokio::fs::read(path).await.map_err(
						|error| tg::error!(!error, path = %path.display(), "failed to read the private key"),
					)?,
				},
				None => load_or_create_token_private_key(config, path).await?,
			};
			let key =
				tg::authorization::PrivateKey::new(config.name.clone(), config.algorithm, bytes);
			tg::authorization::PublicKey::from_private_key(&key)?;

			Some(key)
		},
		None => None,
	};
	let mut public_keys = BTreeMap::new();
	if let Some(config) = config {
		for config in &config.public_keys {
			let bytes = match &config.path {
				Some(path) => match config.algorithm {
					tg::authorization::Algorithm::Ed25519 => tokio::fs::read(path).await.map_err(
						|error| tg::error!(!error, path = %path.display(), "failed to read the public key"),
					)?,
				},
				None => match config.algorithm {
					tg::authorization::Algorithm::Ed25519 => {
						let matching_private_key = private_key.as_ref().filter(|private_key| {
							private_key.name == config.name
								&& private_key.algorithm == config.algorithm
						});
						let key = if let Some(private_key) = matching_private_key {
							tg::authorization::PublicKey::from_private_key(private_key)?
						} else {
							let private_key = tg::authorization::PrivateKey::generate(
								config.name.clone(),
								config.algorithm,
							)?;
							tg::authorization::PublicKey::from_private_key(&private_key)?
						};
						key.bytes
					},
				},
			};
			let key =
				tg::authorization::PublicKey::new(config.name.clone(), config.algorithm, bytes);
			if public_keys.insert(config.name.clone(), key).is_some() {
				return Err(tg::error!(name = %config.name, "duplicate public key"));
			}
		}
	}
	let tokens = Tokens {
		private_key,
		public_keys,
	};

	Ok(tokens)
}

async fn load_or_create_token_private_key(
	config: &config::TokenPrivateKey,
	path: &Path,
) -> tg::Result<Vec<u8>> {
	// Reuse the private key for the lifetime of the server directory.
	match tokio::fs::read(path).await {
		Ok(bytes) => return Ok(bytes),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
		Err(error) => {
			return Err(
				tg::error!(!error, path = %path.display(), "failed to read the private key"),
			);
		},
	}

	// Publish a complete key while the server holds the directory lock.
	let key = tg::authorization::PrivateKey::generate(config.name.clone(), config.algorithm)?;
	let directory = path.parent().unwrap();
	let temp_path = directory.join("tmp").join(uuid::Uuid::now_v7().to_string());
	let mut file = tokio::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(0o600)
		.open(&temp_path)
		.await
		.map_err(|error| tg::error!(!error, "failed to create the temporary private key file"))?;
	scopeguard::defer! {
		std::fs::remove_file(&temp_path).ok();
	}
	file.write_all(&key.bytes)
		.await
		.map_err(|error| tg::error!(!error, "failed to write the private key"))?;
	file.sync_all()
		.await
		.map_err(|error| tg::error!(!error, "failed to sync the private key"))?;
	tokio::fs::rename(&temp_path, path).await.map_err(
		|error| tg::error!(!error, path = %path.display(), "failed to persist the private key"),
	)?;
	tokio::fs::File::open(directory)
		.await
		.map_err(|error| tg::error!(!error, "failed to open the private key directory"))?
		.sync_all()
		.await
		.map_err(|error| tg::error!(!error, "failed to sync the private key directory"))?;

	Ok(key.bytes)
}
