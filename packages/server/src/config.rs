use {
	std::{
		collections::{BTreeMap, BTreeSet},
		net::Ipv4Addr,
		path::PathBuf,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_uri::Uri,
};

#[derive(Clone, Debug)]
pub struct Config {
	pub advanced: Advanced,

	pub authentication: Authentication,

	pub authorization: Authorization,

	pub billing: Option<Billing>,

	pub checkin: Checkin,

	pub checkouts: bool,

	pub cleaner: Cleaner,

	pub database: Database,

	pub directory: Option<PathBuf>,

	pub http: Http,

	pub index: Index,

	pub indexer: Indexer,

	pub instance: Option<String>,

	pub logs: Logs,

	pub messenger: Messenger,

	pub object: Object,

	pub primary_region: Option<String>,

	pub process: Process,

	pub region: Option<String>,

	pub regions: Option<Vec<Region>>,

	pub remote_cache: RemoteCache,

	pub remotes: Option<BTreeMap<String, Remote>>,

	pub roles: BTreeSet<Role>,

	pub runner: Runner,

	pub scheduler: Scheduler,

	pub sandbox: Sandbox,

	pub sync: Sync,

	pub usage: Usage,

	pub version: Option<String>,

	pub vfs: Option<Vfs>,

	pub watch: Option<Watch>,

	pub write: Write,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Role {
	Cleaner,

	Http,

	Indexer,

	Runner,

	Scheduler,
}

#[derive(Clone, Debug)]
pub struct Advanced {
	pub checkpoints: bool,

	pub disable_version_check: bool,

	pub internal_error_locations: bool,

	pub preserve_temp_directories: bool,

	pub single_directory: bool,

	pub single_process: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Authentication {
	pub root: RootAuthentication,

	pub tokens: AuthenticationTokens,

	pub users: Option<UserAuthentication>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RootAuthentication {
	pub token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AuthenticationTokens {
	pub keys: TokenKeys,

	pub ttl: Duration,
}

#[derive(Clone, Debug)]
pub struct UserAuthentication {
	pub interval: Duration,

	pub providers: AuthenticationProviders,

	pub ttl: Duration,

	pub web_url: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct AuthenticationProviders {
	pub github: Option<Github>,

	pub insecure: Option<Insecure>,
}

#[derive(Clone, Debug, Default)]
pub struct Insecure {}

#[derive(Clone, Debug)]
pub struct Github {
	pub auth_url: String,

	pub client_id: String,

	pub client_secret: String,

	pub redirect_url: String,

	pub token_url: String,
}

#[derive(Clone, Debug)]
pub struct Billing {
	pub stripe: Stripe,
}

#[derive(Clone, Debug)]
pub struct Stripe {
	pub secret_key: String,

	pub url: Uri,

	pub webhook_secret: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Authorization {
	pub tokens: Option<TokenKeys>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TokenKeys {
	pub private_key: Option<TokenPrivateKey>,

	pub public_keys: Vec<TokenPublicKey>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TokenPrivateKey {
	pub algorithm: tg::authorization::Algorithm,

	pub name: String,

	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TokenPublicKey {
	pub algorithm: tg::authorization::Algorithm,

	pub name: String,

	pub path: Option<PathBuf>,
}

impl Default for AuthenticationTokens {
	fn default() -> Self {
		Self {
			keys: TokenKeys::default(),
			ttl: default_authentication_token_ttl(),
		}
	}
}

impl Default for Authorization {
	fn default() -> Self {
		Self {
			tokens: default_authorization_tokens(),
		}
	}
}

impl Default for TokenKeys {
	fn default() -> Self {
		Self {
			private_key: Some(TokenPrivateKey {
				algorithm: tg::authorization::Algorithm::Ed25519,
				name: "default".to_owned(),
				path: None,
			}),
			public_keys: vec![TokenPublicKey {
				algorithm: tg::authorization::Algorithm::Ed25519,
				name: "default".to_owned(),
				path: None,
			}],
		}
	}
}

#[derive(Clone, Debug, Default)]
pub struct Checkin {
	pub blob: CheckinBlob,

	pub checkout: CheckinCheckout,

	pub directory: CheckinDirectory,
}

#[derive(Clone, Debug)]
pub struct CheckinBlob {
	pub concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct CheckinCheckout {
	pub batch_size: usize,

	pub concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct CheckinDirectory {
	pub max_branch_children: usize,

	pub max_leaf_entries: usize,
}

#[derive(Clone, Debug)]
pub struct Cleaner {
	pub batch_size: usize,

	pub concurrency: usize,

	pub partition_end: u64,

	pub partition_start: u64,
}

#[derive(Clone, Debug)]
pub enum Database {
	Postgres(PostgresDatabase),

	Sqlite(SqliteDatabase),

	Turso(TursoDatabase),
}

#[derive(Clone, Debug)]
pub struct DatabaseOutbox {
	pub batch_size: usize,
}

#[derive(Clone, Debug)]
pub struct PostgresDatabase {
	pub outbox: DatabaseOutbox,

	pub read: PostgresDatabaseConnection,

	pub retry: Retry,

	pub write: PostgresDatabaseConnection,
}

#[derive(Clone, Debug)]
pub struct PostgresDatabaseConnection {
	pub pool: DatabasePool,

	pub url: Uri,
}

#[derive(Clone, Debug, Default)]
pub struct DatabasePool {
	pub max: Option<usize>,

	pub min: Option<usize>,

	pub ttl: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct SqliteDatabase {
	pub outbox: DatabaseOutbox,

	pub path: PathBuf,

	pub pool: DatabasePool,

	pub retry: Retry,
}

#[derive(Clone, Debug)]
pub struct TursoDatabase {
	pub outbox: DatabaseOutbox,

	pub path: PathBuf,

	pub pool: DatabasePool,

	pub retry: Retry,
}

#[derive(Clone, Debug)]
pub struct Http {
	pub coalescing_target_size: usize,

	pub idle_timeout: Duration,

	pub listeners: Vec<HttpListener>,
}

#[derive(Clone, Debug)]
pub struct HttpListener {
	pub tls: Option<HttpTls>,

	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct HttpTls {
	pub certificate: PathBuf,

	pub key: PathBuf,
}

#[derive(Clone, Debug, derive_more::IsVariant)]
pub enum Index {
	Fdb(FdbIndex),

	Lmdb(LmdbIndex),
}

#[derive(Clone, Debug)]
pub struct FdbIndexAuthorize {
	pub ancestor: IndexAuthorizeSearch,

	pub concurrency: usize,

	pub descendant: IndexAuthorizeSearch,

	pub subtree: IndexAuthorizeSubtree,
}

#[derive(Clone, Debug, Default)]
pub struct LmdbIndexAuthorize {
	pub ancestor: IndexAuthorizeSearch,

	pub descendant: IndexAuthorizeSearch,

	pub subtree: IndexAuthorizeSubtree,
}

#[derive(Clone, Debug)]
pub struct IndexAuthorizeSearch {
	pub max_depth: usize,

	pub max_edges: usize,

	pub max_nodes: usize,

	pub page_size: usize,
}

#[derive(Clone, Debug)]
pub struct IndexAuthorizeSubtree {
	pub max_depth: usize,

	pub max_objects: usize,

	pub max_processes: usize,
}

#[derive(Clone, Debug)]
pub struct FdbIndex {
	pub authorize: FdbIndexAuthorize,

	pub cluster: PathBuf,

	pub partition_total: u64,

	pub prefix: Option<String>,

	pub read_request_batch_size: usize,

	pub read_transaction_concurrency: usize,

	pub usage_partition_total: u64,

	pub write_operation_batch_size: usize,

	pub write_transaction_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct LmdbIndex {
	pub authorize: LmdbIndexAuthorize,

	pub map_size: usize,

	pub path: PathBuf,

	pub read_request_batch_size: usize,

	pub read_transaction_concurrency: usize,

	pub usage_partition_total: u64,

	pub write_operation_batch_size: usize,
}

#[derive(Clone, Debug)]
pub struct Indexer {
	pub database_outbox_wakeup_interval: Duration,

	pub log_compaction: IndexerLogCompaction,

	pub max_process_depth: usize,

	pub message_retry: Retry,

	pub message_timeout: Duration,

	pub object_outbox_wakeup_interval: Duration,

	pub partition_end: u64,

	pub partition_start: u64,

	pub poll_interval: Duration,

	pub updates: IndexerUpdates,

	pub usage: IndexerUsage,
}

#[derive(Clone, Debug)]
pub struct IndexerLogCompaction {
	pub batch_size: usize,

	pub concurrency: usize,

	pub enabled: bool,

	pub wakeup_interval: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct IndexerUsage {
	pub aggregation: IndexerUsageAggregation,

	pub storage: IndexerUpdate,
}

#[derive(Clone, Debug)]
pub struct IndexerUsageAggregation {
	pub batch_size: usize,

	pub concurrency: usize,

	pub enabled: bool,

	pub poll_interval: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct IndexerUpdates {
	pub grants: IndexerUpdate,

	pub nodes: IndexerUpdate,
}

#[derive(Clone, Debug)]
pub struct IndexerUpdate {
	pub batch_size: usize,

	pub concurrency: usize,
}

#[derive(Clone, Debug, Default)]
pub struct Logs {
	pub store: LogStore,
}

#[derive(Clone, Debug)]
pub enum LogStore {
	Fdb(FdbLogStore),

	Lmdb(LmdbLogStore),

	Memory,
}

#[derive(Clone, Debug)]
pub struct FdbLogStore {
	pub cluster: PathBuf,
	pub prefix: Option<String>,
}

#[derive(Clone, Debug)]
pub struct LmdbLogStore {
	pub map_size: usize,

	pub path: PathBuf,
}

#[derive(Clone, Debug, Default)]
pub enum Messenger {
	#[default]
	Memory,

	Nats(NatsMessenger),
}

#[derive(Clone, Debug)]
pub struct NatsMessenger {
	pub credentials: Option<PathBuf>,
	pub password: Option<String>,

	pub url: Uri,
	pub username: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Object {
	pub grant_time_to_live: Duration,

	pub grant_time_to_touch: Duration,

	pub outbox: ObjectOutbox,

	pub store: ObjectStore,

	pub time_to_index: Duration,

	pub time_to_live: Duration,

	pub time_to_touch: Duration,
}

#[derive(Clone, Debug)]
pub struct ObjectOutbox {
	pub batch_size: usize,

	pub fragment_size: usize,

	pub partition_total: u64,
}

#[derive(Clone, Debug)]
pub enum ObjectStore {
	Lmdb(LmdbObjectStore),

	Memory(MemoryObjectStore),

	Scylla(ScyllaObjectStore),
}

#[derive(Clone, Debug)]
pub struct LmdbObjectStore {
	pub map_size: usize,

	pub path: PathBuf,

	pub posix_sem_prefix: Option<String>,

	pub read_batch_size: usize,

	pub read_concurrency: usize,

	pub write_batch_size: usize,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryObjectStore {}

#[derive(Clone, Debug)]
pub struct ScyllaObjectStore {
	pub addr: String,

	pub connections: Option<usize>,

	pub keyspace: String,

	pub password: Option<String>,

	pub speculative_execution: Option<ScyllaObjectStoreSpeculativeExecution>,

	pub username: Option<String>,
}

#[derive(Clone, Debug)]
pub enum ScyllaObjectStoreSpeculativeExecution {
	Percentile(ScyllaObjectStorePercentileSpeculativeExecution),

	Simple(ScyllaObjectStoreSimpleSpeculativeExecution),
}

#[derive(Clone, Debug)]
pub struct ScyllaObjectStorePercentileSpeculativeExecution {
	pub max_retry_count: usize,

	pub percentile: f64,
}

#[derive(Clone, Debug)]
pub struct ScyllaObjectStoreSimpleSpeculativeExecution {
	pub max_retry_count: usize,

	pub retry_interval: u64,
}

#[derive(Clone, Debug)]
pub struct Process {
	pub children_wakeup_interval: Duration,

	pub grant_time_to_live: Duration,

	pub grant_time_to_touch: Duration,

	pub spawn: Spawn,

	pub status_wakeup_interval: Duration,

	pub stdio_wakeup_interval: Duration,

	pub time_to_index: Duration,

	pub time_to_live: Duration,

	pub time_to_touch: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct Spawn {
	pub create_delay: Duration,

	pub host: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Region {
	pub name: String,

	pub reconnect: Option<Reconnect>,

	pub retry: Option<Retry>,

	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct Reconnect {
	pub backoff: Duration,

	pub jitter: Duration,

	pub max_delay: Duration,

	pub max_retries: u64,
}

#[derive(Clone, Debug)]
pub struct Retry {
	pub backoff: Duration,

	pub jitter: Duration,

	pub max_delay: Duration,

	pub max_retries: u64,
}

#[derive(Clone, Debug)]
pub struct Remote {
	pub token: Option<String>,

	pub trusted: bool,

	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct RemoteCache {
	pub time_to_live: Duration,
}

#[derive(Clone, Debug)]
pub struct Runner {
	pub cpus: Option<u64>,

	pub heartbeat_interval: Duration,

	pub id: Option<tg::runner::Id>,

	pub js: Js,

	pub memory: Option<u64>,

	pub process_state_ttl: Duration,

	pub remote: Option<String>,

	pub sandbox_pool_size: usize,

	pub sandbox_state_ttl: Duration,

	pub scheduler_ttl: Duration,

	pub stdio_drain_timeout: Duration,

	pub token: Option<String>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Js {
	pub engine: JsEngine,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum JsEngine {
	#[default]
	Auto,

	QuickJs,

	V8,
}

#[derive(Clone, Debug)]
pub struct Scheduler {
	pub create_sandbox_queue_capacity: usize,

	pub create_sandbox_timeout: Duration,

	pub default_cpu: u64,

	pub default_memory: u64,

	pub heartbeat_interval: Duration,

	pub heartbeat_ttl: Duration,

	pub inbox_ttl: Duration,

	pub message_retry: Retry,

	pub message_timeout: Duration,

	pub max_create_sandbox_attempts: usize,

	pub max_create_sandbox_requests: usize,

	pub max_create_sandbox_requests_per_runner: usize,

	pub runner_ttl: Duration,
}

#[derive(Clone, Debug)]
pub struct Sandbox {
	pub isolation: SandboxIsolation,

	pub network: SandboxNetwork,

	pub nice: u8,

	pub processes_wakeup_interval: Duration,

	pub status_wakeup_interval: Duration,

	pub time_to_live: Duration,
}

#[derive(Clone, Debug)]
pub struct SandboxIsolation {
	pub container: Option<ContainerSandboxIsolation>,

	pub default: Option<SandboxIsolationDefault>,

	pub seatbelt: Option<SeatbeltSandboxIsolation>,

	pub vm: Option<VmSandboxIsolation>,
}

#[derive(Clone, Debug, Default)]
pub struct ContainerSandboxIsolation {
	pub max_pids: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub enum SandboxIsolationDefault {
	Container,
	Seatbelt,
	Vm,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SeatbeltSandboxIsolation {}

#[derive(Clone, Debug)]
pub struct VmSandboxIsolation {
	pub cloud_hypervisor_path: Option<PathBuf>,

	pub dax: Option<Dax>,

	pub kernel_path: PathBuf,

	pub listener_port: Option<u16>,

	pub max_cpu: u64,

	pub max_memory: u64,

	pub snapshot: Option<PathBuf>,

	pub snapshot_cpu: u64,

	pub snapshot_memory: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Dax {
	pub window_size: usize,
}

#[derive(Clone, Debug)]
pub struct SandboxNetwork {
	pub dns: Vec<Ipv4Addr>,

	pub firewall: SandboxNetworkFirewall,

	pub ip_ranges: Vec<IpRange>,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum SandboxNetworkFirewall {
	Iptables,

	#[default]
	Nft,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IpRange {
	pub max: Ipv4Addr,

	pub min: Ipv4Addr,
}

#[derive(Clone, Debug)]
pub struct Sync {
	pub get: SyncGet,

	pub grant_time_to_live: Duration,

	pub grant_time_to_touch: Duration,

	pub max_frame_size: u64,

	pub put: SyncPut,

	pub retry: Retry,
}

#[derive(Clone, Debug, Default)]
pub struct SyncGet {
	pub database: SyncGetDatabase,

	pub index: SyncGetIndex,

	pub queue: SyncGetQueue,

	pub store: SyncGetStore,
}

#[derive(Clone, Copy, Debug)]
pub struct SyncGetDatabase {
	pub batch_size: usize,
}

#[derive(Clone, Debug)]
pub struct SyncGetIndex {
	pub object_batch_size: usize,

	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct SyncGetQueue {
	pub object_batch_size: usize,

	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct SyncGetStore {
	pub lmdb: SyncGetStoreObject,

	pub memory: SyncGetStoreObject,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,

	pub scylla: SyncGetStoreObject,
}

#[derive(Clone, Debug, Default)]
pub struct SyncGetStoreObject {
	pub object_concurrency: usize,

	pub object_max_batch: usize,

	pub object_max_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct SyncPut {
	pub index: SyncPutIndex,

	pub queue: SyncPutQueue,

	pub resolve: SyncPutResolve,

	pub store: SyncPutStore,
}

#[derive(Clone, Debug)]
pub struct SyncPutIndex {
	pub object_batch_size: usize,

	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct SyncPutQueue {
	pub object_batch_size: usize,

	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[derive(Clone, Debug)]
pub struct SyncPutResolve {
	pub batch_size: usize,

	pub batch_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct SyncPutStore {
	pub object_batch_size: usize,
	pub object_batch_timeout: Duration,

	pub object_concurrency: usize,

	pub process_batch_size: usize,

	pub process_batch_timeout: Duration,

	pub process_concurrency: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct Usage {
	pub day_time_to_live: Duration,

	pub delta_time_to_live: Duration,

	pub enabled: bool,

	pub hour_time_to_live: Duration,

	pub month_time_to_live: Duration,

	pub week_time_to_live: Duration,
}

#[derive(Clone, Debug)]
pub struct Vfs {
	/// The macOS app group identifier.
	pub app_group_identifier: Option<String>,

	pub kind: VfsKind,

	pub io: VfsIo,

	pub passthrough: VfsPassthrough,

	pub sqpoll: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum VfsKind {
	#[default]
	Auto,

	Fskit,

	Fuse,

	Nfs,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum VfsIo {
	#[default]
	Auto,

	IoUring,

	ReadWrite,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum VfsPassthrough {
	#[default]
	Auto,

	Disabled,

	Required,
}

#[derive(Clone, Debug)]
pub struct Watch {
	pub ttl: Duration,
}

#[derive(Clone, Debug)]
pub struct Write {
	pub avg_leaf_size: usize,

	pub checkout_pointers: bool,

	pub max_branch_children: usize,

	pub max_leaf_size: usize,

	pub min_leaf_size: usize,
}

impl Config {
	#[must_use]
	pub fn primary_region(&self) -> Option<&str> {
		self.primary_region.as_deref()
	}

	#[must_use]
	pub fn with_directory(directory: PathBuf) -> Self {
		Self {
			directory: Some(directory),
			..Default::default()
		}
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			advanced: Advanced::default(),
			authentication: Authentication::default(),
			authorization: Authorization::default(),
			billing: None,
			checkin: Checkin::default(),
			checkouts: true,
			cleaner: Cleaner::default(),
			database: Database::default(),
			directory: None,
			http: Http::default(),
			index: Index::default(),
			indexer: Indexer::default(),
			instance: None,
			logs: Logs::default(),
			messenger: Messenger::default(),
			object: Object::default(),
			primary_region: None,
			process: Process::default(),
			region: None,
			regions: None,
			remote_cache: RemoteCache::default(),
			remotes: None,
			roles: default_roles(),
			runner: Runner::default(),
			scheduler: Scheduler::default(),
			sandbox: Sandbox::default(),
			sync: Sync::default(),
			usage: Usage::default(),
			version: None,
			vfs: None,
			watch: Some(Watch::default()),
			write: Write::default(),
		}
	}
}

impl Default for Advanced {
	fn default() -> Self {
		Self {
			checkpoints: false,
			disable_version_check: false,
			internal_error_locations: false,
			preserve_temp_directories: false,
			single_directory: true,
			single_process: true,
		}
	}
}

impl Default for UserAuthentication {
	fn default() -> Self {
		Self {
			interval: default_login_interval(),
			providers: AuthenticationProviders::default(),
			ttl: default_login_ttl(),
			web_url: None,
		}
	}
}

impl Default for CheckinBlob {
	fn default() -> Self {
		Self { concurrency: 8 }
	}
}

impl Default for CheckinCheckout {
	fn default() -> Self {
		Self {
			batch_size: 128,
			concurrency: 8,
		}
	}
}

impl Default for CheckinDirectory {
	fn default() -> Self {
		Self {
			max_branch_children: 128,
			max_leaf_entries: 1024,
		}
	}
}

impl Default for Cleaner {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			concurrency: 1,
			partition_end: 1,
			partition_start: 0,
		}
	}
}

impl Default for Database {
	fn default() -> Self {
		Self::Sqlite(SqliteDatabase::default())
	}
}

impl Database {
	#[must_use]
	pub fn outbox(&self) -> &DatabaseOutbox {
		match self {
			Self::Postgres(config) => &config.outbox,
			Self::Sqlite(config) => &config.outbox,
			Self::Turso(config) => &config.outbox,
		}
	}
}

impl Default for DatabaseOutbox {
	fn default() -> Self {
		Self { batch_size: 1024 }
	}
}

impl Default for PostgresDatabase {
	fn default() -> Self {
		let connection = PostgresDatabaseConnection::default();
		Self {
			outbox: DatabaseOutbox::default(),
			read: connection.clone(),
			retry: database_retry_default(),
			write: connection,
		}
	}
}

impl Default for PostgresDatabaseConnection {
	fn default() -> Self {
		Self {
			pool: DatabasePool::default(),
			url: "postgres://localhost:5432".parse().unwrap(),
		}
	}
}

impl Default for SqliteDatabase {
	fn default() -> Self {
		Self {
			outbox: DatabaseOutbox::default(),
			path: PathBuf::from("database"),
			pool: DatabasePool::default(),
			retry: database_retry_default(),
		}
	}
}

impl Default for TursoDatabase {
	fn default() -> Self {
		Self {
			outbox: DatabaseOutbox::default(),
			path: PathBuf::from("database"),
			pool: DatabasePool::default(),
			retry: database_retry_default(),
		}
	}
}

impl Default for Http {
	fn default() -> Self {
		Self {
			coalescing_target_size: tangram_http::body::coalesce::DEFAULT_COALESCING_TARGET_SIZE,
			idle_timeout: Duration::from_secs(30),
			listeners: Vec::new(),
		}
	}
}

impl Default for Index {
	fn default() -> Self {
		Self::Lmdb(LmdbIndex::default())
	}
}

impl Default for IndexAuthorizeSearch {
	fn default() -> Self {
		Self {
			max_depth: 16,
			max_edges: 1024,
			max_nodes: 1024,
			page_size: 64,
		}
	}
}

impl Default for IndexAuthorizeSubtree {
	fn default() -> Self {
		Self {
			max_depth: 16,
			max_objects: 1024,
			max_processes: 1024,
		}
	}
}

impl Default for FdbIndexAuthorize {
	fn default() -> Self {
		Self {
			ancestor: IndexAuthorizeSearch::default(),
			concurrency: 64,
			descendant: IndexAuthorizeSearch::default(),
			subtree: IndexAuthorizeSubtree::default(),
		}
	}
}

impl Default for FdbIndex {
	fn default() -> Self {
		Self {
			authorize: FdbIndexAuthorize::default(),
			cluster: PathBuf::from("/etc/foundationdb/fdb.cluster"),
			partition_total: 1,
			prefix: None,
			read_request_batch_size: 64,
			read_transaction_concurrency: 64,
			usage_partition_total: 1,
			write_operation_batch_size: 8_000,
			write_transaction_concurrency: 256,
		}
	}
}

impl Default for LmdbIndex {
	fn default() -> Self {
		Self {
			authorize: LmdbIndexAuthorize::default(),
			map_size: 1_099_511_627_776,
			path: PathBuf::from("index"),
			read_request_batch_size: 64,
			read_transaction_concurrency: 4,
			usage_partition_total: 1,
			write_operation_batch_size: 8_000,
		}
	}
}

impl Default for Indexer {
	fn default() -> Self {
		Self {
			database_outbox_wakeup_interval: Duration::from_mins(1),
			log_compaction: IndexerLogCompaction::default(),
			max_process_depth: 1024,
			message_retry: message_retry_default(),
			message_timeout: Duration::from_secs(10),
			object_outbox_wakeup_interval: Duration::from_mins(1),
			partition_end: 1,
			partition_start: 0,
			poll_interval: Duration::from_millis(10),
			updates: IndexerUpdates::default(),
			usage: IndexerUsage::default(),
		}
	}
}

impl Default for IndexerLogCompaction {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			concurrency: 1,
			enabled: true,
			wakeup_interval: Duration::from_mins(1),
		}
	}
}

impl Default for IndexerUsageAggregation {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			concurrency: 1,
			enabled: true,
			poll_interval: Duration::from_secs(1),
		}
	}
}

impl Default for IndexerUpdate {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			concurrency: 1,
		}
	}
}

impl Default for LogStore {
	fn default() -> Self {
		Self::Lmdb(LmdbLogStore::default())
	}
}

impl Default for FdbLogStore {
	fn default() -> Self {
		Self {
			cluster: PathBuf::from("/etc/foundationdb/fdb.cluster"),
			prefix: None,
		}
	}
}

impl Default for LmdbLogStore {
	fn default() -> Self {
		Self {
			map_size: 1_099_511_627_776,
			path: PathBuf::from("logs"),
		}
	}
}

impl Default for NatsMessenger {
	fn default() -> Self {
		let url = "nats://localhost:4222".parse().unwrap();
		Self {
			credentials: None,
			password: None,
			url,
			username: None,
		}
	}
}

impl Default for Object {
	fn default() -> Self {
		Self {
			grant_time_to_live: default_object_grant_time_to_live(),
			grant_time_to_touch: default_time_to_touch(),
			outbox: ObjectOutbox::default(),
			store: ObjectStore::default(),
			time_to_index: default_time_to_index(),
			time_to_live: default_time_to_live(),
			time_to_touch: default_time_to_touch(),
		}
	}
}

impl Default for ObjectOutbox {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			fragment_size: 1024,
			partition_total: 1,
		}
	}
}

impl Default for ObjectStore {
	fn default() -> Self {
		Self::Lmdb(LmdbObjectStore::default())
	}
}

impl Default for LmdbObjectStore {
	fn default() -> Self {
		Self {
			map_size: 1_099_511_627_776,
			path: PathBuf::from("objects"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		}
	}
}

impl LmdbObjectStore {
	/// Returns the configured POSIX semaphore prefix or the app group default.
	#[must_use]
	pub fn resolved_posix_sem_prefix(&self) -> Option<String> {
		self.posix_sem_prefix.clone().or_else(|| {
			std::env::var("TANGRAM_MACOS_APP_GROUP_IDENTIFIER")
				.ok()
				.filter(|value| !value.is_empty())
				.map(|identifier| format!("{identifier}/lmdb"))
		})
	}
}

impl Default for Process {
	fn default() -> Self {
		Self {
			children_wakeup_interval: Duration::from_mins(1),
			grant_time_to_live: default_process_grant_time_to_live(),
			grant_time_to_touch: default_time_to_touch(),
			spawn: Spawn::default(),
			status_wakeup_interval: Duration::from_mins(1),
			stdio_wakeup_interval: Duration::from_mins(1),
			time_to_index: default_time_to_index(),
			time_to_live: default_time_to_live(),
			time_to_touch: default_time_to_touch(),
		}
	}
}

impl Default for RemoteCache {
	fn default() -> Self {
		Self {
			time_to_live: Duration::from_mins(5),
		}
	}
}

impl Default for Runner {
	fn default() -> Self {
		Self {
			cpus: None,
			heartbeat_interval: Duration::from_secs(1),
			id: None,
			js: Js::default(),
			memory: None,
			process_state_ttl: Duration::from_mins(1),
			remote: None,
			sandbox_pool_size: 1,
			sandbox_state_ttl: Duration::from_mins(1),
			scheduler_ttl: Duration::from_secs(10),
			stdio_drain_timeout: Duration::from_secs(1),
			token: None,
		}
	}
}

impl Default for Scheduler {
	fn default() -> Self {
		Self {
			create_sandbox_queue_capacity: default_scheduler_create_sandbox_queue_capacity(),
			create_sandbox_timeout: Duration::from_secs(10),
			default_cpu: default_scheduler_cpu(),
			default_memory: default_scheduler_memory(),
			heartbeat_interval: Duration::from_secs(1),
			heartbeat_ttl: Duration::from_secs(10),
			inbox_ttl: Duration::from_mins(1),
			message_retry: message_retry_default(),
			message_timeout: Duration::from_secs(10),
			max_create_sandbox_attempts: default_scheduler_max_create_sandbox_attempts(),
			max_create_sandbox_requests: default_scheduler_max_create_sandbox_requests(),
			max_create_sandbox_requests_per_runner:
				default_scheduler_max_create_sandbox_requests_per_runner(),
			runner_ttl: Duration::from_secs(10),
		}
	}
}

impl Default for Sandbox {
	fn default() -> Self {
		Self {
			isolation: SandboxIsolation::default(),
			network: SandboxNetwork::default(),
			nice: 5,
			processes_wakeup_interval: Duration::from_mins(1),
			status_wakeup_interval: Duration::from_mins(1),
			time_to_live: default_time_to_live(),
		}
	}
}

impl Default for SandboxIsolation {
	fn default() -> Self {
		if cfg!(target_os = "linux") {
			Self {
				container: Some(ContainerSandboxIsolation::default()),
				default: None,
				seatbelt: None,
				vm: None,
			}
		} else if cfg!(target_os = "macos") {
			Self {
				container: None,
				default: None,
				seatbelt: Some(SeatbeltSandboxIsolation {}),
				vm: None,
			}
		} else {
			Self {
				container: None,
				default: None,
				seatbelt: None,
				vm: None,
			}
		}
	}
}

impl Default for Dax {
	fn default() -> Self {
		Self {
			window_size: 8 * 1024 * 1024 * 1024,
		}
	}
}

impl Default for SandboxNetwork {
	fn default() -> Self {
		Self {
			dns: default_dns(),
			firewall: SandboxNetworkFirewall::default(),
			ip_ranges: default_ip_ranges(),
		}
	}
}

impl Default for Sync {
	fn default() -> Self {
		Self {
			get: SyncGet::default(),
			grant_time_to_live: default_time_to_live(),
			grant_time_to_touch: default_time_to_touch(),
			max_frame_size: default_sync_max_frame_size(),
			put: SyncPut::default(),
			retry: sync_retry_default(),
		}
	}
}

impl Default for SyncGetDatabase {
	fn default() -> Self {
		Self { batch_size: 128 }
	}
}

impl Default for SyncGetIndex {
	fn default() -> Self {
		Self {
			object_batch_size: 16,
			object_batch_timeout: Duration::ZERO,
			object_concurrency: 8,
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
		}
	}
}

impl Default for SyncGetQueue {
	fn default() -> Self {
		Self {
			object_batch_size: 16,
			object_batch_timeout: Duration::ZERO,
			object_concurrency: 8,
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
		}
	}
}

impl Default for SyncGetStore {
	fn default() -> Self {
		Self {
			lmdb: SyncGetStoreObject {
				object_concurrency: 1,
				object_max_batch: 1_000,
				object_max_bytes: 1_000_000,
			},
			memory: SyncGetStoreObject {
				object_concurrency: 1,
				object_max_batch: 1,
				object_max_bytes: u64::MAX,
			},
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
			scylla: SyncGetStoreObject {
				object_concurrency: 64,
				object_max_batch: 1_000,
				object_max_bytes: 65_536,
			},
		}
	}
}

impl Default for SyncPutIndex {
	fn default() -> Self {
		Self {
			object_batch_size: 16,
			object_batch_timeout: Duration::ZERO,
			object_concurrency: 8,
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
		}
	}
}

impl Default for SyncPutQueue {
	fn default() -> Self {
		Self {
			object_batch_size: 16,
			object_batch_timeout: Duration::ZERO,
			object_concurrency: 8,
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
		}
	}
}

impl Default for SyncPutResolve {
	fn default() -> Self {
		Self {
			batch_size: 16,
			batch_timeout: Duration::ZERO,
		}
	}
}

impl Default for SyncPutStore {
	fn default() -> Self {
		Self {
			object_batch_size: 16,
			object_batch_timeout: Duration::ZERO,
			object_concurrency: 8,
			process_batch_size: 16,
			process_batch_timeout: Duration::ZERO,
			process_concurrency: 8,
		}
	}
}

impl Usage {
	pub fn validate(&self) -> tg::Result<()> {
		// Retain each input kind for at least one complete parent period.
		if self.delta_time_to_live < Duration::from_hours(1) {
			return Err(tg::error!(
				"the usage delta time to live must be at least one hour"
			));
		}
		if self.hour_time_to_live < Duration::from_hours(24) {
			return Err(tg::error!(
				"the usage hour time to live must be at least 24 hours"
			));
		}
		if self.day_time_to_live < Duration::from_hours(31 * 24) {
			return Err(tg::error!(
				"the usage day time to live must be at least 31 days"
			));
		}

		Ok(())
	}
}

impl Default for Usage {
	fn default() -> Self {
		Self {
			day_time_to_live: Duration::from_hours(45 * 24),
			delta_time_to_live: Duration::from_hours(2),
			enabled: false,
			hour_time_to_live: Duration::from_hours(36),
			month_time_to_live: Duration::from_hours(365 * 24),
			week_time_to_live: Duration::from_hours(6 * 7 * 24),
		}
	}
}

impl Default for Vfs {
	fn default() -> Self {
		Self {
			app_group_identifier: None,
			kind: VfsKind::Auto,
			io: VfsIo::Auto,
			passthrough: VfsPassthrough::Auto,
			sqpoll: true,
		}
	}
}

impl Vfs {
	/// Resolves the macOS app group socket path.
	#[must_use]
	pub fn resolved_app_group_socket(&self) -> Option<PathBuf> {
		if let Some(path) = std::env::var_os("TANGRAM_MACOS_APP_GROUP_SOCKET") {
			return Some(PathBuf::from(path));
		}
		let identifier = self.app_group_identifier.as_ref()?;
		let home = std::env::var_os("HOME")?;
		let socket = PathBuf::from(home)
			.join("Library/Group Containers")
			.join(identifier)
			.join("socket");
		Some(socket)
	}
}

impl Default for Watch {
	fn default() -> Self {
		Self {
			ttl: Duration::from_hours(1),
		}
	}
}

impl Default for Write {
	fn default() -> Self {
		Self {
			avg_leaf_size: 65_536,
			checkout_pointers: true,
			max_branch_children: 1_024,
			max_leaf_size: 131_072,
			min_leaf_size: 4_096,
		}
	}
}

impl From<Retry> for tangram_futures::retry::Options {
	fn from(retry: Retry) -> Self {
		Self {
			backoff: retry.backoff,
			jitter: retry.jitter,
			max_delay: retry.max_delay,
			max_retries: retry.max_retries,
		}
	}
}

mod ip_range {
	use {super::IpRange, std::net::Ipv4Addr, tangram_client::prelude::*};

	impl std::fmt::Display for IpRange {
		fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			write!(f, "{}-{}", self.min, self.max)
		}
	}

	impl std::str::FromStr for IpRange {
		type Err = tg::Error;

		fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
			if let Some((min, max)) = s.split_once('-') {
				let min = min
					.trim()
					.parse()
					.map_err(|error| tg::error!(!error, "invalid minimum address"))?;
				let max = max
					.trim()
					.parse()
					.map_err(|error| tg::error!(!error, "invalid maximum address"))?;
				Ok(IpRange { max, min })
			} else if let Some((addr, prefix)) = s.split_once('/') {
				let addr: Ipv4Addr = addr
					.trim()
					.parse()
					.map_err(|error| tg::error!(!error, "invalid address"))?;
				let prefix: u8 = prefix
					.trim()
					.parse()
					.map_err(|error| tg::error!(!error, "invalid prefix"))?;
				if prefix > 32 {
					return Err(tg::error!(%prefix, "invalid prefix"));
				}
				let bits = u32::from(addr);
				let mask = if prefix == 0 {
					0
				} else {
					u32::MAX << (32 - prefix)
				};
				let min = Ipv4Addr::from(bits & mask);
				let max = Ipv4Addr::from((bits & mask) | !mask);
				Ok(IpRange { max, min })
			} else {
				Err(tg::error!(%s, "invalid IP range"))
			}
		}
	}
}

fn default_ip_ranges() -> Vec<IpRange> {
	vec!["172.18.0.4-172.31.255.255".parse().unwrap()]
}

fn default_dns() -> Vec<Ipv4Addr> {
	Vec::new()
}

fn database_retry_default() -> Retry {
	let options = tangram_futures::retry::Options {
		max_retries: 20,
		..tangram_futures::retry::Options::default()
	};
	Retry {
		backoff: options.backoff,
		jitter: options.jitter,
		max_delay: options.max_delay,
		max_retries: options.max_retries,
	}
}

fn message_retry_default() -> Retry {
	let options = tangram_futures::retry::Options {
		max_retries: u64::MAX,
		..tangram_futures::retry::Options::default()
	};
	Retry {
		backoff: options.backoff,
		jitter: options.jitter,
		max_delay: options.max_delay,
		max_retries: options.max_retries,
	}
}

fn default_time_to_index() -> Duration {
	Duration::from_mins(10)
}

fn default_time_to_live() -> Duration {
	Duration::from_hours(24)
}

fn default_object_grant_time_to_live() -> Duration {
	Duration::from_hours(24)
}

fn default_process_grant_time_to_live() -> Duration {
	Duration::from_hours(24)
}

fn default_time_to_touch() -> Duration {
	Duration::from_hours(1)
}

fn default_login_interval() -> Duration {
	Duration::from_secs(5)
}

fn default_login_ttl() -> Duration {
	Duration::from_mins(15)
}

fn default_authentication_token_ttl() -> Duration {
	Duration::from_hours(24)
}

fn default_sync_max_frame_size() -> u64 {
	tg::sync::Config::default().max_frame_size
}

fn sync_retry_default() -> Retry {
	let options = tangram_futures::retry::Options::default();
	Retry {
		backoff: options.backoff,
		jitter: options.jitter,
		max_delay: options.max_delay,
		max_retries: options.max_retries,
	}
}

#[expect(clippy::unnecessary_wraps)]
fn default_authorization_tokens() -> Option<TokenKeys> {
	Some(TokenKeys::default())
}

fn default_roles() -> BTreeSet<Role> {
	[
		Role::Cleaner,
		Role::Http,
		Role::Indexer,
		Role::Runner,
		Role::Scheduler,
	]
	.into_iter()
	.collect()
}

fn default_scheduler_cpu() -> u64 {
	1
}

fn default_scheduler_create_sandbox_queue_capacity() -> usize {
	1024
}

fn default_scheduler_max_create_sandbox_attempts() -> usize {
	8
}

fn default_scheduler_max_create_sandbox_requests() -> usize {
	256
}

fn default_scheduler_max_create_sandbox_requests_per_runner() -> usize {
	16
}

fn default_scheduler_memory() -> u64 {
	1_073_741_824
}
